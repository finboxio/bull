--[[
  Remove jobs from the specific set.

  Input:
    KEYS[1]  set key,
    KEYS[2]  priority key
    KEYS[3]  rate limiter key

    ARGV[1]  jobPrefix
    ARGV[2]  timestamp
    ARGV[3]  limit the number of jobs to be removed. 0 is unlimited
    ARGV[4]  set name, can be any of 'wait', 'active', 'paused', 'delayed', 'completed', or 'failed'
]]

local rcall = redis.call
local limit = tonumber(ARGV[3])
local batchSize = 1000
if limit > 0 then
  batchSize = limit
end

local deleted = {}
local deletedCount = 0

local skipped = {}
local skippedCount = 0

if ARGV[4] == "wait" or ARGV[4] == "active" or ARGV[4] == "paused" then
  local POP = "RPOP"
  local PUSH = "RPUSH"

  -- TODO: update batchSize based on remaining limit
  local batch = rcall(POP, KEYS[1], batchSize)

  while batch and #batch > 0 do
    for _, jobId in ipairs(batch) do
      if limit > 0 and deletedCount >= limit then
        break
      end

      local jobKey = ARGV[1] .. jobId
      if (rcall("EXISTS", jobKey .. ":lock") == 0) then
        local jobTS = rcall("HGET", jobKey, "timestamp")
        if (not jobTS or jobTS < ARGV[2]) then
          rcall("ZREM", KEYS[2], jobId)
          rcall("DEL", jobKey)
          rcall("DEL", jobKey .. ":logs")

          -- delete keys related to rate limiter
          -- NOTE: this code is unncessary for other sets than wait, paused and delayed.
          local limiterIndexTable = KEYS[3] .. ":index"
          local limitedSetKey = rcall("HGET", limiterIndexTable, jobId)
          if limitedSetKey then
            rcall("SREM", limitedSetKey, jobId)
            rcall("HDEL", limiterIndexTable, jobId)
          end

          deletedCount = deletedCount + 1
          table.insert(deleted, jobId)
        else
          skippedCount = skippedCount + 1
          table.insert(skipped, jobId)
        end
      else
        skippedCount = skippedCount + 1
        table.insert(skipped, jobId)
      end
    end

    if limit > 0 and deletedCount >= limit then
      break
    end

    batch = rcall(POP, KEYS[1], batchSize)
  end

  -- Restore skipped IDs
  for i=1, skippedCount do
    rcall(PUSH, KEYS[1], skipped[skippedCount + 1 - i])
  end
else
  local offset = skippedCount
  local batch = rcall("ZRANGE", KEYS[1], offset, offset + batchSize - 1)

  while batch and #batch > 0 do
    local remStart = -1

    for i, jobId in ipairs(batch) do
      if limit > 0 and deletedCount >= limit then
        break
      end

      local jobKey = ARGV[1] .. jobId
      if (rcall("EXISTS", jobKey .. ":lock") == 0) then
        local jobTS = rcall("HGET", jobKey, "timestamp")
        if (not jobTS or jobTS < ARGV[2]) then
          if remStart < 0 then
            remStart = offset + i - 1
          end

          rcall("ZREM", KEYS[2], jobId)
          rcall("DEL", jobKey)
          rcall("DEL", jobKey .. ":logs")

          -- delete keys related to rate limiter
          -- NOTE: this code is unnecessary for other sets than wait, paused and delayed.
          local limiterIndexTable = KEYS[3] .. ":index"
          local limitedSetKey = rcall("HGET", limiterIndexTable, jobId)
          if limitedSetKey then
            rcall("SREM", limitedSetKey, jobId)
            rcall("HDEL", limiterIndexTable, jobId)
          end

          deletedCount = deletedCount + 1
          table.insert(deleted, jobId)
        else
          skippedCount = skippedCount + 1
          if remStart >= 0 then
            rcall("ZREMRANGEBYRANK", KEYS[1], remStart, offset + i - 2)
            remStart = -1
          end
        end
      else
        skippedCount = skippedCount + 1
        if remStart >= 0 then
          rcall("ZREMRANGEBYRANK", KEYS[1], remStart, offset + i - 2)
          remStart = -1
        end
      end
    end

    if remStart >= 0 then
      rcall("ZREMRANGEBYRANK", KEYS[1], remStart, offset + #batch - 1)
    end

    if limit > 0 and deletedCount >= limit then
      break
    end

    offset = skippedCount
    -- TODO: update batchSize based on remaining limit
    batch = rcall("ZRANGE", KEYS[1], offset, offset + batchSize - 1)
  end
end

return deleted
