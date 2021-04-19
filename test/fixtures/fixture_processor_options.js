/**
 * A processor file to be used in tests.
 *
 */
'use strict';

const delay = require('delay');

module.exports = (options) => function(/*job*/) {
  return delay(500).then(() => {
    return options;
  });
};
