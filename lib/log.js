'use strict';

const debug = require('debug');

module.exports = function(name) {
  let debuggerName = `postgres-entities:${name}:`;
  let log = {}
  for (let level of ['trace', 'debug', 'info', 'warn', 'error', 'fatal']) {
    log[level] = debug(debuggerName + level);
  }
  return log;
}

