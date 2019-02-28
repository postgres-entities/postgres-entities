'use strict';

const log = require('pino')().child({
  library: 'postgres-entities',
});

log.info({
  pid: process.pid,
  node: process.version,
  cwd: process.cwd(),
  platform: process.platform,
  arch: process.arch,
}, 'Started Logging');

module.exports = function(name) {
  return log.child({
    module: name,
  });
};
