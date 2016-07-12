const fs = require('fs');
const parse = require('csv-parse');
const transform = require('stream-transform');
const mware = require('mware')();

module.exports = function (config) {
  if (!config.filePath) {
    throw new Error('config.filePath Required for innie');
  }
  if (!config.end) {
    throw new Error('config.end Required for innie');
  }
  if (!config.writer) {
    throw new Error('config.writer Required for innie');
  }
  const input = fs.createReadStream(config.filePath);
  return {
    registerMiddleware: mware,
    run: function () {
      input
        .pipe(parse({
          columns: true
        }))
        .pipe(transform(function (record, cb) {
          const memo = {};
          mware.run(memo, record, function (err) {
            cb(err, memo);
          });
        }))
        .pipe(config.writer)
        .on('end', config.end);
    }
  };
};
