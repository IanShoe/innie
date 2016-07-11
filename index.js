const fs = require('fs');
const parse = require('csv-parse');
const transform = require('stream-transform');
const mware = require('mware')();

module.exports = function (config) {
  const input = fs.createReadStream(config.filePath);
  return {
    registerMiddleware: mware,
    run: function () {
      input.on('end', config.end);
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
        .pipe(config.writer);
    }
  };
};
