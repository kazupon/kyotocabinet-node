var kc = require('kyotocabinet-node');
var DB = kc.DB;
var Error = kc.Error;


var db = new DB();

var N = 10;
var records = {};
for (var i = 0; i < N; i++) {
  records['key' + (i + 1)] = 'value' + (i + 1);
}

db.open({
  path: '%',
  mode: DB.OWRITER | DB.OCREATE
}, function (err) {
  if (err) {
    console.error('database open error: %s (%d)', err.message, err.code);
    return;
  }

  db.set_bulk({ recs: records }, function (err, num) {
    if (err) {
      console.error('set bulk records error: %s (%d)', err.message, err.code);
      return;
    }
    console.log('set bulk record: %d', num);

    db.cursor(function (err, cur) {
      if (err) {
        console.error('cursor error: %s (%d)', err.message, err.code);
        return;
      }

      cur.jump(function (err) {
        if (err) {
          console.error('cursor jump error: %s (%d)', err.message, err.code);
          return;
        }

        var iterate = function (cur, cb) {
          cur.get(true, function (err, key, value) { // auto step to cursor next position
            if (err && (!err.code || err.code === Error.NOREC)) {
              return cb();
            } else if (!err) {
              console.log('cursor get: %s, %s', key, value);
              return iterate(cur, cb);
            } else { // error !!
              return cb(err);
            }
          });
        };

        iterate(cur, function (err) {
          if (err) {
            console.error('iterate by cursor error: %s', err.message);
            return;
          }

          db.close(function (err) {
            if (err) {
              console.error('database close error: %s (%d)', err.message, err.code);
              return;
            }

          });
        });
      });
    });
  });
});
