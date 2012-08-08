var kc = require('kyotocabinet-node');
var DB = kc.DB;

var db = new DB();

var N = 10;
var records = {};
var keys = [];
for (var i = 0; i < N; i++) {
  records['key' + (i + 1)] = 'value' + (i + 1);
  keys.push('key' + (i + 1));
}

db.open({
  path: './bulk.kct',
  mode: DB.OWRITER | DB.OCREATE | DB.OTRUNCATE
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

    db.get_bulk({ keys: keys }, function (err, values) {
      if (err) {
        console.error('get bulk records error: %s (%d)', err.message, err.code);
        return;
      }
      console.log('get bulk records: ', values);

      db.close(function (err) {
        if (err) {
          console.error('database close error: %s (%d)', err.message, err.code);
          return;
        }

      });
    });
  });
});
