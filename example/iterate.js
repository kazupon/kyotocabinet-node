var kc = require('kyotocabinet-node');
var DB = kc.DB;
var Visitor = kc.Visitor;


var db = new DB();

var N = 10;
var records = {};
for (var i = 0; i < N; i++) {
  records['key' + (i + 1)] = 'value' + (i + 1);
}

db.open({
  path: '-',
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

    db.iterate({
      visitor: {
        visit_full: function (key, value) {
          console.log('iterate: %s %s', key, value);
          return Visitor.NOP;
        }
      },
      writable: false
    }, function (err) {
      if (err) {
        console.error('iterate records error: %s (%d)', err.message, err.code);
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
