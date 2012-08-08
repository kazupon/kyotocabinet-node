var kc = require('kyotocabinet-node');
var DB = kc.DB;

var db = new DB();

var records = {
  'key1': 'hello',
  'key2': 'hello',
  'key3': 'hello',
  'key4': 'hello',
  'key5': 'hello',
  'key6': 'world',
  'key7': 'world',
  'key8': 'world',
  'key9': 'world',
  'key10': 'world'
};

db.open({
  path: '+',
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

    var words = {};
    db.mapreduce({
      map: function (key, value, emit) {
        return emit(value, key);
      },
      reduce: function (key, iter) {
        words[key] = [];
        while (true) {
          var value = iter();
          if (!value) {
            break;
          }
          words[key].push(value);
        }
        return true;
      },
      opts: DB.XNOLOCK
    }, function (err) {
      if (err) {
        console.error('mapreduce error: %s (%d)', err.message, err.code);
        return;
      }
      console.log('words:\n', words);

      db.close(function (err) {
        if (err) {
          console.error('database close error: %s (%d)', err.message, err.code);
          return;
        }

      });
    });
  });
});
