var kc = require('kyotocabinet-node');
var DB = kc.DB;

var db = new DB();

db.open({
  path: './basic.kch',
  mode: DB.OWRITER | DB.OCREATE | DB.OTRUNCATE
}, function (err) {
  if (err) {
    console.error('database open error: %s (%d)', err.message, err.code);
    return;
  }

  db.set({ key: 'hello', value: 'world' }, function (err) {
    if (err) {
      console.error('set record error: %s (%d)', err.message, err.code);
      return;
    }
    console.log('set `world` value of the `hello` key');

    db.get({ key: 'hello' }, function (err, value) {
      if (err) {
        console.error('get record error: %s (%d)', err.message, err.code);
        return;
      }
      console.log('get value of the `hello` key: %s', value);

      db.close(function (err) {
        if (err) {
          console.error('database close error: %s (%d)', err.message, err.code);
          return;
        }

      });
    });
  });
});
