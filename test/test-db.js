// 
// import(s)
//

var vows = require('vows');
var assert = require('assert');
var events = require('events');
var makeCheckConstantContext = require('./macro').makeCheckConstantContext;
var makeCheckConstantContexts = require('./macro').makeCheckConstantContexts;
var kc = require('../lib/kyotocabinet');
var DB = kc.DB;


// 
// test(s)
//

var suite = vows.describe('kyotocabinet.DB tests');
suite.addBatch({
  'when create DB object by `new` operator': {
    topic: new DB(),
    'should create a DB object': function (db) {
      assert.isObject(db);
    },
    'when call `open` method': {
      'with specific path -> `cascket.kct`, mode -> `omitted`': {
        topic: function (db) {
        }
      }
    }
  }
}).addBatch(makeCheckConstantContexts(DB, {
  'OREADER': 1,
  'OWRITER': 2,
  'OCREATE': 4,
  'OTRUNCATE': 8,
  'OAUTOTRAN': 16,
  'OAUTOSYNC': 32,
  'ONOLOCK': 64,
  'OTRYLOCK': 128,
  'ONOREPAIR': 256,
  'MSET': 0,
  'MADD': 1,
  'MREPLACE': 2,
  'MAPPEND': 3,
  'XNOLOCK': 1,
  'XPARAMAP': 2,
  'XPARARED': 4,
  'XPARAFLS': 8,
  'XNOCOMP': 256
})).export(module);
