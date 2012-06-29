// 
// import(s)
//
var vows = require('vows');
var assert = require('assert');
var events = require('events');
var format = require('util').format;
var kc = require('../lib/kyotocabinet');
var DB = kc.DB;

// 
// macro(s)
//
var makeCheckConstantContext = function (obj, const_name, val) {
  var context = {};
  context = {
    topic: obj[const_name],
    'should be defined': function (val) {
      assert.isDefined(val);
    }
  };
  var _val = val;
  context[format('should be `%d`', _val)] = function (val) {
    assert.equal(_val, val);
  };
  return context;
};

var makeCheckConstantContexts = function (obj, const_pairs) {
  var batch = {};
  for (var const_name in const_pairs) {
    batch[format('`%s` constant', const_name)] = makeCheckConstantContext(obj, const_name, const_pairs[const_name]);
  }
  return batch;
};


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
