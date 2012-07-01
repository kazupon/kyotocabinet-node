// 
// import(s)
//

var vows = require('vows');
var assert = require('assert');
var fs = require('fs');
var emitter = require('./macro').emitter;
var makeCheckConstantContext = require('./macro').makeCheckConstantContext;
var makeCheckConstantContexts = require('./macro').makeCheckConstantContexts;
var kc = require('../lib/kyotocabinet');
var DB = kc.DB;
var Error = kc.Error;


// 
// test(s)
//

var suite = vows.describe('kyotocabinet.DB tests');
suite.addBatch(makeCheckConstantContexts(DB, {
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
})).addBatch({
  'when create DB object by `new` operator': {
    topic: new DB(),
    'should create a DB object': function (db) {
      assert.isObject(db);
    },
    teardown: function () {
      fs.unlink('casket.kct', this.callback.bind(this));
    },
    'when call `open` method': {
      'with specific path -> `casket.kct`, mode -> `OREADER + OWRITER + OCREATE`': {
        topic: function (db) {
          this.db = db;
          return emitter(function (promise) {
            db.open({
              path: 'casket.kct',
              mode: DB.OREADER + DB.OWRITER + DB.OCREATE
            }, promise.emit.bind(promise, 'open'));
            promise.emit('success');
          });
        },
        on: {
          'open': {
            'will catch `success`': function (topic) {
              assert.isNull(topic);
            },
            'when call `close` method': {
              topic: function () {
                var db = this.db;
                return emitter(function (promise) {
                  db.close(promise.emit.bind(promise, 'close'));
                  promise.emit('success');
                });
              },
              on: {
                'close': {
                  'will catch `success`': function (topic) {
                    assert.isNull(topic);
                  }
                }
              }
            }
          }
        }
      }
    },
    'when call `close` method': {
      topic: function (db) {
        return emitter(function (promise) {
          db.close(promise.emit.bind(promise, 'close'));
          promise.emit('success');
        });
      },
      on: {
        'close': {
          'will catch `error`': function (topic) {
            assert.isNotNull(topic);
            assert.equal(Error.INVALID, topic.code);
          }
        }
      }
    }
  }
}).export(module);
