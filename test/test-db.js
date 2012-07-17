// 
// import(s)
//

var should = require('should');
var fs = require('fs');
var log = console.log;
var checkConstants = require('./macro').checkConstants;
var kc = require('../lib/kyotocabinet');
var DB = kc.DB;
var Visitor = kc.Visitor;
var Error = kc.Error;


// 
// test(s)
//

describe('DB constants tests', function () {
  checkConstants(DB, {
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
  });
});

describe('DB class tests', function () {
  var db;
  var db1;
  before(function (done) {
    db1 = new DB();
    db1.open({ path: "casket1.kct", mode: DB.OWRITER + DB.OCREATE }, function (err) {
      if (err) { done(err); }
      done();
    });
  });
  after(function (done) {
    db1.close(function (err) {
      if (err) { done(err); }
      fs.unlink("casket1.kct", function () {
        done();
      });
    });
  });

  describe('when create DB object by `new` operator', function () {
    afterEach(function (done) {
      fs.unlink('casket.kct', function (err) {
        done();
      });
    });

    it('should create a DB object', function () {
      db = new DB();
      db.should.be.an.instanceOf(DB);
    });


    // 
    // open
    // 
    describe('when call `open` method', function () {
      describe('with specific path -> `casket.kct`, mode -> `OREADER + OWRITER + OCREATE` object', function () {
        it('will catch `success` callback', function (done) {
          db.open({
            path: 'casket.kct',
            mode: DB.OREADER + DB.OWRITER + DB.OCREATE
          }, function (err) {
            if (err) { done(err); }
            done();
          });
        });
        describe('when call `close` method', function () {
          it('will catch `success` callback', function (done) {
            db.close(function (err) {
              if (err) { done(err); }
              done();
            });
          });
        });
      });
      describe('with specific params nothing', function () {
        it('will catch `success` callback', function (done) {
          db.open(function (err) {
            if (err) { done(err); }
            db.close(function (err) {
              if (err) { done(err); }
              done();
            });
          });
        });
      });
    });


    // 
    // close
    //
    describe('db not open', function () {
      describe('when call `close` method', function () {
        it('should be `INVALID` error', function (done) {
          db.close(function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('call `close` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should be `Success`', function (done) {
            db.open(function (err) {
              if (err) { return done(err); }
              db.close();
              done();
            });
          });
        });
        describe('with specific `object`', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              db.close({ key: 1 });
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific parameter not object', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              db.close(1);
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
    });


    //
    // set
    //
    describe('when call `set` method', function () {
      describe('with specific `key` -> `hoge1`, `value` -> `1`', function () {
        it('should catch `success` callback', function (done) {
          db1.set({ key: 'hoge1', value: '1' }, function (err) {
            if (err) { done(err); }
            done();
          });
        });
        describe('when call `get` method', function () {
          describe('with specific `key` -> `hoge1`', function () {
            it('should catch `success` callback into `1` value', function (done) {
              db1.get({ key: 'hoge1' }, function (err, value) {
                if (err) { done(err); }
                value.should.eql('1');
                done();
              });
            });
          });
        });
      });
      describe('with specific `key` -> `ほげ１`, `value` -> `ほえ`', function () {
        it('should catch `success` callback', function (done) {
          db1.set({ key: 'ほげ１', value: 'ほえ' }, function (err) {
            if (err) { done(err); }
            done();
          });
        });
        describe('when call `get` method', function () {
          describe('with specific `key` -> `ほげ１', function () {
            it('should catch `success` callback into `ほえ` value', function (done) {
              db1.get({ key: 'ほげ１' }, function (err, value) {
                if (err) { done(err); }
                value.should.eql('ほえ');
                done();
              });
            });
          });
          describe('when call `remove` method', function () {
            describe('with specific `key` -> `ほげ１', function () {
              it('should catch `success` callback', function (done) {
                db1.remove({ key: 'ほげ１' }, function (err) {
                  if (err) { done(err); }
                  done();
                });
              });
              describe('when call `get` method', function () {
                it('should catch `err` callback into `NOREC` value at `code` property', function (done) {
                  db1.get({ key: 'ほげ１' }, function (err) {
                    err.should.be.a('object');
                    err.should.have.property('code');
                    err.code.should.eql(Error.NOREC);
                    done();
                  });
                });
              });
            });
          });
        });
      });
      describe('with specific `key` -> `ほhogeげ`, `value` -> `ほ1え`', function () {
        it('should catch `success` callback', function (done) {
          db1.set({ key: 'ほhogeげ', value: 'ほ1え' }, function (err) {
            if (err) { done(err); }
            done();
          });
        });
        describe('when call `get` method', function () {
          describe('with specific `key` -> `ほhogeげ`', function () {
            it('should catch `success` callback into `ほ1え` value', function (done) {
              db1.get({ key: 'ほhogeげ' }, function (err, value) {
                if (err) { done(err); }
                value.should.eql('ほ1え');
                done();
              });
            });
          });
        });
      });
      describe('with no specific `key`', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db1.set({ value: '1' }, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with no specific `value`', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db1.set({ key: 'hoge1' }, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with no specific `key` & `value`', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db1.set({}, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with no specific parameter', function () {
        it('should catch `Error` exception', function (done) {
          try {
            db1.set();
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific `key` type not string', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.set({ key: 1, value: 'hoge' });
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific `value` type not string', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.set({ key: 'hoge', value: {} });
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific parameter not object', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.set('hoge');
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with db not open', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db.set({ key: 'hoge1', value: 'foo' }, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });


    //
    // get
    //
    describe('when call `get` method', function () {
      describe('with specific `key` -> `norecord`', function () {
        it('should catch `err` callback into `NOREC` value at `code` property', function (done) {
          db1.get({ key: 'norecord' }, function (err) {
            err.should.be.a('object');
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
      });
      describe('with no specific `key`', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db1.get({}, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with no specific parameter', function () {
        it('should catch `Error` exception', function (done) {
          try {
            db1.get();
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific `key` type not string', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.get({ key: 1 });
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific parameter not object', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.get(1);
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with db not open', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db.get({ key: 'hoge1' }, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });


    // 
    // clear
    //
    describe('db not open', function () {
      describe('when call `clear` method', function () {
        it('should be `INVALID` error', function (done) {
          db.clear(function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });
    describe('db open', function () {
      describe('when call `clear` method', function () {
        it('should be clear ', function (done) {
          db1.clear(function (err) {
            if (err) { return done(err); }
            db1.get({ key: 'norecord' }, function (err) {
              err.should.be.a('object');
              err.should.have.property('code');
              err.code.should.eql(Error.NOREC);
              done();
            });
          });
        });
      });
      describe('call `clear` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should be `Success`', function (done) {
            db1.clear();
            done();
          });
        });
        describe('with specific `object`', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              db1.clear({ key: 1 });
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific parameter not object', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              db1.clear(1);
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
    });


    // 
    // add
    //
    describe('when call `add` method', function () {
      it('will catch `success` callback', function (done) {
        db1.add({ key: 'hoge1', value: '111' }, function (err) {
          if (err) { done(err); }
          done();
        });
      });
      describe('when call `get` method', function () {
        it('with catch `success` callback into `111` value', function (done) {
          db1.get({ key: 'hoge1' }, function (err, value) {
            if (err) { done(err); }
            value.should.eql('111');
            done();
          });
        });
      });
      describe('when call `add` method', function () {
        it('with catch `success` callback', function (done) {
          db1.add({ key: 'hoge1', value: '222' }, function (err) {
            err.should.be.a('object');
            err.should.have.property('code');
            err.code.should.eql(Error.DUPREC);
            done();
          });
        });
        describe('when call `get` method', function () {
          it('with catch `success` callback into `111` value', function (done) {
            db1.get({ key: 'hoge1' }, function (err, value) {
              if (err) { done(err); }
              value.should.eql('111');
              done();
            });
          });
        });
      });
      describe('with no specific `key`', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db1.add({}, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with no specific parameter', function () {
        it('should catch `Error` exception', function (done) {
          try {
            db1.add();
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific `key` type not string', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.add({ key: 1 });
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific parameter not object', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.add(1);
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with db not open', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db.add({ key: 'hoge1', value: '112' }, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });


    // 
    // append
    //
    describe('when call `append` method', function () {
      it('will catch `success` callback', function (done) {
        db1.append({ key: 'hoge2', value: 'hello' }, function (err) {
          if (err) { done(err); }
          done();
        });
      });
      describe('when call `get` method', function () {
        it('with catch `success` callback into `hello` value', function (done) {
          db1.get({ key: 'hoge2' }, function (err, value) {
            if (err) { done(err); }
            value.should.eql('hello');
            done();
          });
        });
      });
      describe('when call `append` method', function () {
        it('with catch `success` callback', function (done) {
          db1.append({ key: 'hoge2', value: ',world' }, function (err) {
            if (err) { done(err); }
            done();
          });
        });
        describe('when call `get` method', function () {
          it('with catch `success` callback into `hello,world` value', function (done) {
            db1.get({ key: 'hoge2' }, function (err, value) {
              if (err) { done(err); }
              value.should.eql('hello,world');
              done();
            });
          });
        });
      });
      describe('with no specific `key`', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db1.append({}, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with no specific parameter', function () {
        it('should catch `Error` exception', function (done) {
          try {
            db1.append();
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific `key` type not string', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.append({ key: 1 });
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific parameter not object', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.append(1);
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with db not open', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db.append({ key: 'hoge1', value: '112' }, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });


    //
    // remove
    //
    describe('when call `remove` method', function () {
      describe('with specific `key` -> `norecord`', function () {
        it('should catch `err` callback into `NOREC` value at `code` property', function (done) {
          db1.remove({ key: 'norecord' }, function (err) {
            err.should.be.a('object');
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
      });
      describe('with no specific `key`', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db1.remove({}, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with no specific parameter', function () {
        it('should catch `Error` exception', function (done) {
          try {
            db1.remove();
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific `key` type not string', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.remove({ key: 1 });
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific parameter not object', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.remove(1);
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with db not open', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db.remove({ key: 'hoge1' }, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });


    //
    // replace
    //
    describe('when call `replace` method', function () {
      describe('with specific `key` -> `hoge3`, `value` -> `replace`', function () {
        it('should catch `success` callback', function (done) {
          db1.set({ key: 'hoge3', value: '1' }, function (err) {
            if (err) { done(err); }
            db1.replace({ key: 'hoge3', value: 'replace' }, function (err) {
              if (err) { done(err); }
              db1.get({ key: 'hoge3' }, function (err, value) {
                if (err) { done(err); }
                value.should.eql('replace');
                done();
              });
            });
          });
        });
      });
      describe('with specific `key` -> `norecord`', function () {
        it('should catch `err` callback into `NOREC` value at `code` property', function (done) {
          db1.replace({ key: 'norecord', value: 'norecord' }, function (err) {
            err.should.be.a('object');
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
      });
      describe('with no specific `key`', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db1.replace({}, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with no specific parameter', function () {
        it('should catch `Error` exception', function (done) {
          try {
            db1.replace();
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific `key` type not string', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.replace({ key: 1 });
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific parameter not object', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.replace(1);
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with db not open', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db.replace({ key: 'hoge1', value: 'foo' }, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });


    // 
    // seize
    //
    describe('when call `seize` method', function () {
      describe('with specific `key` -> `hoge3`', function () {
        it('should catch `success` callback', function (done) {
          db1.seize({ key: 'hoge3' }, function (err, value) {
            if (err) { done(err); }
            value.should.eql('replace');
            db1.get({ key: 'hoge3' }, function (err) {
              err.should.be.a('object');
              err.should.have.property('code');
              err.code.should.eql(Error.NOREC);
              done();
            });
          });
        });
      });
      describe('with specific `key` -> `norecord`', function () {
        it('should catch `err` callback into `NOREC` value at `code` property', function (done) {
          db1.seize({ key: 'norecord' }, function (err) {
            err.should.be.a('object');
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
      });
      describe('with no specific `key`', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db1.seize({}, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with no specific parameter', function () {
        it('should catch `Error` exception', function (done) {
          try {
            db1.seize();
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific `key` type not string', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.seize({ key: 1 });
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific parameter not object', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.seize(1);
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with db not open', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db.seize({ key: 'hoge3', value: 'foo' }, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });


    // 
    // increment
    //
    describe('when call `increment` method', function () {
      describe('and no record', function () {
        afterEach(function (done) {
          db1.remove({ key: 'hoge4' }, function (err) {
            if (err) { console.error(err); }
            done();
          });
        });
        describe('with specific `key` -> `hoge4`, `num` -> `1`, `orig` -> `1`', function () {
          it('should catch `success` callback', function (done) {
            db1.increment({ key: 'hoge4', num: 1, orig: 1 }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(2);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge4`, `num` -> `1` (ommit `orig`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment({ key: 'hoge4', num: 1 }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(1);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge4`, `orig` -> `1` (ommit `num`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment({ key: 'hoge4', orig: 1 }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(1);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge4` (ommit `num`, `orig`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment({ key: 'hoge4' }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(0);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge4`, `num` -> `10`, `orig` -> negative infinity', function () {
          it('should catch `error` callback', function (done) {
            db1.increment({ key: 'hoge4', num: 10, orig: Number.NEGATIVE_INFINITY }, function (err) {
              err.should.have.property('code');
              err.code.should.eql(Error.LOGIC);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge4`, `orig` -> negative infinity (ommit `num`)', function () {
          it('should catch `error` callback', function (done) {
            db1.increment({ key: 'hoge4', orig: Number.NEGATIVE_INFINITY }, function (err, value) {
              err.should.have.property('code');
              err.code.should.eql(Error.LOGIC);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge4`, `num` -> `10`, `orig` -> positive infinity', function () {
          it('should catch `success` callback', function (done) {
            db1.increment({ key: 'hoge4', num: 10, orig: Number.POSITIVE_INFINITY }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(10);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge4`, `orig` -> positive infinity (ommit `num`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment({ key: 'hoge4', orig: Number.POSITIVE_INFINITY }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(0);
              done();
            });
          });
        });
      });
      describe('and already numeric record', function () {
        beforeEach(function (done) {
          db1.remove({ key: 'hoge4already' }, function (err) {
            if (err) { console.error(err); }
            db1.increment({ key: 'hoge4already', num: 11, orig: 0 }, function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge4already`, `num` -> `1`, `orig` -> `1`', function () {
          it('should catch `success` callback', function (done) {
            db1.increment({ key: 'hoge4already', num: 1, orig: 1 }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(12);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge4already`, `num` -> `1` (ommit `orig`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment({ key: 'hoge4already', num: 1 }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(12);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge4already`, `orig` -> `1` (ommit `num`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment({ key: 'hoge4already', orig: 1 }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(11);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge4already` (ommit `num`, `orig`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment({ key: 'hoge4already' }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(11);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge4already`, `num` -> `10`, `orig` -> negative infinity', function () {
          it('should catch `success` callback', function (done) {
            db1.increment({ key: 'hoge4already', num: 10, orig: Number.NEGATIVE_INFINITY }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(21);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge4already`, `orig` -> negative infinity (ommit `num`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment({ key: 'hoge4already', orig: Number.NEGATIVE_INFINITY }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(11);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge4already`, `num` -> `10`, `orig` -> positive infinity', function () {
          it('should catch `success` callback', function (done) {
            db1.increment({ key: 'hoge4already', num: 10, orig: Number.POSITIVE_INFINITY }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(10);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge4already`, `orig` -> positive infinity (ommit `num`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment({ key: 'hoge4already', orig: Number.POSITIVE_INFINITY }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(0);
              done();
            });
          });
        });
      });
      describe('with no specific `key`', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db1.increment({}, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with no specific parameter', function () {
        it('should catch `Error` exception', function (done) {
          try {
            db1.increment();
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific `key` type not string', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.increment({ key: 1 });
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific parameter not object', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.increment(1);
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
    });


    // 
    // increment_double
    //
    describe('when call `increment_double` method', function () {
      describe('and no record', function () {
        afterEach(function (done) {
          db1.remove({ key: 'hoge5' }, function (err) {
            if (err) { console.error(err); }
            done();
          });
        });
        describe('with specific `key` -> `hoge5`, `num` -> `1.0`, `orig` -> `1.0`', function () {
          it('should catch `success` callback', function (done) {
            db1.increment_double({ key: 'hoge5', num: 1.0, orig: 1.0 }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(2.0);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge5`, `num` -> `1.0` (ommit `orig`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment_double({ key: 'hoge5', num: 1.0 }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(1.0);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge5`, `orig` -> `1.0` (ommit `num`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment_double({ key: 'hoge5', orig: 1.0 }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(1.0);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge5` (ommit `num`, `orig`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment_double({ key: 'hoge5' }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(0.0);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge5`, `num` -> `10.0`, `orig` -> negative infinity', function () {
          it('should catch `error` callback', function (done) {
            db1.increment_double({
              key: 'hoge5',
              num: 10.0,
              orig: Number.NEGATIVE_INFINITY
            }, function (err) {
              err.should.have.property('code');
              err.code.should.eql(Error.LOGIC);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge5`, `orig` -> negative infinity (ommit `num`)', function () {
          it('should catch `error` callback', function (done) {
            db1.increment_double({
              key: 'hoge5',
              orig: Number.NEGATIVE_INFINITY
            }, function (err, value) {
              err.should.have.property('code');
              err.code.should.eql(Error.LOGIC);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge5`, `num` -> `10.0`, `orig` -> positive infinity', function () {
          it('should catch `success` callback', function (done) {
            db1.increment_double({
              key: 'hoge5',
              num: 10.0,
              orig: Number.POSITIVE_INFINITY
            }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(10.0);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge5`, `orig` -> positive infinity (ommit `num`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment_double({
              key: 'hoge5',
              orig: Number.POSITIVE_INFINITY
            }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(0.0);
              done();
            });
          });
        });
      });
      describe('and already numeric record', function () {
        beforeEach(function (done) {
          db1.remove({ key: 'hoge5already' }, function (err) {
            if (err) { console.error(err); }
            db1.increment_double({ key: 'hoge5already', num: 11.0, orig: 0.0 }, function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge5already`, `num` -> `1.0`, `orig` -> `1.0`', function () {
          it('should catch `success` callback', function (done) {
            db1.increment_double({
              key: 'hoge5already',
              num: 1.0,
              orig: 1.0
            }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(12.0);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge5already`, `num` -> `1.0` (ommit `orig`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment_double({ key: 'hoge5already', num: 1.0 }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(12.0);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge5already`, `orig` -> `1.0` (ommit `num`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment_double({
              key: 'hoge5already',
              orig: 1.0
            }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(11.0);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge5already` (ommit `num`, `orig`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment_double({ key: 'hoge5already' }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(11.0);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge5already`, `num` -> `10.0`, `orig` -> negative infinity', function () {
          it('should catch `success` callback', function (done) {
            db1.increment_double({
              key: 'hoge5already',
              num: 10.0,
              orig: Number.NEGATIVE_INFINITY
            }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(21.0);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge5already`, `orig` -> negative infinity (ommit `num`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment_double({
              key: 'hoge5already',
              orig: Number.NEGATIVE_INFINITY
            }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(11.0);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge5already`, `num` -> `10.0`, `orig` -> positive infinity', function () {
          it('should catch `success` callback', function (done) {
            db1.increment_double({
              key: 'hoge5already',
              num: 10.0,
              orig: Number.POSITIVE_INFINITY
            }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(10.0);
              done();
            });
          });
        });
        describe('with specific `key` -> `hoge5already`, `orig` -> positive infinity (ommit `num`)', function () {
          it('should catch `success` callback', function (done) {
            db1.increment_double({
              key: 'hoge5already',
              orig: Number.POSITIVE_INFINITY
            }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql(0.0);
              done();
            });
          });
        });
      });
      describe('with no specific `key`', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db1.increment_double({}, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with no specific parameter', function () {
        it('should catch `Error` exception', function (done) {
          try {
            db1.increment_double();
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific `key` type not string', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.increment_double({ key: 1 });
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific parameter not object', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.increment_double(1);
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
    });


    // 
    // increment_double
    //
    describe('no record', function () {
      describe('when call `cas` method', function () {
        beforeEach(function (done) {
          db1.remove({ key: 'cas_no_record' }, function (err) {
            if (err) { console.error(err); }
            done();
          });
        });
        describe('with specific `key` -> `cas_no_record`, `oval` -> `hello`, `nval` -> `world`', function () {
          it('should be `LOGIC` error', function (done) {
            db1.cas({ key: 'cas_no_record', oval: 'hello', nval: 'world' }, function (err) {
              err.should.have.property('code');
              err.code.should.eql(Error.LOGIC);
              done();
            });
          });
        });
        describe('with specific `key` -> `cas_no_record`, `oval` -> `hello` (ommit `nval`)', function () {
          it('should be `LOGIC` error', function (done) {
            db1.cas({ key: 'cas_no_record', oval: 'hello' }, function (err) {
              err.should.have.property('code');
              err.code.should.eql(Error.LOGIC);
              done();
            });
          });
        });
        describe('with specific `key` -> `cas_no_record`, `nval` -> `world` (ommit `oval`)', function () {
          it('should create `cas_no_record` key record', function (done) {
            db1.cas({ key: 'cas_no_record', nval: 'world' }, function (err) {
              if (err) { return done(err); }
              db1.get({ key: 'cas_no_record' }, function (err, value) {
                if (err) { return done(err); }
                value.should.eql('world');
                done();
              });
            });
          });
        });
      });
    });
    describe('already record', function () {
      describe('when call `cas` method', function () {
        beforeEach(function (done) {
          db1.set({ key: 'cas_record', value: '1234' }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        describe('with specific `key` -> `cas_record`, `oval` -> `1234`, `nval` -> `5678`', function () {
          it('should change from `1234` to `5678` value', function (done) {
            db1.cas({ key: 'cas_record', oval: '1234', nval: '5678' }, function (err) {
              if (err) { return done(err); }
              db1.get({ key: 'cas_record' }, function (err, value) {
                if (err) { return done(err); }
                value.should.eql('5678');
                done();
              });
            });
          });
        });
        describe('with specific `key` -> `cas_record`, `oval` -> `hello`, `nval` -> `5678`', function () {
          it('should be `LOGIC` error', function (done) {
            db1.cas({ key: 'cas_record', oval: 'hello', nval: '5678' }, function (err) {
              err.should.have.property('code');
              err.code.should.eql(Error.LOGIC);
              done();
            });
          });
        });
        describe('with specific `key` -> `cas_record`, `oval` -> `1234` (ommit `nval`)', function () {
          it('should remove `cas_record` key record', function (done) {
            db1.cas({ key: 'cas_record', oval: '1234' }, function (err) {
              if (err) { return done(err); }
              db1.get({ key: 'cas_record' }, function (err, value) {
                err.should.have.property('code');
                err.code.should.eql(Error.NOREC);
                done();
              });
            });
          });
        });
        describe('with specific `key` -> `cas_record`, `nval` -> `5678` (ommit `oval`)', function () {
          it('should be `LOGIC` error', function (done) {
            db1.cas({ key: 'cas_record', nval: '5678' }, function (err) {
              err.should.have.property('code');
              err.code.should.eql(Error.LOGIC);
              done();
            });
          });
        });
      });
    });
    describe('when call `cas` method', function () {
      describe('with no specific `key`', function () {
        it('should be `INVALID` error', function (done) {
          db1.cas({}, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with no specific parameter', function () {
        it('should occured `Error` exception', function (done) {
          try {
            db1.cas();
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific `key` type not string', function () {
        it('should occured `TypeError` exception', function (done) {
          try {
            db1.cas({ key: 1 });
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific parameter not object', function () {
        it('should occured `TypeError` exception', function (done) {
          try {
            db1.cas(1);
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
    });


    // 
    // count
    //
    describe('db not open', function () {
      it('should be `INVALID` error', function (done) {
        var cdb = new DB();
        cdb.count(function (err, ret) {
          ret.should.eql(-1);
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });
    describe('db open', function () {
      var cdb;
      var fname = 'count.kch';
      before(function (done) {
        cdb = new DB();
        cdb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        cdb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function () {
            done();
          });
        });
      });
      describe('call `count` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should be `success`', function (done) {
            cdb.count();
            done();
          });
        });
        describe('with specific `object`', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              cdb.count({ key: 1 });
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific parameter not object', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              cdb.count(1);
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
      describe('when not regist record in db', function () {
        it('should be `0` value', function (done) {
          cdb.count(function (err, ret) {
            if (err) { return done(err); }
            ret.should.eql(0);
            done();
          });
        });
        describe('when add record', function () {
          it('should be `1` value', function (done) {
            cdb.add({ key: 'hoge', value: 'hello' }, function (err) {
              if (err) { return done(err); }
              cdb.count(function (err, ret) {
                if (err) { return done(err); }
                ret.should.eql(1);
                done();
              });
            });
          });
          describe('when remove record', function () {
            it('should be `0` value', function (done) {
              cdb.remove({ key: 'hoge' }, function (err) {
                if (err) { return done(err); }
                cdb.count(function (err, ret) {
                  if (err) { return done(err); }
                  ret.should.eql(0);
                  done();
                });
              });
            });
          });
        });
      });
    });


    // 
    // size
    //
    describe('db not open', function () {
      it('should be `INVALID` error', function (done) {
        var cdb = new DB();
        cdb.size(function (err, ret) {
          ret.should.eql(-1);
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });
    describe('db open', function () {
      var cdb;
      var fname = 'size.kch';
      before(function (done) {
        cdb = new DB();
        cdb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        cdb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function () {
            done();
          });
        });
      });
      describe('call `size` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should be `success` exception', function (done) {
            cdb.size();
            done();
          });
        });
        describe('with specific `object`', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              cdb.size({ key: 1 });
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific parameter not object', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              cdb.size(1);
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
      describe('when not regist record in db', function () {
        it('should be `success` the getting size value', function (done) {
          cdb.size(function (err, ret) {
            if (err) { return done(err); }
            ret.should.be.above(0);
            done();
          });
        });
        describe('when add record', function () {
          it('should be `1` value', function (done) {
            cdb.add({ key: 'hoge', value: 'hello' }, function (err) {
              if (err) { return done(err); }
              cdb.size(function (err, ret) {
                if (err) { return done(err); }
                ret.should.be.above(0);
                done();
              });
            });
          });
          describe('when remove record', function () {
            it('should be `0` value', function (done) {
              cdb.remove({ key: 'hoge' }, function (err) {
                if (err) { return done(err); }
                cdb.size(function (err, ret) {
                  if (err) { return done(err); }
                  ret.should.be.above(0);
                  done();
                });
              });
            });
          });
        });
      });
    });


    // 
    // status
    //
    describe('db not open', function () {
      it('should be `INVALID` error', function (done) {
        var cdb = new DB();
        cdb.status(function (err, ret) {
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });
    describe('db open', function () {
      var cdb;
      var fname = 'status.kch';
      before(function (done) {
        cdb = new DB();
        cdb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        cdb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function () {
            done();
          });
        });
      });
      describe('call `status` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should be `success`', function (done) {
            cdb.status();
            done();
          });
        });
        describe('with specific `object`', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              cdb.status({ key: 1 });
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific parameter not object', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              cdb.status(1);
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
      describe('when not regist record in db', function () {
        it('should be `success` the getting status info object', function (done) {
          cdb.status(function (err, ret) {
            if (err) { return done(err); }
            ret.should.be.an.instanceOf(Object);
            ret.should.have.property('size');
            ret.should.have.property('path');
            ret.count.should.eql('0');
            done();
          });
        });
        describe('when add record', function () {
          it('should be `success` the getting status info object', function (done) {
            cdb.add({ key: 'hoge', value: 'hello' }, function (err) {
              if (err) { return done(err); }
              cdb.status(function (err, ret) {
                if (err) { return done(err); }
                ret.should.be.an.instanceOf(Object);
                ret.should.have.property('size');
                ret.should.have.property('path');
                ret.count.should.eql('1');
                done();
              });
            });
          });
          describe('when remove record', function () {
            it('should be `success` the getting status info object', function (done) {
              cdb.remove({ key: 'hoge' }, function (err) {
                if (err) { return done(err); }
                cdb.status(function (err, ret) {
                  if (err) { return done(err); }
                  ret.should.be.an.instanceOf(Object);
                  ret.should.have.property('size');
                  ret.should.have.property('path');
                  ret.count.should.eql('0');
                  done();
                });
              });
            });
          });
        });
      });
    });


    // 
    // check
    //
    describe('db not open', function () {
      it('should be `INVALID` error', function (done) {
        var mdb = new DB();
        mdb.check(function (err, ret) {
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });
    describe('db open', function () {
      var mdb;
      var fname = 'check.kch';
      before(function (done) {
        mdb = new DB();
        mdb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        mdb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function () {
            done();
          });
        });
      });
      describe('call `check` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.check();
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with no specific `key`', function () {
          it('should be `INVALID` error', function (done) {
            mdb.check({}, function (err) {
              err.should.have.property('code');
              err.code.should.eql(Error.INVALID);
              done();
            });
          });
        });
        describe('with specific `key` type not string', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.check({ key: 1 });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific parameter not object', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.check(1);
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
      describe('not regist record in db', function () {
        it('should be `NOREC` error', function (done) {
          mdb.check({ key: 'check1' }, function (err, size) {
            size.should.eql(-1);
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
        describe('add key `check1` record', function () {
          it('should be get `5` size', function (done) {
            mdb.add({ key: 'check1', value: 'hello' }, function (err) {
              if (err) { return done(err); }
              mdb.check({ key: 'check1' }, function (err, size) {
                if (err) { return done(err); }
                size.should.eql(5);
                done();
              });
            });
          });
          describe('add key `チェック1` record', function () {
            it('should be get `9` size', function (done) {
              mdb.add({ key: 'チェック1', value: 'チェック' }, function (err) {
                if (err) { return done(err); }
                mdb.check({ key: 'チェック1' }, function (err, size) {
                  if (err) { return done(err); }
                  size.should.eql(12);
                  done();
                });
              });
            });
          });
        });
      });
    });


    // 
    // get_bulk
    //
    describe('db not open', function () {
      it('should be `INVALID` error', function (done) {
        var mdb = new DB();
        mdb.get_bulk({ keys: [ 'key1', 'key2' ] }, function (err, values) {
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });
    describe('db open', function () {
      var mdb;
      var fname = 'get_bulk.kct';
      before(function (done) {
        mdb = new DB();
        mdb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        mdb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function () {
            done();
          });
        });
      });
      describe('call `get_bulk` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.get_bulk();
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with no specific `keys`', function () {
          it('should be `INVALID` error', function (done) {
            mdb.get_bulk({}, function (err) {
              err.should.have.property('code');
              err.code.should.eql(Error.INVALID);
              done();
            });
          });
        });
        describe('with specific `keys` type not array', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.get_bulk({ keys: 1 });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `atomic` type not boolean', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.get_bulk({ keys: [ 'key1' ], atomic: 'hello' });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific parameter not object', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.get_bulk(1);
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
      describe('not regist record in db', function () {
        it('should be `NOREC` error', function (done) {
          mdb.get_bulk({ keys: [ 'key1' ], atomic: false }, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
        describe('add `10` records', function () {
          var keys = null;

          before(function (done) {
            var cnt = 0;
            var max = 10;
            keys = [];
            (function fn (cnt, keys, cb) {
              cnt++;
              var key = 'key' + cnt.toString();
              mdb.set({ key: key, value: key }, function (err) {
                if (err) { return cb(err); }
                keys.push(key);
                if (cnt === max) { return cb(null, keys); }
                return fn(cnt, keys, cb);
              });
            })(cnt, keys, function (err, keys) {
              if (err) { return done(err); }
              done();
            });
          });

          describe('with specific all registered keys', function () {
            it('should be get `10` records', function (done) {
              mdb.get_bulk({ keys: keys, atomic: true }, function (err, recs) {
                if (err) { return done(err); }
                recs.should.have.keys(keys);
                for (var i = 0; i < 10; i++) {
                  recs['key' + (i + 1)].should.eql('key' + (i + 1));
                }
                done();
              });
            });
          });
          describe('with specific half registered keys & half not registered keys', function () {
            it('should be get `5` records', function (done) {
              keys = [];
              for (var i = 0; i < 5; i++) {
                keys.push('key' + (i + 1));
              }
              keys.push('hoge');
              keys.push('foo');
              keys.push('bar');
              keys.push('buz');
              keys.push('moge');
              mdb.get_bulk({ keys: keys }, function (err, recs) {
                if (err) { return done(err); }
                Object.keys(recs).should.have.length(5);
                for (var i = 0; i < 5; i++) {
                  recs['key' + (i + 1)].should.eql('key' + (i + 1));
                }
                done();
              });
            });
          });
        });
      });
    });


    // 
    // set_bulk
    //
    describe('db not open', function () {
      it('should be `INVALID` error', function (done) {
        var mdb = new DB();
        mdb.set_bulk({ recs: {
          key1: 'key1',
          key2: 'key2'
        }}, function (err, values) {
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });
    describe('db open', function () {
      var mdb;
      var fname = 'set_bulk.kct';
      before(function (done) {
        mdb = new DB();
        mdb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        mdb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function () {
            done();
          });
        });
      });
      describe('call `set_bulk` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.set_bulk();
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with no specific `keys`', function () {
          it('should be `INVALID` error', function (done) {
            mdb.set_bulk({}, function (err) {
              err.should.have.property('code');
              err.code.should.eql(Error.INVALID);
              done();
            });
          });
        });
        describe('with specific `keys` type not object', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.set_bulk({ recs: 1 });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `atomic` type not boolean', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.set_bulk({ recs: { key1: 'key1' }, atomic: 'hello' });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific parameter not object', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.set_bulk(1);
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
      describe('set `10` records', function () {
        it('should be `success`', function (done) {
          var recs = {};
          var max = 10;
          for (var i = 0; i < max; i++) {
            recs['key' + (i + 1)] = 'key' + (i + 1);
          }
          mdb.set_bulk({ recs: recs, atomic: false }, function (err, num) {
            if (err) { return done(err); }
            num.should.eql(max);
            mdb.get_bulk({ keys: Object.keys(recs) }, function (err, ret) {
              if (err) { return done(err); }
              ret.should.eql(recs);
              mdb.count(function (err, cnt) {
                if (err) { return done(err); }
                cnt.should.eql(max);
                done();
              });
            });
          });
        });
      });
    });


    // 
    // remove_bulk
    //
    describe('db not open', function () {
      it('should be `INVALID` error', function (done) {
        var mdb = new DB();
        mdb.remove_bulk({ keys: [ 'key1', 'key2' ] }, function (err, num) {
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });
    describe('db open', function () {
      var mdb;
      var fname = 'remove_bulk.kct';
      before(function (done) {
        mdb = new DB();
        mdb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        mdb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function () {
            done();
          });
        });
      });
      describe('call `remove_bulk` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.remove_bulk();
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with no specific `keys`', function () {
          it('should be `INVALID` error', function (done) {
            mdb.remove_bulk({}, function (err) {
              err.should.have.property('code');
              err.code.should.eql(Error.INVALID);
              done();
            });
          });
        });
        describe('with specific `keys` type not array', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.remove_bulk({ keys: 1 });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `atomic` type not boolean', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.remove_bulk({ keys: [ 'key1' ], atomic: 'hello' });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific parameter not object', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.remove_bulk(1);
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
      describe('not regist record in db', function () {
        it('should be `NOREC` error', function (done) {
          mdb.remove_bulk({ keys: [ 'key1' ], atomic: false }, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
        describe('add `10` records', function () {
          var recs;
          beforeEach(function (done) {
            recs = {};
            for (var i = 0; i < 10; i++) {
              recs['key' + (i + 1)] = 'key' + (i + 1);
            }
            mdb.set_bulk({ recs: recs }, function (err, num) {
              if (err) { return done(err); }
              done();
            });
          });

          describe('with specific all registered keys', function () {
            it('should be remove `10` records', function (done) {
              mdb.remove_bulk({ keys: Object.keys(recs) }, function (err, num) {
                if (err) { return done(err); }
                num.should.eql(10);
                mdb.count(function (err, cnt) {
                  if (err) { return done(err); }
                  cnt.should.eql(0);
                  done();
                });
              });
            });
          });
          describe('with specific half registered keys & half not registered keys', function () {
            it('should be remove `10` records', function (done) {
              var keys = [];
              for (var i = 0; i < 5; i++) {
                keys.push('key' + (i + 1));
              }
              keys.push('hoge');
              keys.push('foo');
              keys.push('bar');
              keys.push('buz');
              keys.push('moge');
              mdb.remove_bulk({ keys: keys }, function (err, num) {
                if (err) { return done(err); }
                num.should.eql(5);
                mdb.count(function (err, cnt) {
                  if (err) { return done(err); }
                  cnt.should.eql(5);
                  done();
                });
              });
            });
          });
        });
      });
    });


    // 
    // match_prefix
    //
    describe('db not open', function () {
      it('should be `INVALID` error', function (done) {
        var mdb = new DB();
        mdb.match_prefix({ prefix: '_', max: 10 }, function (err, keys) {
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });
    describe('db open', function () {
      var mdb;
      var fname = 'match_prefix.kct';
      before(function (done) {
        mdb = new DB();
        mdb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        mdb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function () {
            done();
          });
        });
      });
      describe('call `match_prefix` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.match_prefix();
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with no specific `prefix`', function () {
          it('should be `INVALID` error', function (done) {
            mdb.match_prefix({}, function (err) {
              err.should.have.property('code');
              err.code.should.eql(Error.INVALID);
              done();
            });
          });
        });
        describe('with specific `prefix` type not string', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.match_prefix({ prefix: 1 });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `max` type not number', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.match_prefix({ prefix: 'key', max: 'hello' });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific parameter not object', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.match_prefix(1);
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
      describe('not regist record in db', function () {
        it('should be `NOREC` error', function (done) {
          mdb.match_prefix({ prefix: 'key' }, function (err, keys) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
        describe('add `10` records', function () {
          before(function (done) {
            var recs = {};
            recs._key1 = 'key1';
            recs._key2 = 'key2';
            recs.key_3 = 'key3';
            recs.key4 = 'key4';
            recs._key5 = 'key5';
            recs.hoge = 'hoge';
            recs.foo = 'foo';
            recs.bar = 'bar';
            recs.buz = 'buz';
            recs.tako = 'tako';
            mdb.set_bulk({ recs: recs }, function (err, num) {
              if (err) { return done(err); }
              done();
            });
          });

          describe('with specific prefix -> `_` max -> `5`', function () {
            it('should be get `3` keys', function (done) {
              mdb.match_prefix({ prefix: '_', max: 5 }, function (err, keys) {
                if (err) { return done(err); }
                console.log(keys);
                Object.keys(keys).should.have.length(3);
                keys.should.include('_key1');
                keys.should.include('_key2');
                keys.should.include('_key5');
                done();
              });
            });
          });
          describe('with specific prefix -> `_` max -> `2`', function () {
            it('should be get `2` keys', function (done) {
              mdb.match_prefix({ prefix: '_', max: 2 }, function (err, keys) {
                if (err) { return done(err); }
                console.log(keys);
                Object.keys(keys).should.have.length(2);
                done();
              });
            });
          });
          describe('with specific prefix -> `_` max -> `-1`', function () {
            it('should be get `3` keys', function (done) {
              mdb.match_prefix({ prefix: '_', max: -1 }, function (err, keys) {
                if (err) { return done(err); }
                console.log(keys);
                Object.keys(keys).should.have.length(3);
                done();
              });
            });
          });
          describe('with specific prefix -> `key` (max ommit)', function () {
            it('should be get `2` keys', function (done) {
              mdb.match_prefix({ prefix: 'key' }, function (err, keys) {
                if (err) { return done(err); }
                console.log(keys);
                Object.keys(keys).should.have.length(2);
                keys.should.include('key_3');
                keys.should.include('key4');
                done();
              });
            });
          });
        });
      });
    });


    // 
    // match_regex
    //
    describe('db not open', function () {
      it('should be `INVALID` error', function (done) {
        var mdb = new DB();
        mdb.match_regex({ regex: /hoge/, max: 10 }, function (err, keys) {
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });
    describe('db open', function () {
      var mdb;
      var fname = 'match_regex.kct';
      before(function (done) {
        mdb = new DB();
        mdb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        mdb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function () {
            done();
          });
        });
      });
      describe('call `match_regex` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.match_regex();
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with no specific `regex`', function () {
          it('should be `INVALID` error', function (done) {
            mdb.match_regex({}, function (err) {
              err.should.have.property('code');
              err.code.should.eql(Error.INVALID);
              done();
            });
          });
        });
        describe('with specific `regex` type not string', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.match_regex({ regex: 1 });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `max` type not number', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.match_regex({ regex: 'key', max: 'hello' });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific parameter not object', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.match_regex(1);
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
      describe('not regist record in db', function () {
        it('should be `NOREC` error', function (done) {
          mdb.match_regex({ regex: /key/ }, function (err, keys) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
        describe('add `10` records', function () {
          before(function (done) {
            var recs = {};
            recs._key1 = 'key1';
            recs._key2 = 'key2';
            recs.key_3 = 'key3';
            recs.key4 = 'key4';
            recs._key5 = 'key5';
            recs.hoge = 'hoge';
            recs.foo = 'foo';
            recs.bar = 'bar';
            recs.buz = 'buz';
            recs.tako = 'tako';
            mdb.set_bulk({ recs: recs }, function (err, num) {
              if (err) { return done(err); }
              done();
            });
          });

          describe('with specific regex-> `ey` max -> `5`', function () {
            it('should be get `5` keys', function (done) {
              mdb.match_regex({ regex: /ey/, max: 5 }, function (err, keys) {
                if (err) { return done(err); }
                console.log(keys);
                Object.keys(keys).should.have.length(5);
                keys.should.include('_key1');
                keys.should.include('_key2');
                keys.should.include('key_3');
                keys.should.include('key4');
                keys.should.include('_key5');
                done();
              });
            });
          });
          describe('with specific regex -> `ey|oo` max -> `2`', function () {
            it('should be get `2` keys', function (done) {
              mdb.match_regex({ regex: /ey|oo/, max: 2 }, function (err, keys) {
                if (err) { return done(err); }
                console.log(keys);
                Object.keys(keys).should.have.length(2);
                done();
              });
            });
          });
          describe('with specific regex -> `^b.*` max -> `-1`', function () {
            it('should be get `2` keys', function (done) {
              mdb.match_regex({ regex: /^b.*/, max: -1 }, function (err, keys) {
                if (err) { return done(err); }
                console.log(keys);
                Object.keys(keys).should.have.length(2);
                keys.should.include('bar');
                keys.should.include('buz');
                done();
              });
            });
          });
          describe('with specific regex -> `key` (max ommit)', function () {
            it('should be get `5` keys', function (done) {
              mdb.match_regex({ regex: /key/ }, function (err, keys) {
                if (err) { return done(err); }
                console.log(keys);
                Object.keys(keys).should.have.length(5);
                keys.should.include('_key1');
                keys.should.include('_key2');
                keys.should.include('key_3');
                keys.should.include('key4');
                keys.should.include('_key5');
                done();
              });
            });
          });
        });
      });
    });


    // 
    // match_similar
    //
    describe('db not open', function () {
      it('should be `INVALID` error', function (done) {
        var mdb = new DB();
        mdb.match_similar({ origin: 'hoge' }, function (err, keys) {
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });
    describe('db open', function () {
      var mdb;
      var fname = 'match_similar.kct';
      before(function (done) {
        mdb = new DB();
        mdb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        mdb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function () {
            done();
          });
        });
      });
      describe('call `match_similar` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.match_similar();
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with no specific `origin`', function () {
          it('should be `INVALID` error', function (done) {
            mdb.match_similar({}, function (err) {
              err.should.have.property('code');
              err.code.should.eql(Error.INVALID);
              done();
            });
          });
        });
        describe('with specific `origin` type not string', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.match_similar({ origin: 1 });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `max` type not number', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.match_similar({ origin: 'key', max: 'hello' });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `range` type not number', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.match_similar({ origin: 'key', range: 'hello' });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `utf` type not boolean', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.match_similar({ origin: 'key', utf: 'hello' });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific parameter not object', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.match_similar(1);
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
      describe('not regist record in db', function () {
        it('should be `NOREC` error', function (done) {
          mdb.match_similar({ origin: 'key' }, function (err, keys) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
        describe('add `5` records', function () {
          before(function (done) {
            var recs = {};
            recs['japan'] = 'japan';
            recs['japanese'] = 'japanese';
            recs['javan'] = 'javan';
            recs['日本'] = '日本';
            recs['にほん'] = 'にほん';
            mdb.set_bulk({ recs: recs }, function (err, num) {
              if (err) { return done(err); }
              done();
            });
          });

          describe('with specific origin-> `japan` max -> `5`, range -> `3`, utf -> `true`', function () {
            it('should be get `3` keys', function (done) {
              mdb.match_similar({ origin: 'japan', max: 5, range: 3, utf: true }, function (err, keys) {
                if (err) { return done(err); }
                console.log(keys);
                Object.keys(keys).should.have.length(3);
                keys.should.include('japan');
                keys.should.include('japanese');
                keys.should.include('javan');
                done();
              });
            });
          });
          describe('with specific origin -> `japan` max -> `-1`, range -> `1`, utf -> `false`', function () {
            it('should be get `2` keys', function (done) {
              mdb.match_similar({ origin: 'japan', max: -1, range: 1, utf: false }, function (err, keys) {
                if (err) { return done(err); }
                console.log(keys);
                Object.keys(keys).should.have.length(2);
                keys.should.include('japan');
                keys.should.include('javan');
                done();
              });
            });
          });
          describe('with specific origin -> `japan`, range -> `1`, utf -> `true` (max ommit)', function () {
            it('should be get `2` keys', function (done) {
              mdb.match_similar({ origin: 'japan', range: 1, utf: true }, function (err, keys) {
                if (err) { return done(err); }
                console.log(keys);
                Object.keys(keys).should.have.length(2);
                keys.should.include('japan');
                keys.should.include('javan');
                done();
              });
            });
          });
          describe('with specific origin -> `japan` max -> `1`, utf -> `false` (range ommit)', function () {
            it('should be get `1` keys', function (done) {
              mdb.match_similar({ origin: 'japan', max: 1, utf: false }, function (err, keys) {
                if (err) { return done(err); }
                console.log(keys);
                Object.keys(keys).should.have.length(1);
                done();
              });
            });
          });
          describe('with specific origin -> `japan` max -> `2`, range -> `1` (utf ommit)', function () {
            it('should be get `2` keys', function (done) {
              mdb.match_similar({ origin: 'japan', max: 2, range: 1 }, function (err, keys) {
                if (err) { return done(err); }
                console.log(keys);
                Object.keys(keys).should.have.length(2);
                done();
              });
            });
          });
        });
      });
    });


    // 
    // copy
    //
    describe('db not open', function () {
      it('should be `INVALID` error', function (done) {
        var mdb = new DB();
        mdb.copy('copy.kct', function (err) {
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });
    describe('db open', function () {
      var mdb;
      var fname = 'copy.kct';
      before(function (done) {
        mdb = new DB();
        mdb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        mdb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function () {
            done();
          });
        });
      });
      describe('call `copy` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.copy();
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific type not string', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.copy(123);
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
      describe('not regist record in db', function () {
        var dest = 'copy_dest.kct';
        afterEach(function (done) {
          fs.unlink(dest, function () {
            done();
          });
        });
        it('should be `success`', function (done) {
          mdb.copy(dest, function (err) {
            if (err) { return done(err); }
            var ddb = new DB();
            ddb.open({ path: dest, mode: DB.OREADER }, function (err) {
              if (err) { return done(err); }
              ddb.count(function (err, cnt) {
                if (err) { return done(err); }
                cnt.should.eql(0);
                ddb.close(function (err) {
                  if (err) { return done(err); }
                  done();
                });
              });
            });
          });
        });
        describe('add record', function () {
          before(function (done) {
            mdb.set({ key: 'hoge', value: 'hello' }, function (err) {
              if (err) { return done(err); }
              done();
            });
          });
          it('should be get record from copy db', function (done) {
            mdb.copy(dest, function (err) {
              if (err) { return done(err); }
              var ddb = new DB();
              ddb.open({ path: dest, mode: DB.OREADER }, function (err) {
                if (err) { return done(err); }
                ddb.get({ key: 'hoge' }, function (err, value) {
                  if (err) { return done(err); }
                  value.should.eql('hello');
                  ddb.count(function (err, cnt) {
                    if (err) { return done(err); }
                    cnt.should.eql(1);
                    ddb.close(function (err) {
                      if (err) { return done(err); }
                      done();
                    });
                  });
                });
              });
            });
          });
        });
      });
    });


    // 
    // merge
    //
    describe('db not open', function () {
      it('should be `INVALID` error', function (done) {
        var mdb = new DB();
        mdb.copy('merge.kct', function (err) {
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });
    describe('db open', function () {
      var mdb;
      var fname = 'merge.kct';
      var dbs = [];
      before(function (done) {
        dbs = [];
        mdb = new DB();
        mdb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          new DB().open({ path: '+', mode: DB.OWRITER + DB.OCREATE }, function (err) {
            if (err) { return done(err); }
            dbs.push(this);
            new DB().open({ path: '%', mode: DB.OWRITER + DB.OCREATE }, function (err) {
              if (err) { return done(err); }
              dbs.push(this);
              done();
            });
          });
        });
      });
      after(function (done) {
        mdb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function () {
            var cnt = 0;
            var len = dbs.length;
            dbs.forEach(function (db) {
              db.close(function (err) {
                if (err) { return done(err); }
                cnt++;
                if (cnt === len) { return done() }
              });
            });
          });
        });
      });
      afterEach(function (done) {
        mdb.clear(function (err) {
          if (err) { return done(err); }
          var cnt = 0;
          var len = dbs.length;
          dbs.forEach(function (db) {
            db.clear(function (err) {
              if (err) { return done(err); }
              cnt++;
              if (cnt === len) { return done() }
            });
          });
        });
      });
      describe('call `merge` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.merge();
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `srcary` type not array', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.merge({ srcary: 1 });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `mode` type not number', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.merge({ srcary: dbs, mode: 'hello' });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
      describe('with specific mode -> `MSET`', function () {
        before(function (done) {
          dbs[0].set_bulk({
            recs: { key1: 'hello', key2: 'world' }
          }, function (err) {
            if (err) { return done(err); }
            dbs[1].set_bulk({
              recs: { key1: 'world', key2: 'hoge', key3: 'dio' }
            }, function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        it('should be merge `success`', function (done) {
          mdb.merge({ srcary: dbs, mode: DB.MSET }, function (err) {
            if (err) { return done(err); }
            mdb.count(function (err, cnt) {
              cnt.should.eql(3);
              if (err) { return done(err); }
              mdb.get_bulk({ keys: [ 'key1', 'key2', 'key3' ] }, function (err, recs) {
                if (err) { return done(err); }
                console.log(recs);
                recs.key1.should.eql('world');
                recs.key2.should.eql('hoge');
                recs.key3.should.eql('dio');
                done();
              });
            });
          });
        });
      });
      describe('with specific mode -> `MADD`', function () {
        before(function (done) {
          dbs[0].set_bulk({
            recs: { key1: 'hello', key2: 'world' }
          }, function (err) {
            if (err) { return done(err); }
            dbs[1].set_bulk({
              recs: { key1: 'world', key2: 'hoge', key3: 'dio' }
            }, function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        it('should be merge `success`', function (done) {
          mdb.merge({ srcary: dbs, mode: DB.MADD }, function (err) {
            if (err) { return done(err); }
            mdb.count(function (err, cnt) {
              cnt.should.eql(3);
              if (err) { return done(err); }
              mdb.get_bulk({ keys: [ 'key1', 'key2', 'key3' ] }, function (err, recs) {
                if (err) { return done(err); }
                console.log(recs);
                recs.key1.should.eql('hello');
                recs.key2.should.eql('world');
                recs.key3.should.eql('dio');
                done();
              });
            });
          });
        });
      });
      describe('with specific mode -> `MAPPEND`', function () {
        before(function (done) {
          dbs[0].set_bulk({
            recs: { key1: 'hello', key2: 'world' }
          }, function (err) {
            if (err) { return done(err); }
            dbs[1].set_bulk({
              recs: { key1: 'world', key2: 'hoge', key3: 'dio' }
            }, function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        it('should be merge `success`', function (done) {
          mdb.merge({ srcary: dbs, mode: DB.MAPPEND }, function (err) {
            if (err) { return done(err); }
            mdb.count(function (err, cnt) {
              cnt.should.eql(3);
              if (err) { return done(err); }
              mdb.get_bulk({ keys: [ 'key1', 'key2', 'key3' ] }, function (err, recs) {
                if (err) { return done(err); }
                console.log(recs);
                recs.key1.should.eql('helloworld');
                recs.key2.should.eql('worldhoge');
                recs.key3.should.eql('dio');
                done();
              });
            });
          });
        });
      });
      describe('with specific mode -> `unknown value`', function () {
        before(function (done) {
          dbs[0].set_bulk({
            recs: { key1: 'hello', key2: 'world' }
          }, function (err) {
            if (err) { return done(err); }
            dbs[1].set_bulk({
              recs: { key1: 'world', key2: 'hoge', key3: 'dio' }
            }, function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        it('should be `INVALID` error', function (done) {
          mdb.merge({ srcary: dbs, mode: 256 }, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });


    // 
    // dump_snapshot
    //
    describe('db not open', function () {
      it('should be `INVALID` error', function (done) {
        var mdb = new DB();
        mdb.dump_snapshot('dump_snapshot.snp', function (err) {
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });
    describe('db open', function () {
      var mdb;
      var fname = 'dump_snapshot.kct';
      before(function (done) {
        mdb = new DB();
        mdb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        mdb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function (err) {
            done();
          });
        });
      });
      describe('call `dump_snapshot` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.dump_snapshot();
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific type not string', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.dump_snapshot(1);
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
      describe('with specific file path`', function () {
        var dest = 'dump_snapshot.snp';
        before(function (done) {
          mdb.clear(function (err) {
            if (err) { return done(err); }
            mdb.set({ key: 'hello', value: 'world' }, function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        after(function (done) {
          fs.unlink(dest, function (err) {
            if (err) { console.error(err); }
            done();
          });
        });
        it('should be dump snapshot', function (done) {
          mdb.dump_snapshot(dest, function (err) {
            if (err) { return done(err); }
            mdb.clear(function (err) {
              if (err) { return done(err); }
              mdb.count(function (err, cnt) {
                if (err) { return done(err); }
                cnt.should.eql(0);
                mdb.load_snapshot(dest, function (err) {
                  if (err) { return done(err); }
                  mdb.get({ key: 'hello' }, function (err, value) {
                    if (err) { return done(err); }
                    value.should.eql('world');
                    done();
                  });
                });
              });
            });
          });
        });
      });
      describe('with specific already file path`', function () {
        var dest = 'dump_snapshot.snp';
        before(function (done) {
          mdb.clear(function (err) {
            if (err) { return done(err); }
            mdb.set({ key: 'hello', value: 'world' }, function (err) {
              if (err) { return done(err); }
              mdb.dump_snapshot(dest, function (err) {
                if (err) { return done(err); }
                done();
              });
            });
          });
        });
        after(function (done) {
          fs.unlink(dest, function (err) {
            if (err) { console.error(err); }
            done();
          });
        });
        it('should be dump snapshot', function (done) {
          mdb.dump_snapshot(dest, function (err) {
            if (err) { return done(err); }
            mdb.clear(function (err) {
              if (err) { return done(err); }
              mdb.count(function (err, cnt) {
                if (err) { return done(err); }
                cnt.should.eql(0);
                mdb.load_snapshot(dest, function (err) {
                  if (err) { return done(err); }
                  mdb.get({ key: 'hello' }, function (err, value) {
                    if (err) { return done(err); }
                    value.should.eql('world');
                    done();
                  });
                });
              });
            });
          });
        });
      });
      describe('with specific no permission file path`', function () {
        var dest = '/dump_snapshot.snp';
        before(function (done) {
          mdb.clear(function (err) {
            if (err) { return done(err); }
            mdb.set({ key: 'hello', value: 'world' }, function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        it('should be `NOREPOS` error', function (done) {
          mdb.dump_snapshot(dest, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREPOS);
            done();
          });
        });
      });
    });


    // 
    // load_snapshot
    //
    describe('db not open', function () {
      it('should be `NOREPOS` error', function (done) {
        var mdb = new DB();
        mdb.load_snapshot('load_snapshot.snp', function (err) {
          err.should.have.property('code');
          err.code.should.eql(Error.NOREPOS);
          done();
        });
      });
    });
    describe('db open', function () {
      var mdb;
      var fname = 'load_snapshot.kct';
      before(function (done) {
        mdb = new DB();
        mdb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        mdb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function (err) {
            done();
          });
        });
      });
      describe('call `load_snapshot` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.load_snapshot();
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific type not string', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              mdb.load_snapshot(1);
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
      describe('with specific file path`', function () {
        var dest = 'load_snapshot.snp';
        before(function (done) {
          mdb.clear(function (err) {
            if (err) { return done(err); }
            mdb.set({ key: 'hello', value: 'world' }, function (err) {
              if (err) { return done(err); }
              mdb.dump_snapshot(dest, function (err) {
                if (err) { return done(err); }
                done();
              });
            });
          });
        });
        after(function (done) {
          fs.unlink(dest, function (err) {
            if (err) { console.error(err); }
            done();
          });
        });
        it('should be dump snapshot', function (done) {
          mdb.load_snapshot(dest, function (err) {
            if (err) { return done(err); }
            mdb.get({ key: 'hello' }, function (err, value) {
              if (err) { return done(err); }
              value.should.eql('world');
              done();
            });
          });
        });
      });
      describe('with specific nothing file path`', function () {
        var dest = 'load_snapshot.snp';
        it('should be `NOREPOS` error', function (done) {
          mdb.load_snapshot(dest, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREPOS);
            done();
          });
        });
      });
      describe('with specific no permission file path`', function () {
        var dest = '/load_snapshot.snp';
        it('should be `NOREPOS` error', function (done) {
          mdb.dump_snapshot(dest, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREPOS);
            done();
          });
        });
      });
    });


    var visit_full_called = false;
    var visit_empty_called = false;
    var visit_called = false;

    // 
    // accept
    //
    var accept_visitor = {
      visit_full: function (key, value) {
        console.log('visit_full: %s, %s', key, value);
        key.should.be.a.ok;
        value.should.be.a.ok;
        visit_full_called = true;
        return Visitor.NOP;
      },
      visit_empty: function (key) {
        console.log('visit_empty: %s', key);
        key.should.be.a.ok;
        visit_empty_called = true;
        return Visitor.NOP;
      }
    };
    describe('db not open', function () {
      it('should be `INVALID` error', function (done) {
        new DB().accept({
          key: 'hoge',
          visitor: accept_visitor,
          writable: false
        }, function (err) {
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });
    describe('db open', function () {
      var adb;
      var fname = 'accept.kct';
      before(function (done) {
        adb = new DB();
        adb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      beforeEach(function (done) {
        adb.set_bulk({
          recs: {
            key1: 'hello',
            key2: ''
          }
        }, function (err, num) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        adb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function (err) {
            done();
          });
        });
      });
      afterEach(function (done) {
        visit_full_called = false;
        visit_empty_called = false;
        visit_called = false;
        adb.clear(function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      describe('call `accept` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              adb.accept();
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `key` type not string', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              adb.accept({ key: 1, visitor: accept_visitor, writable: true });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `visitor` type number', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              adb.accept({ key: 'hoge', visitor: 1, writable: false });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `writable` type not boolean', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              adb.accept({ key: 'hoge', visitor: accept_visitor, writable: 1 });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
      describe('with specific key -> `key1`, visitor -> `no operation visit_full visitor`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.accept({
            key: 'key1',
            visitor: {
              visit_full: function (key, value) {
                key.should.eql('key1');
                value.should.eql('hello');
                visit_full_called = true;
                return Visitor.NOP;
              }
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be called `visit_full` method', function (done) {
          visit_full_called.should.eql(true);
          done();
        });
        it('should be get `hello`', function (done) {
          adb.get({ key: 'key1' }, function (err, value) {
            value.should.eql('hello');
            done();
          });
        });
      });
      describe('with specific key -> `key1`, visitor -> `remove visit_full visitor`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.accept({
            key: 'key1',
            visitor: {
              visit_full: function (key, value) {
                key.should.eql('key1');
                value.should.eql('hello');
                visit_full_called = true;
                return Visitor.REMOVE;
              }
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be called `visit_full` method', function (done) {
          visit_full_called.should.eql(true);
          done();
        });
        it('should be `NOREC` error', function (done) {
          adb.get({ key: 'key1' }, function (err, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
      });
      describe('with specific key -> `key1`, visitor -> `update visit_full visitor`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.accept({
            key: 'key1',
            visitor: {
              visit_full: function (key, value) {
                key.should.eql('key1');
                value.should.eql('hello');
                visit_full_called = true;
                return 'world';
              }
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be called `visit_full` method', function (done) {
          visit_full_called.should.eql(true);
          done();
        });
        it('should be get `world`', function (done) {
          adb.get({ key: 'key1' }, function (err, value) {
            value.should.eql('world');
            done();
          });
        });
      });
      describe('with specific key -> `hoge`, visitor -> `no operation visit_empty visitor`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.accept({
            key: 'hoge',
            visitor: {
              visit_empty: function (key) {
                key.should.eql('hoge');
                visit_empty_called = true;
                return Visitor.NOP;
              }
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be called `visit_empty` method', function (done) {
          visit_empty_called.should.eql(true);
          done();
        });
        it('should be `NOREC` error', function (done) {
          adb.get({ key: 'hoge' }, function (err, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
      });
      describe('with specific key -> `hoge`, visitor -> `remove visit_empty visitor`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.accept({
            key: 'hoge',
            visitor: {
              visit_empty: function (key) {
                key.should.eql('hoge');
                visit_empty_called = true;
                return Visitor.REMOVE;
              }
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be called `visit_empty` method', function (done) {
          visit_empty_called.should.eql(true);
          done();
        });
        it('should be `NOREC` error', function (done) {
          adb.get({ key: 'hoge' }, function (err, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
      });
      describe('with specific key -> `hoge`, visitor -> `update visit_empty visitor`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.accept({
            key: 'hoge',
            visitor: {
              visit_empty: function (key) {
                key.should.eql('hoge');
                visit_empty_called = true;
                return 'world';
              }
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be called `visit_empty` method', function (done) {
          visit_empty_called.should.eql(true);
          done();
        });
        it('should be get `world`', function (done) {
          adb.get({ key: 'hoge' }, function (err, value) {
            value.should.eql('world');
            done();
          });
        });
      });
      describe('with specific visitor -> `accept_visitor`, writable -> `true` (ommit key)', function () {
        it('should be `INVALID` error', function (done) {
          adb.accept({
            visitor: accept_visitor,
            writable: true
          }, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with specific key -> `key1`, visitor -> `no operation visitor function object`, writable -> `true`', function () {
        var visit;
        beforeEach(function (done) {
          visit = function (key, value) {
            key.should.be.a.ok;
            value.should.be.a.ok;
            visit_called = true;
            return Visitor.NOP;
          };
          visit.called = false;
          adb.accept({
            key: 'key1',
            visitor: visit,
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be called `visitor` function', function (done) {
          visit_called.should.eql(true);
          done();
        });
        it('should be get `hello`', function (done) {
          adb.get({ key: 'key1' }, function (err, value) {
            value.should.eql('hello');
            done();
          });
        });
      });
      describe('with specific key -> `key1`, visitor -> `remove operation visitor function object`, writable -> `true`', function () {
        var visit;
        beforeEach(function (done) {
          visit = function (key, value) {
            key.should.be.a.ok;
            value.should.be.a.ok;
            visit_called = true;
            return Visitor.REMOVE;
          };
          visit.called = false;
          adb.accept({
            key: 'key1',
            visitor: visit,
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be called `visitor` function', function (done) {
          visit_called.should.eql(true);
          done();
        });
        it('should be `NOREC` error', function (done) {
          adb.get({ key: 'key1' }, function (err, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
      });
      describe('with specific key -> `key2`, visitor -> `update operation visitor function object`, writable -> `true`', function () {
        var visit;
        beforeEach(function (done) {
          visit = function (key) {
            key.should.be.a.ok;
            visit_called = true;
            return 'world';
          };
          visit.called = false;
          adb.accept({
            key: 'key2',
            visitor: visit,
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be called `visitor` function', function (done) {
          visit_called.should.eql(true);
          done();
        });
        it('should be get `world` error', function (done) {
          adb.get({ key: 'key2' }, function (err, value) {
            value.should.eql('world');
            done();
          });
        });
      });
      describe('with specific key -> `key1`, writable -> `false` (ommit visitor)', function () {
        it('should be `INVALID` error', function (done) {
          adb.accept({
            key: 'key1',
            writable: false
          }, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with specific key -> `key1`, visitor -> `update visitor function object`, writable -> `false`', function () {
        var visit;
        beforeEach(function (done) {
          visit = function (key, value) {
            key.should.be.a.ok;
            value.should.be.a.ok;
            visit_called = true;
            return 'world';
          };
          visit.called = false;
          adb.accept({
            key: 'key1',
            visitor: visit,
            writable: false
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be called `visitor` function', function (done) {
          visit_called.should.eql(true);
          done();
        });
        it('should be get `hello` error', function (done) {
          adb.get({ key: 'key1' }, function (err, value) {
            value.should.eql('hello');
            done();
          });
        });
      });
      describe('with specific key -> `hoge`, visitor -> `update visit_empty visitor` (ommit  writable)', function () {
        beforeEach(function (done) {
          adb.accept({
            key: 'hoge',
            visitor: {
              visit_empty: function (key) {
                key.should.eql('hoge');
                visit_empty_called = true;
                return 'world';
              }
            }
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be called `visit_empty` method', function (done) {
          visit_empty_called.should.eql(true);
          done();
        });
        it('should be get `world`', function (done) {
          adb.get({ key: 'hoge' }, function (err, value) {
            value.should.eql('world');
            done();
          });
        });
      });
    });


    var visit_full_call_count = 0;
    var visit_empty_call_count = 0;
    var visit_call_count = 0;

    // 
    // accept_bulk
    //
    describe('db not open', function () {
      it('should be `INVALID` error', function (done) {
        new DB().accept_bulk({
          keys: [ 'hoge', 'foo' ],
          visitor: {
            visit_full: function (key, value) { return Visitor.NOP; },
            visit_empty: function (key) { return Visitor.NOP; }
          },
          writable: false
        }, function (err) {
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });
    describe('db open', function () {
      var adb;
      var fname = 'accept_bulk.kct';
      before(function (done) {
        adb = new DB();
        adb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      beforeEach(function (done) {
        adb.set_bulk({
          recs: {
            key1: 'hello',
            key2: ''
          }
        }, function (err, num) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        adb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function (err) {
            done();
          });
        });
      });
      afterEach(function (done) {
        visit_full_call_count = 0;
        visit_empty_call_count = 0;
        visit_call_count = 0;
        adb.clear(function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      describe('call `accept_bulk` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              adb.accept_bulk();
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `key` type not array', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              adb.accept_bulk({
                keys: 1,
                visitor: function (key, value) { return Visitor.NOP; },
                writable: true
              });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `visitor` type number', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              adb.accept_bulk({ keys: [ 'hoge' ], visitor: 1, writable: false });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `writable` type not boolean', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              adb.accept_bulk({
                keys: [ 'hoge' ],
                visitor: function (key, value) { return Visitor.NOP; },
                writable: 1
              });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
      describe('with specific keys -> `[key1]`, visitor -> `no operation visit_full visitor`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.accept_bulk({
            keys: [ 'key1' ],
            visitor: {
              visit_full: function (key, value) {
                key.should.be.a.ok;
                value.should.be.a.ok;
                visit_full_call_count++;
                return Visitor.NOP;
              }
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `1` called `visit_full` method', function (done) {
          visit_full_call_count.should.eql(1);
          done();
        });
        it('should be get `hello`', function (done) {
          adb.get({ key: 'key1' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('hello');
            done();
          });
        });
      });
      describe('with specific keys -> [`key1`, `key2`], visitor -> `remove visit_full visitor`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.accept_bulk({
            keys: [ 'key1', 'key2' ],
            visitor: {
              visit_full: function (key, value) {
                key.should.be.a.ok;
                value.should.be.a.ok;
                visit_full_call_count++;
                return Visitor.REMOVE;
              }
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `2` called `visit_full` method', function (done) {
          visit_full_call_count.should.eql(2);
          done();
        });
        it('should be `0` recode count', function (done) {
          adb.count(function (err, num) {
            if (err) { return done(err); }
            num.should.eql(0);
            done();
          });
        });
      });
      describe('with specific key -> `[key1, key2]`, visitor -> `update visit_full visitor`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.accept_bulk({
            keys: [ 'key1', 'key2' ],
            visitor: {
              visit_full: function (key, value) {
                visit_full_call_count++;
                return (key === 'key1' ? 'world' : 'the');
              }
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `2` called `visit_full` method', function (done) {
          visit_full_call_count.should.eql(2);
          done();
        });
        it('should be get `world` value from `key1` key', function (done) {
          adb.get({ key: 'key1' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('world');
            done();
          });
        });
        it('should be get `the` value from `key2` key', function (done) {
          adb.get({ key: 'key2' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('the');
            done();
          });
        });
      });
      describe('with specific keys -> [`hoge`, `foo`, `hogehoge`], visitor -> `no operation visit_empty visitor`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.accept_bulk({
            keys: [ 'hoge', 'foo', 'hogehoge' ],
            visitor: {
              visit_empty: function (key) {
                visit_empty_call_count++;
                return Visitor.NOP;
              }
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `3` called `visit_empty` method', function (done) {
          visit_empty_call_count.should.eql(3);
          done();
        });
        it('should be `NOREC` error from `hoge` key', function (done) {
          adb.get({ key: 'hoge' }, function (err, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
        it('should be `NOREC` error from `foo` key', function (done) {
          adb.get({ key: 'foo' }, function (err, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
        it('should be `NOREC` error from `hogehoge` key', function (done) {
          adb.get({ key: 'hogehoge' }, function (err, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
      });
      describe('with specific key -> [`hoge`], visitor -> `remove visit_empty visitor`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.accept_bulk({
            keys: [ 'hoge' ],
            visitor: {
              visit_empty: function (key) {
                visit_empty_call_count++;
                return Visitor.REMOVE;
              }
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `1` called `visit_empty` method', function (done) {
          visit_empty_call_count.should.eql(1);
          done();
        });
        it('should have `2` recodes', function (done) {
          adb.count(function (err, num) {
            if (err) { return done(err); }
            num.should.eql(2);
            done();
          });
        });
      });
      describe('with specific key -> [`hoge`, `foo`], visitor -> `update visit_empty visitor`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.accept_bulk({
            keys: [ 'hoge', 'foo' ],
            visitor: {
              visit_empty: function (key) {
                visit_empty_call_count++;
                return 'world';
              }
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `2` called `visit_empty` method', function (done) {
          visit_empty_call_count.should.eql(2);
          done();
        });
        it('should be get `world` form `hoge` key', function (done) {
          adb.get({ key: 'hoge' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('world');
            done();
          });
        });
        it('should be get `world` form `foo` key', function (done) {
          adb.get({ key: 'foo' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('world');
            done();
          });
        });
      });
      describe('with specific key -> [`key1`, `key2`, `foo`, `bar`], visitor -> `visit_full and visit_empty multi operation visitor`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.accept_bulk({
            keys: [ 'key1', 'key2', 'foo', 'bar' ],
            visitor: {
              visit_full: function (key, value) {
                visit_full_call_count++;
                return (key === 'key1' ? Visitor.NOP : Visitor.REMOVE);
              },
              visit_empty: function (key) {
                visit_empty_call_count++;
                return (key === 'foo' ? 'the' : 'world');
              }
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `2` called `visit_full` method', function (done) {
          visit_full_call_count.should.eql(2);
          done();
        });
        it('should be `2` called `visit_empty` method', function (done) {
          visit_empty_call_count.should.eql(2);
          done();
        });
        it('should be get `hello` from `key1` key', function (done) {
          adb.get({ key: 'key1' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('hello');
            done();
          });
        });
        it('should be `NOREC` error from `key2` key', function (done) {
          adb.get({ key: 'key2' }, function (err, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
        it('should be get `the` from `foo` key', function (done) {
          adb.get({ key: 'foo' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('the');
            done();
          });
        });
        it('should be get `world` from `bar` key', function (done) {
          adb.get({ key: 'bar' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('world');
            done();
          });
        });
      });
      describe('with specific visitor -> `visit_full implemented visitor`, writable -> `true` (ommit keys)', function () {
        it('should be `INVALID` error', function (done) {
          adb.accept_bulk({
            visitor: {
              visit_full: function (key, value) { return Visitor.NOP; }
            },
            writable: true
          }, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with specific key -> [`key1`, `key2`], visitor -> `no operation visitor function object`, writable -> `true`', function () {
        var visit;
        beforeEach(function (done) {
          visit = function (key, value) {
            key.should.be.a.ok;
            value.should.be.a.ok;
            visit_call_count++;
            return Visitor.NOP;
          };
          adb.accept_bulk({
            keys: [ 'key1', 'key2' ],
            visitor: visit,
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `2` called `visitor` function', function (done) {
          visit_call_count.should.eql(2);
          done();
        });
        it('should be get `hello` from `key1` key', function (done) {
          adb.get({ key: 'key1' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('hello');
            done();
          });
        });
        it('should be get `` from `key2` key', function (done) {
          adb.get({ key: 'key2' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('');
            done();
          });
        });
      });
      describe('with specific key -> [`key1`], visitor -> `remove operation visitor function object`, writable -> `true`', function () {
        var visit;
        beforeEach(function (done) {
          visit = function (key, value) {
            key.should.be.a.ok;
            value.should.be.a.ok;
            visit_call_count++;
            return Visitor.REMOVE;
          };
          adb.accept_bulk({
            keys: [ 'key1' ],
            visitor: visit,
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `1` called `visitor` function', function (done) {
          visit_call_count.should.eql(1);
          done();
        });
        it('should be `NOREC` error from `key1` key', function (done) {
          adb.get({ key: 'key1' }, function (err, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
      });
      describe('with specific keys -> [`key2`, `hoge`], visitor -> `update operation visitor function object`, writable -> `true`', function () {
        var visit;
        beforeEach(function (done) {
          visit = function (key, value) {
            visit_call_count++;
            return (key === 'key2' ? 'the' : 'world');
          };
          adb.accept_bulk({
            keys: [ 'key2', 'foo' ],
            visitor: visit,
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `2` called `visitor` function', function (done) {
          visit_call_count.should.eql(2);
          done();
        });
        it('should be get `the` value from `key2` key', function (done) {
          adb.get({ key: 'key2' }, function (err, value) {
            value.should.eql('the');
            done();
          });
        });
        it('should be get `world` value from `foo` key', function (done) {
          adb.get({ key: 'foo' }, function (err, value) {
            value.should.eql('world');
            done();
          });
        });
      });
      describe('with specific keys -> [`key1`], writable -> `false` (ommit visitor)', function () {
        it('should be `INVALID` error', function (done) {
          adb.accept({
            keys: [ 'key1' ],
            writable: false
          }, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with specific key -> [`key1`, `key2`, `hoge`, `bar`], visitor -> `visit_full and visit_empty multi operation visitor`, writable -> `false`', function () {
        beforeEach(function (done) {
          adb.accept_bulk({
            keys: [ 'key1', 'key2', 'hoge', 'bar' ],
            visitor: {
              visit_full: function (key, value) {
                visit_full_call_count++;
                return (key === 'key1' ? Visitor.NOP : Visitor.REMOVE);
              },
              visit_empty: function (key) {
                visit_empty_call_count++;
                return (key === 'hoge' ? 'the' : 'world');
              }
            },
            writable: false
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `2` called `visit_full` method', function (done) {
          visit_full_call_count.should.eql(2);
          done();
        });
        it('should be `2` called `visit_empty` method', function (done) {
          visit_empty_call_count.should.eql(2);
          done();
        });
        it('should be get `hello` from `key1` key', function (done) {
          adb.get({ key: 'key1' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('hello');
            done();
          });
        });
        it('should be get `` from `key2` key', function (done) {
          adb.get({ key: 'key2' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('');
            done();
          });
        });
        it('should be `NOREC` error `hoge` key', function (done) {
          adb.get({ key: 'hoge' }, function (err, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
        it('should be `NOREC` error from `bar` key', function (done) {
          adb.get({ key: 'bar' }, function (err, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
      });
      describe('with specific keys -> [`hoge`], visitor -> `update function visitor` (ommit  writable)', function () {
        beforeEach(function (done) {
          adb.accept_bulk({
            keys: [ 'hoge' ],
            visitor: function (key, value) {
              visit_call_count++;
              return 'hello';
            }
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `1` called `visitor` function', function (done) {
          visit_call_count.should.eql(1);
          done();
        });
        it('should be get `hello` from `hoge` key', function (done) {
          adb.get({ key: 'hoge' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('hello');
            done();
          });
        });
      });
    });


    // 
    // iterate
    //
    describe('db not open', function () {
      it('should be `INVALID` error', function (done) {
        new DB().iterate({
          visitor: {
            visit_full: function (key, value) { return Visitor.NOP; },
            visit_empty: function (key) { return Visitor.NOP; }
          },
          writable: false
        }, function (err) {
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });
    describe('db open', function () {
      var adb;
      var fname = 'iterate.kct';
      before(function (done) {
        adb = new DB();
        adb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      beforeEach(function (done) {
        adb.set_bulk({
          recs: {
            key1: 'hello',
            key2: 'world',
            key3: 'hoge',
            key4: 'foo',
            key5: 'bar'
          }
        }, function (err, num) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        adb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function (err) {
            done();
          });
        });
      });
      afterEach(function (done) {
        visit_full_call_count = 0;
        visit_call_count = 0;
        adb.clear(function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      describe('call `iterate` method parameter check', function () {
        describe('with no specific parameter', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              adb.iterate();
            } catch (e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `visitor` type number', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              adb.iterate({ visitor: 1, writable: false });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
        describe('with specific `writable` type not boolean', function () {
          it('should occured `TypeError` exception', function (done) {
            try {
              adb.iterate({
                visitor: function (key, value) { return Visitor.NOP; },
                writable: 1
              });
            } catch(e) {
              e.should.be.an.instanceOf(TypeError);
              done();
            }
          });
        });
      });
      describe('with specific visitor -> `no operation visit_full visitor`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.iterate({
            visitor: {
              visit_full: function (key, value) {
                key.should.be.a.ok;
                value.should.be.a.ok;
                visit_full_call_count++;
                return Visitor.NOP;
              }
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `5` called `visit_full` method', function (done) {
          visit_full_call_count.should.eql(5);
          done();
        });
      });
      describe('with specific visitor -> `remove visit_full visitor`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.iterate({
            visitor: {
              visit_full: function (key, value) {
                key.should.be.a.ok;
                value.should.be.a.ok;
                visit_full_call_count++;
                return Visitor.REMOVE;
              }
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `5` called `visit_full` method', function (done) {
          visit_full_call_count.should.eql(5);
          done();
        });
        it('should be `0` recode count', function (done) {
          adb.count(function (err, num) {
            if (err) { return done(err); }
            num.should.eql(0);
            done();
          });
        });
      });
      describe('with specific visitor -> `update visit_full visitor`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.iterate({
            visitor: {
              visit_full: function (key, value) {
                visit_full_call_count++;
                return 'hogehogehoge';
              }
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `5` called `visit_full` method', function (done) {
          visit_full_call_count.should.eql(5);
          done();
        });
        it('should be get all `hogehogehoge` values', function (done) {
          adb.get_bulk({
            keys: [ 'key1', 'key2', 'key3', 'key4', 'key5' ]
          }, function (err, values) {
            if (err) { return done(err); }
            for (var key in values) {
              values[key].should.eql('hogehogehoge');
            }
            done();
          });
        });
      });
      describe('with specific visitor -> `visit_full multi operation visitor`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.iterate({
            visitor: {
              visit_full: function (key, value) {
                visit_full_call_count++;
                var ret = Visitor.NOP;
                switch (key) {
                  case 'key3':
                    ret = Visitor.REMOVE;
                    break;
                  case 'key4':
                    ret = Visitor.REMOVE;
                    break;
                  case 'key5':
                    ret = 'dio';
                    break;
                }
                return ret;
              }
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `5` called `visit_full` method', function (done) {
          visit_full_call_count.should.eql(5);
          done();
        });
        it('should be get `hello` value from `key1` key', function (done) {
          adb.get({ key: 'key1' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('hello');
            done();
          });
        });
        it('should be get `world` value from `key2` key', function (done) {
          adb.get({ key: 'key2' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('world');
            done();
          });
        });
        it('should be `NOREC` error from `key3` key', function (done) {
          adb.get({ key: 'key3' }, function (err, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
        it('should be `NOREC` error from `key4` key', function (done) {
          adb.get({ key: 'key4' }, function (err, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
        it('should be get `dio` value from `key5` key', function (done) {
          adb.get({ key: 'key5' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('dio');
            done();
          });
        });
      });
      describe('with specific visitor -> `no operation visitor function object`, writable -> `true`', function () {
        var visit;
        beforeEach(function (done) {
          visit = function (key, value) {
            key.should.be.a.ok;
            value.should.be.a.ok;
            visit_call_count++;
            return Visitor.NOP;
          };
          adb.iterate({
            visitor: visit,
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `5` called `visitor` function', function (done) {
          visit_call_count.should.eql(5);
          done();
        });
      });
      describe('with specific visitor -> `remove operation visitor function object`, writable -> `true`', function () {
        var visit;
        beforeEach(function (done) {
          visit = function (key, value) {
            key.should.be.a.ok;
            value.should.be.a.ok;
            visit_call_count++;
            return Visitor.REMOVE;
          };
          adb.iterate({
            visitor: visit,
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `5` called `visitor` function', function (done) {
          visit_call_count.should.eql(5);
          done();
        });
        it('should be `NOREC` error from `key1` key', function (done) {
          adb.get({ key: 'key1' }, function (err, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
      });
      describe('with specific visitor -> `update operation visitor function object`, writable -> `true`', function () {
        var visit;
        beforeEach(function (done) {
          visit = function (key, value) {
            visit_call_count++;
            return (key === 'key2' ? 'the' : 'world');
          };
          adb.iterate({
            visitor: visit,
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `5` called `visitor` function', function (done) {
          visit_call_count.should.eql(5);
          done();
        });
        it('should be get changed record values', function (done) {
          adb.get_bulk({
            keys: [ 'key1', 'key2', 'key3', 'key4', 'key5' ]
          }, function (err, values) {
            if (err) { return done(err); }
            values['key1'].should.eql('world');
            values['key2'].should.eql('the');
            values['key3'].should.eql('world');
            values['key4'].should.eql('world');
            values['key5'].should.eql('world');
            done();
          });
        });
      });
      describe('with specific writable -> `false` (ommit visitor)', function () {
        it('should be `INVALID` error', function (done) {
          adb.iterate({
            writable: false
          }, function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with specific visitor -> `multi operation visitor function`, writable -> `true`', function () {
        beforeEach(function (done) {
          adb.iterate({
            visitor: function (key, value) {
              visit_call_count++;
              var ret = Visitor.NOP;
              switch (key) {
                case 'key3':
                  ret = Visitor.REMOVE;
                  break;
                case 'key4':
                  ret = Visitor.REMOVE;
                  break;
                case 'key5':
                  ret = 'dio';
                  break;
              }
              return ret;
            },
            writable: true
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `5` called `visit` function', function (done) {
          visit_call_count.should.eql(5);
          done();
        });
        it('should be get `hello` value from `key1` key', function (done) {
          adb.get({ key: 'key1' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('hello');
            done();
          });
        });
        it('should be get `world` value from `key2` key', function (done) {
          adb.get({ key: 'key2' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('world');
            done();
          });
        });
        it('should be `NOREC` error from `key3` key', function (done) {
          adb.get({ key: 'key3' }, function (err, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
        it('should be `NOREC` error from `key4` key', function (done) {
          adb.get({ key: 'key4' }, function (err, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
        it('should be get `dio` value from `key5` key', function (done) {
          adb.get({ key: 'key5' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('dio');
            done();
          });
        });
      });
      describe('with specific visitor -> `multi operation visitor function`, writable -> `false`', function () {
        beforeEach(function (done) {
          adb.iterate({
            visitor: function (key, value) {
              visit_call_count++;
              var ret = Visitor.NOP;
              switch (key) {
                case 'key3':
                  ret = Visitor.REMOVE;
                  break;
                case 'key4':
                  ret = Visitor.REMOVE;
                  break;
                case 'key5':
                  ret = 'dio';
                  break;
              }
              return ret;
            },
            writable: false
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `5` called `visit` function', function (done) {
          visit_call_count.should.eql(5);
          done();
        });
        it('should be get `hello` value from `key1` key', function (done) {
          adb.get({ key: 'key1' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('hello');
            done();
          });
        });
        it('should be get `world` value from `key2` key', function (done) {
          adb.get({ key: 'key2' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('world');
            done();
          });
        });
        it('should be get `hoge` value from `key3` key', function (done) {
          adb.get({ key: 'key3' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('hoge');
            done();
          });
        });
        it('should be get `foo` value from `key4` key', function (done) {
          adb.get({ key: 'key4' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('foo');
            done();
          });
        });
        it('should be get `bar` value from `key5` key', function (done) {
          adb.get({ key: 'key5' }, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('bar');
            done();
          });
        });
      });
      describe('with specific visitor -> `update function visitor` (ommit  writable)', function () {
        beforeEach(function (done) {
          adb.iterate({
            visitor: function (key, value) {
              visit_call_count++;
              return 'hello';
            }
          }, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('should be `5` called `visit` function', function (done) {
          visit_call_count.should.eql(5);
          done();
        });
        it('should be get all `hello` values', function (done) {
          adb.get_bulk({
            keys: [ 'key1', 'key2', 'key3', 'key4', 'key5' ]
          }, function (err, values) {
            if (err) { return done(err); }
            for (var key in values) {
              values[key].should.eql('hello');
            }
            done();
          });
        });
      });
    });


    // 
    // begin_transaction / end_transaction
    //
    describe('db not open', function () {
      describe('when call `begin_transaction`', function () {
        it('should be `INVALID` error', function (done) {
          new DB().begin_transaction(function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('when call `end_transaction`', function () {
        it('should be `INVALID` error', function (done) {
          new DB().end_transaction(function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });
    describe('db open', function () {
      var adb;
      var fname = 'begin_end_transaction.kct';
      before(function (done) {
        adb = new DB();
        adb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        adb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function (err) {
            done();
          });
        });
      });
      afterEach(function (done) {
        adb.clear(function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      describe('iligale parameter check', function () {
        describe('begin_transaction', function () {
          describe('with specific `hard` type not boolean', function () {
            it('should occured `TypeError` exception', function (done) {
              try {
                adb.begin_transaction(1);
              } catch(e) {
                e.should.be.an.instanceOf(TypeError);
                done();
              }
            });
          });
        });
        describe('end_transaction', function () {
          describe('with specific `commit` type not boolean', function () {
            it('should occured `TypeError` exception', function (done) {
              try {
                adb.end_transaction(1);
              } catch(e) {
                e.should.be.an.instanceOf(TypeError);
                done();
              }
            });
          });
        });
      });
      describe('not execute `begin_transaction`', function () {
        describe('when call `end_transaction` with specific commit -> `true`', function () {
          it('should be `INVALID` error', function (done) {
            adb.end_transaction(true, function (err) {
              err.should.have.property('code');
              err.code.should.eql(Error.INVALID);
              done();
            });
          });
        });
      });
      describe('when call `begin_transaction` with specific hard -> `true`', function () {
        beforeEach(function (done) {
          adb.begin_transaction(true, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        describe('when set key -> `key1`, value -> `hello` record', function () {
          beforeEach(function (done) {
            adb.set({ key: 'key1', value: 'hello' }, function (err) {
              if (err) { return done(err); }
              done();
            });
          });
          describe('when call `end_transaction` with specific commit -> `true`', function () {
            beforeEach(function (done) {
              adb.end_transaction(true, function (err) {
                if (err) { return done(err); }
                done();
              });
            });
            describe('when get `key1` key', function () {
              it('should be `hello` value', function (done) {
                adb.get({ key: 'key1' }, function (err, value) {
                  if (err) { return done(err); }
                  value.should.eql('hello');
                  done();
                });
              });
            });
          });
          describe('when call `end_transaction` with specific commit -> `false`', function () {
            beforeEach(function (done) {
              adb.end_transaction(false, function (err) {
                if (err) { return done(err); }
                done();
              });
            });
            describe('when get `key1` key', function () {
              it('should be `NOREC` error', function (done) {
                adb.get({ key: 'key1' }, function (err, value) {
                  err.should.have.property('code');
                  err.code.should.eql(Error.NOREC);
                  done();
                });
              });
            });
          });
          describe('when call `end_transaction` with specific commit ommit', function () {
            beforeEach(function (done) {
              adb.end_transaction(function (err) {
                if (err) { return done(err); }
                done();
              });
            });
            describe('when get `key1` key', function () {
              it('should be `hello` value', function (done) {
                adb.get({ key: 'key1' }, function (err, value) {
                  if (err) { return done(err); }
                  value.should.eql('hello');
                  done();
                });
              });
            });
          });
        });
      });
      describe('when call `begin_transaction` with specific hard -> `false`', function () {
        beforeEach(function (done) {
          adb.begin_transaction(false, function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        describe('when set key -> `key1`, value -> `hello` record', function () {
          beforeEach(function (done) {
            adb.set({ key: 'key1', value: 'hello' }, function (err) {
              if (err) { return done(err); }
              done();
            });
          });
          describe('when call `end_transaction` with specific commit ommit', function () {
            beforeEach(function (done) {
              adb.end_transaction(function (err) {
                if (err) { return done(err); }
                done();
              });
            });
            describe('when get `key1` key', function () {
              it('should be `hello` value', function (done) {
                adb.get({ key: 'key1' }, function (err, value) {
                  if (err) { return done(err); }
                  value.should.eql('hello');
                  done();
                });
              });
            });
          });
        });
      });
      describe('when call `begin_transaction` with specific hard ommit', function () {
        beforeEach(function (done) {
          adb.begin_transaction(function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        describe('when set key -> `key1`, value -> `hello` record', function () {
          beforeEach(function (done) {
            adb.set({ key: 'key1', value: 'hello' }, function (err) {
              if (err) { return done(err); }
              done();
            });
          });
          describe('when call `end_transaction` with specific commit ommit', function () {
            beforeEach(function (done) {
              adb.end_transaction(function (err) {
                if (err) { return done(err); }
                done();
              });
            });
            describe('when get `key1` key', function () {
              it('should be `hello` value', function (done) {
                adb.get({ key: 'key1' }, function (err, value) {
                  if (err) { return done(err); }
                  value.should.eql('hello');
                  done();
                });
              });
            });
          });
        });
      });
    });
    

    // 
    // transaction
    //
    describe('db not open', function () {
      describe('when call `transaction`', function () {
        it('should be `INVALID` error', function (done) {
          new DB().transaction(function (err, commit) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            commit(false);
            done();
          });
        });
      });
    });
    describe('db open', function () {
      var adb;
      var fname = 'transaction.kct';
      before(function (done) {
        adb = new DB();
        adb.open({ path: fname, mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        adb.close(function (err) {
          if (err) { return done(err); }
          fs.unlink(fname, function (err) {
            done();
          });
        });
      });
      afterEach(function (done) {
        adb.clear(function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      describe('iligale parameter check', function () {
        describe('transaction', function () {
          describe('with specific `hard` type not boolean', function () {
            it('should occured `TypeError` exception', function (done) {
              try {
                adb.begin_transaction(1);
              } catch(e) {
                e.should.be.an.instanceOf(TypeError);
                done();
              }
            });
          });
          describe('with no specific', function () {
            it('should occured `TypeError` exception', function (done) {
              try {
                adb.begin_transaction();
              } catch(e) {
                e.should.be.an.instanceOf(TypeError);
                done();
              }
            });
          });
        });
      });
      describe('when call `transaction` with specific hard -> `true`', function () {
        describe('when set key -> `key1`, value -> `hello` record', function () {
          describe('when call commit function with specific `true`', function () {
            beforeEach(function (done) {
              adb.transaction(true, function (err, commit) {
                if (err) { return done(err); }
                adb.set({ key: 'key1', value: 'hello' }, function (err) {
                  if (err) { return done(err); }
                  commit(true);
                  done();
                });
              });
            });
            describe('when get `key1` key', function () {
              it('should be `hello` value', function (done) {
                adb.get({ key: 'key1' }, function (err, value) {
                  if (err) { return done(err); }
                  value.should.eql('hello');
                  done();
                });
              });
            });
          });
          describe('when call commit function with specific `false`', function () {
            beforeEach(function (done) {
              adb.transaction(true, function (err, commit) {
                if (err) { return done(err); }
                adb.set({ key: 'key1', value: 'hello' }, function (err) {
                  if (err) { return done(err); }
                  commit(false);
                  done();
                });
              });
            });
            describe('when get `key1` key', function () {
              it('should be `NOREC` error', function (done) {
                adb.get({ key: 'key1' }, function (err, value) {
                  err.should.have.property('code');
                  err.code.should.eql(Error.NOREC);
                  done();
                });
              });
            });
          });
          describe('when call commit function with no specific', function () {
            beforeEach(function (done) {
              adb.transaction(true, function (err, commit) {
                if (err) { return done(err); }
                adb.set({ key: 'key1', value: 'hello' }, function (err) {
                  if (err) { return done(err); }
                  commit();
                  done();
                });
              });
            });
            describe('when get `key1` key', function () {
              it('should be `hello` value', function (done) {
                adb.get({ key: 'key1' }, function (err, value) {
                  if (err) { return done(err); }
                  value.should.eql('hello');
                  done();
                });
              });
            });
          });
        });
      });
      describe('when call `transaction` with specific hard -> `false`', function () {
        describe('when set key -> `key1`, value -> `hello` record', function () {
          describe('when call commit function with specific `true`', function () {
            beforeEach(function (done) {
              adb.transaction(false, function (err, commit) {
                if (err) { return done(err); }
                adb.set({ key: 'key1', value: 'hello' }, function (err) {
                  if (err) { return done(err); }
                  commit(true);
                  done();
                });
              });
            });
            describe('when get `key1` key', function () {
              it('should be `hello` value', function (done) {
                adb.get({ key: 'key1' }, function (err, value) {
                  if (err) { return done(err); }
                  value.should.eql('hello');
                  done();
                });
              });
            });
          });
        });
      });
      describe('when call `begin_transaction` with specific hard ommit', function () {
        describe('when set key -> `key1`, value -> `hello` record', function () {
          describe('when call commit function with specific `true`', function () {
            beforeEach(function (done) {
              adb.transaction(function (err, commit) {
                if (err) { return done(err); }
                adb.set({ key: 'key1', value: 'hello' }, function (err) {
                  if (err) { return done(err); }
                  commit(true);
                  done();
                });
              });
            });
            describe('when get `key1` key', function () {
              it('should be `hello` value', function (done) {
                adb.get({ key: 'key1' }, function (err, value) {
                  if (err) { return done(err); }
                  value.should.eql('hello');
                  done();
                });
              });
            });
          });
        });
      });
    });


  });
});

