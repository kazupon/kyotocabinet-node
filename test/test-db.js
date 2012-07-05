// 
// import(s)
//

var should = require('should');
var fs = require('fs');
var log = console.log;
var checkConstants = require('./macro').checkConstants;
var kc = require('../lib/kyotocabinet');
var DB = kc.DB;
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
      log('db1 open');
      done();
    });
  });
  after(function (done) {
    db1.close(function (err) {
      if (err) { done(err); }
      log('db1.close');
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

    describe('when call `close` method', function () {
      it('will catch `err` callback into `INVALID` value at `code` property', function (done) {
        db.close(function (err) {
          err.should.be.a('object');
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });

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
                //console.log(err, value);
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
                //console.log(err, value);
                if (err) { done(err); }
                value.should.eql('ほえ');
                done();
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
                //console.log(err, value);
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
            //console.log(err);
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with no specific `value`', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db1.set({ key: 'hoge1' }, function (err) {
            //console.log(err);
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
      describe('with no specific `key` & `value`', function () {
        it('should catch `err` callback into `INVALID` value at `code` property', function (done) {
          db1.set({}, function (err) {
            //console.log(err);
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
    });
    describe('when call `get` method', function () {
      describe('with specific `key` -> `norecord`', function () {
        it('should catch `err` callback into `NOREC` value at `code` property', function (done) {
          db1.get({ key: 'norecord' }, function (err) {
            //console.log(err);
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
            //console.log(err);
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
            db1.set({ key: 1 });
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
      describe('with specific parameter not object', function () {
        it('should catch `TypeError` exception', function (done) {
          try {
            db1.set(1);
          } catch (e) {
            e.should.be.an.instanceOf(TypeError);
            done();
          }
        });
      });
    });

    describe('when call `clear` method', function () {
      it('will catch `success` callback', function (done) {
        db1.clear(function (err) {
          if (err) { done(err); }
          done();
        });
      });
      describe('when call `get` method', function () {
        it('should catch `err` callback into `NOREC` value at `code` property', function (done) {
          db1.get({ key: 'norecord' }, function (err) {
            //console.log(err);
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

