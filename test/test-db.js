// 
// import(s)
//

var should = require('should');
var assert = require('assert');
var fs = require('fs');
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
  describe('when create DB object by `new` operator', function () {
    it('should create a DB object', function () {
      db = new DB();
      db.should.be.an.instanceOf(DB);
    });
    afterEach(function (done) {
      fs.unlink('casket.kct', function (err) {
        done();
      });
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
            done();
          });
        });
      });
      describe('with specific `nothing`', function () {
        it('shoule returned `this` object', function (done) {
          db.open().should.eql(db);
          done();
        });
      });
    });
    describe('when call `close` method', function () {
      it('will catch `err` callback', function (done) {
        db.close(function (err) {
          err.should.be.a('object');
          err.should.have.property('code');
          err.code.should.eql(Error.INVALID);
          done();
        });
      });
    });
  });
});

