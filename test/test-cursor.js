// 
// import(s)
//

var log = console.log;
var error = console.error;
var should = require('should');
var assert = require('assert');
var checkConstants = require('./macro').checkConstants;
var kc = require('../lib/kyotocabinet');
var DB = kc.DB;
var Cursor = kc.Cursor;


// 
// test(s)
//

describe('Cursor class tests', function () {

  // 
  // constructor
  //
  describe('constructor', function () {
    var db = new DB();
    describe('db not open', function () {
      it('cursor object should be created', function (done) {
        var cur = new Cursor(db);
        cur.should.be.a.ok;
        cur.should.be.an.instanceOf(Cursor);
        done();
      });
    });
    describe('db open', function () {
      before(function (done) {
        db.open(function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      after(function (done) {
        db.close(function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      it('cursor object should be created', function (done) {
        var cur = new Cursor(db);
        cur.should.be.a.ok;
        cur.should.be.an.instanceOf(Cursor);
        done();
      });
    });
    describe('no specific db object', function () {
      it('cursor object should not be created', function (done) {
        var cur = null;
        try {
          cur = new Cursor();
        } catch (e) {
          error(e);
        }
        should.not.exist(cur);
        done();
      });
    });
    describe('specific not db object', function () {
      it('cursor object should not be created', function (done) {
        var cur = null;
        try {
          cur = new Cursor({});
        } catch (e) {
          error(e);
        }
        should.not.exist(cur);
        done();
      });
    });
    describe('specific primitive value', function () {
      it('cursor object should not be created', function (done) {
        var cur = null;
        try {
          cur = new Cursor(1);
        } catch (e) {
          error(e);
        }
        should.not.exist(cur);
        done();
      });
    });
  });

});

