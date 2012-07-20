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
var Error = kc.Error;
var Cursor = kc.Cursor;


// 
// test(s)
//

describe('Cursor class tests', function () {

  // 
  // create cursor object (sync)
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


  // 
  // create cursor object (async)
  //
  describe('create', function () {
    var db = new DB();
    describe('db not open', function () {
      it('cursor object should be created', function (done) {
        Cursor.create(db, function (err, cur) {
          if (err) { return done(err); }
          cur.should.be.a.ok;
          cur.should.be.an.instanceOf(Cursor);
          done();
        });
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
        Cursor.create(db, function (err, cur) {
          if (err) { return done(err); }
          cur.should.be.a.ok;
          cur.should.be.an.instanceOf(Cursor);
          done();
        });
      });
    });
    describe('no specific db object', function () {
      it('cursor object should not be created', function (done) {
        try {
          Cursor.create(function (err, cur) {});
        } catch (e) {
          //e.should.be.an.instanceOf(Error);
          error(e);
          done();
        }
      });
    });
    describe('specific not db object', function () {
      it('cursor object should not be created', function (done) {
        try {
          Cursor.create({}, function (err, cur) {});
        } catch (e) {
          //e.should.be.an.instanceOf(Error);
          error(e);
          done();
        }
      });
    });
    describe('specific primitive value', function () {
      it('cursor object should not be created', function (done) {
        try {
          Cursor.create(1, function (err, cur) {});
        } catch (e) {
          //e.should.be.an.instanceOf(Error);
          error(e);
          done();
        }
      });
    });
  });

  
  //
  // jump (async)
  //
  describe('jump', function () {
    describe('db not open', function () {
      it('operation should be failed', function (done) {
        Cursor.create(new DB(), function (err, cur) {
          if (err) { return done(err); }
          cur.jump(function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });
    describe('db open', function () {
      var db;
      var cur;
      before(function (done) {
        db = new DB();
        db.open({ path: '+', mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          db.set({ key: 'key1', value: 'hello' }, function (err) {
            if (err) { return done(err); }
            db.set({ key: 'key2', value: 'world' }, function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
      });
      after(function (done) {
        db.close(function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      beforeEach(function (done) {
        Cursor.create(db, function (err, c) {
          if (err) { return done(err); }
          cur = c;
          done();
        });
      });
      afterEach(function (done) {
        cur = null;
        done();
      });
      describe('jump to record head', function () {
        it('operation should be success', function (done) {
          cur.jump(function (err) {
            if (err) { return done(err); }
            done();
          });
        });
      });
      describe('jump to specific `key` record', function () {
        it('operation should be success', function (done) {
          cur.jump('key2', function (err) {
            if (err) { return done(err); }
            done();
          });
        });
      });
      describe('parameter check', function () {
        describe('specific `key` is not string type', function () {
          it('operation should be occured exception', function (done) {
            try {
              cur.jump(111, function (err) {});
            } catch (e) {
              error(e);
              done();
            }
          });
        });
      });
    });
  });


  //
  // jump_back (async)
  //
  describe('jump_back', function () {
    describe('db not open', function () {
      it('operation should be failed', function (done) {
        Cursor.create(new DB(), function (err, cur) {
          if (err) { return done(err); }
          cur.jump_back(function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });
    describe('db open', function () {
      describe('jump back not support', function () {
        var db;
        before(function (done) {
          db = new DB();
          db.open({ path: '-', mode: DB.OWRITER + DB.OCREATE }, function (err) {
            if (err) { return done(err); }
            db.set({ key: 'key1', value: 'hello' }, function (err) {
              if (err) { return done(err); }
              db.set({ key: 'key2', value: 'world' }, function (err) {
                if (err) { return done(err); }
                done();
              });
            });
          });
        });
        after(function (done) {
          db.close(function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        it('operation should be failed', function (done) {
          Cursor.create(new DB(), function (err, cur) {
            if (err) { return done(err); }
            cur.jump_back(function (err) {
              err.should.have.property('code');
              err.code.should.eql(Error.INVALID);
              done();
            });
          });
        });
      });
      describe('jump back support', function () {
        var db;
        var cur;
        before(function (done) {
          db = new DB();
          db.open({ path: '+', mode: DB.OWRITER + DB.OCREATE }, function (err) {
            if (err) { return done(err); }
            db.set({ key: 'key1', value: 'hello' }, function (err) {
              if (err) { return done(err); }
              db.set({ key: 'key2', value: 'world' }, function (err) {
                if (err) { return done(err); }
                done();
              });
            });
          });
        });
        after(function (done) {
          db.close(function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        beforeEach(function (done) {
          Cursor.create(db, function (err, c) {
            if (err) { return done(err); }
            cur = c;
            done();
          });
        });
        afterEach(function (done) {
          cur = null;
          done();
        });
        describe('jump back to record head', function () {
          it('operation should be success', function (done) {
            cur.jump(function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        describe('jump back to specific `key` record', function () {
          it('operation should be success', function (done) {
            cur.jump_back('key1', function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        describe('parameter check', function () {
          describe('specific `key` is not string type', function () {
            it('operation should be occured exception', function (done) {
              try {
                cur.jump_back(111, function (err) {});
              } catch (e) {
                error(e);
                done();
              }
            });
          });
        });
      });
    });
  });

  //
  // get (async)
  //
  /*
  describe('get', function () {
    describe('db not open', function () {
      it('operation should be failed', function (done) {
        Cursor.create(new DB(), function (err, cur) {
          if (err) { return done(err); }
          cur.get(function (err, rec) {
            should.not.exist(rec);
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });
    describe('cursor not support db', function () {
      var db;
      before(function (done) {
        db = new DB();
        db.open({ path: '+', mode: DB.OWRITER + DB.OCREATE }, function (err) {
          if (err) { return done(err); }
          db.set({ key: 'key1', value: 'hello' }, function (err) {
            if (err) { return done(err); }
            db.set({ key: 'key2', value: 'world' }, function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
      });
      after(function (done) {
        db.clear(function (err) {
          if (err) { return done(err); }
          db.close(function (err) {
            if (err) { return done(err); }
            done();
          });
        });
      });
      it('operation should be failed', function (done) {
        Cursor.create(db, function (err, cur) {
          if (err) { return done(err); }
          cur.get(function (err, rec) {
            console.log(err);
            should.not.exist(rec);
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });
    describe('cursor support db', function () {
      describe('no record', function () {
      });
      describe('two records', function () {
        describe('move cursor', function () {
          describe('first call', function () {
          });
          describe('second call', function () {
          });
        });
        describe('no move cursor', function () {
          describe('first call', function () {
          });
          describe('second call', function () {
          });
        });
      });
    });
  });
  */


});

