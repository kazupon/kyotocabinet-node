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
      describe('jump to head record', function () {
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
        describe('no specific', function () {
          it('operation should be occured exception', function (done) {
            try {
              cur.jump();
            } catch (e) {
              error(e);
              done();
            }
          });
        });
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
        describe('jump back to last record', function () {
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
          describe('no specific', function () {
            it('operation should be occured exception', function (done) {
              try {
                cur.jump_back();
              } catch (e) {
                error(e);
                done();
              }
            });
          });
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
  // step (async)
  //
  describe('step', function () {
    describe('db not open', function () {
      it('operation should be failed', function (done) {
        Cursor.create(new DB(), function (err, cur) {
          if (err) { return done(err); }
          cur.step(function (err) {
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
      describe('step to next record', function () {
        it('operation should be success', function (done) {
          cur.jump(function (err) {
            if (err) { return done(err); }
            cur.step(function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        describe('and step to next record', function () {
          it('operation should be failed', function (done) {
            cur.step(function (err) {
              err.should.have.property('code');
              err.code.should.eql(Error.NOREC);
              done();
            });
          });
          describe('and step to ...', function () {
            it('operation should be failed', function (done) {
              cur.step(function (err) {
                err.should.have.property('code');
                err.code.should.eql(Error.NOREC);
                done();
              });
            });
          });
        });
      });
      describe('parameter check', function () {
        describe('no specific', function () {
          it('operation should be occured exception', function (done) {
            try {
              cur.step();
            } catch (e) {
              error(e);
              done();
            }
          });
        });
        describe('specific no function', function () {
          it('operation should be occured exception', function (done) {
            try {
              cur.step(111);
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
  // step_back (async)
  //
  describe('step_back', function () {
    describe('db not open', function () {
      it('operation should be failed', function (done) {
        Cursor.create(new DB(), function (err, cur) {
          if (err) { return done(err); }
          cur.step_back(function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });
    describe('db open', function () {
      describe('step back not support', function () {
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
            cur.step_back(function (err) {
              err.should.have.property('code');
              err.code.should.eql(Error.INVALID);
              done();
            });
          });
        });
      });
      describe('step back support', function () {
        var db;
        var cur;
        before(function (done) {
          db = new DB();
          db.open({ path: '%', mode: DB.OWRITER + DB.OCREATE }, function (err) {
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
        describe('step back to prev record', function () {
          it('operation should be success', function (done) {
            cur.jump_back(function (err) {
              if (err) { return done(err); }
              cur.step_back(function (err) {
                if (err) { return done(err); }
                done();
              });
            });
          });
          describe('and step back to prev record', function () {
            it('operation should be failed', function (done) {
              cur.step_back(function (err) {
                err.should.have.property('code');
                err.code.should.eql(Error.NOREC);
                done();
              });
            });
            describe('and step back to ...', function () {
              it('operation should be failed', function (done) {
                cur.step_back(function (err) {
                  err.should.have.property('code');
                  err.code.should.eql(Error.NOREC);
                  done();
                });
              });
            });
          });
        });
        describe('parameter check', function () {
          describe('no specific', function () {
            it('operation should be occured exception', function (done) {
              try {
                cur.step_back();
              } catch (e) {
                error(e);
                done();
              }
            });
          });
          describe('specific `key` is not string type', function () {
            it('operation should be occured exception', function (done) {
              try {
                cur.step_back(111);
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
  describe('get', function () {
    describe('db not open', function () {
      it('operation should be failed', function (done) {
        Cursor.create(new DB(), function (err, cur) {
          if (err) { return done(err); }
          cur.get(function (err, key, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });
    describe('db open', function () {
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
        db.close(function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      describe('get current record', function () {
        var cur;
        before(function (done) {
          Cursor.create(db, function (err, c) {
            if (err) { return done(err); }
            cur = c;
            cur.jump(function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        after(function (done) {
          cur = null;
          done();
        });
        it('should be `key1` key and `hello` value', function (done) {
          cur.get(function (err, key, value) {
            if (err) { return done(err); }
            key.should.eql('key1');
            value.should.eql('hello');
            done();
          });
        });
        describe('and get current record', function () {
          it('should be `key1` key and `hello` value', function (done) {
            cur.get(function (err, key, value) {
              if (err) { return done(err); }
              key.should.eql('key1');
              value.should.eql('hello');
              done();
            });
          });
        });
      });
      describe('get current record and step next to record', function () {
        var cur;
        before(function (done) {
          Cursor.create(db, function (err, c) {
            if (err) { return done(err); }
            cur = c;
            cur.jump(function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        after(function (done) {
          cur = null;
          done();
        });
        it('should be `key1` key and `hello` value', function (done) {
          cur.get(true, function (err, key, value) {
            if (err) { return done(err); }
            key.should.eql('key1');
            value.should.eql('hello');
            done();
          });
        });
        describe('and get current record next to record', function () {
          it('should be `key2` key and `world` value', function (done) {
            cur.get(true, function (err, key, value) {
              if (err) { return done(err); }
              key.should.eql('key2');
              value.should.eql('world');
              done();
            });
          });
          describe('and get current record ...', function () {
            it('operation should be failed', function (done) {
              cur.get(true, function (err) {
                err.should.have.property('code');
                err.code.should.eql(Error.NOREC);
                done();
              });
            });
          });
        });
      });
      describe('parameter check', function () {
        var cur;
        before(function (done) {
          Cursor.create(db, function (err, c) {
            if (err) { return done(err); }
            cur = c;
            done();
          });
        });
        describe('specific `step` is not boolean type', function () {
          it('operation should be occured exception', function (done) {
            try {
              cur.get(1111, function (err, key, value) {});
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
  // get_key (async)
  //
  describe('get_key', function () {
    describe('db not open', function () {
      it('operation should be failed', function (done) {
        Cursor.create(new DB(), function (err, cur) {
          if (err) { return done(err); }
          cur.get_key(function (err, key, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });
    describe('db open', function () {
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
        db.close(function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      describe('get current record', function () {
        var cur;
        before(function (done) {
          Cursor.create(db, function (err, c) {
            if (err) { return done(err); }
            cur = c;
            cur.jump(function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        after(function (done) {
          cur = null;
          done();
        });
        it('should be `key1` key', function (done) {
          cur.get_key(function (err, key) {
            if (err) { return done(err); }
            key.should.eql('key1');
            done();
          });
        });
        describe('and get current record key', function () {
          it('should be `key1` key', function (done) {
            cur.get_key(function (err, key) {
              if (err) { return done(err); }
              key.should.eql('key1');
              done();
            });
          });
        });
      });
      describe('get current record key and step next to record', function () {
        var cur;
        before(function (done) {
          Cursor.create(db, function (err, c) {
            if (err) { return done(err); }
            cur = c;
            cur.jump(function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        after(function (done) {
          cur = null;
          done();
        });
        it('should be `key1` key', function (done) {
          cur.get_key(true, function (err, key) {
            if (err) { return done(err); }
            key.should.eql('key1');
            done();
          });
        });
        describe('and get current record key next to record', function () {
          it('should be `key2` key', function (done) {
            cur.get_key(true, function (err, key) {
              if (err) { return done(err); }
              key.should.eql('key2');
              done();
            });
          });
          describe('and get current record key ...', function () {
            it('operation should be failed', function (done) {
              cur.get_key(true, function (err, key) {
                err.should.have.property('code');
                err.code.should.eql(Error.NOREC);
                done();
              });
            });
          });
        });
      });
      describe('parameter check', function () {
        var cur;
        before(function (done) {
          Cursor.create(db, function (err, c) {
            if (err) { return done(err); }
            cur = c;
            done();
          });
        });
        describe('specific `step` is not boolean type', function () {
          it('operation should be occured exception', function (done) {
            try {
              cur.get_key(1111, function (err, key) {});
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
  // get_value (async)
  //
  describe('get_value', function () {
    describe('db not open', function () {
      it('operation should be failed', function (done) {
        Cursor.create(new DB(), function (err, cur) {
          if (err) { return done(err); }
          cur.get_value(function (err, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });
    describe('db open', function () {
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
        db.close(function (err) {
          if (err) { return done(err); }
          done();
        });
      });
      describe('get current record value', function () {
        var cur;
        before(function (done) {
          Cursor.create(db, function (err, c) {
            if (err) { return done(err); }
            cur = c;
            cur.jump(function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        after(function (done) {
          cur = null;
          done();
        });
        it('should be `hello` value', function (done) {
          cur.get_value(function (err, value) {
            if (err) { return done(err); }
            value.should.eql('hello');
            done();
          });
        });
        describe('and get current record value', function () {
          it('should be `hello` value', function (done) {
            cur.get_value(function (err, value) {
              if (err) { return done(err); }
              value.should.eql('hello');
              done();
            });
          });
        });
      });
      describe('get current record value and step next to record', function () {
        var cur;
        before(function (done) {
          Cursor.create(db, function (err, c) {
            if (err) { return done(err); }
            cur = c;
            cur.jump(function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        after(function (done) {
          cur = null;
          done();
        });
        it('should be `hello` value', function (done) {
          cur.get_value(true, function (err, value) {
            if (err) { return done(err); }
            value.should.eql('hello');
            done();
          });
        });
        describe('and get current record value next to record', function () {
          it('should be `world` value', function (done) {
            cur.get_value(true, function (err, value) {
              if (err) { return done(err); }
              value.should.eql('world');
              done();
            });
          });
          describe('and get current record value ...', function () {
            it('operation should be failed', function (done) {
              cur.get_value(true, function (err, value) {
                err.should.have.property('code');
                err.code.should.eql(Error.NOREC);
                done();
              });
            });
          });
        });
      });
      describe('parameter check', function () {
        var cur;
        before(function (done) {
          Cursor.create(db, function (err, c) {
            if (err) { return done(err); }
            cur = c;
            done();
          });
        });
        describe('specific `step` is not boolean type', function () {
          it('operation should be occured exception', function (done) {
            try {
              cur.get_value(1111, function (err, value) {});
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
  // remove (async)
  //
  describe('remove', function () {
    describe('db not open', function () {
      it('operation should be failed', function (done) {
        Cursor.create(new DB(), function (err, cur) {
          if (err) { return done(err); }
          cur.remove(function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });
    describe('db open', function () {
      var db;
      before(function (done) {
        db = new DB();
        db.open({ path: '+', mode: DB.OWRITER + DB.OCREATE }, function (err) {
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
      describe('not registed record', function () {
        var cur;
        before(function (done) {
          Cursor.create(db, function (err, c) {
            if (err) { return done(err); }
            cur = c;
            done();
          });
        });
        it('operation should be failed', function (done) {
          cur.remove(function (err) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
      });
      describe('registed 2 record', function () {
        before(function (done) {
          db.set({ key: 'key1', value: 'hello' }, function (err) {
            if (err) { return done(err); }
            db.set({ key: 'key2', value: 'world' }, function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        after(function (done) {
          db.clear(function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        describe('remove current record', function () {
          var cur;
          before(function (done) {
            Cursor.create(db, function (err, c) {
              if (err) { return done(err); }
              cur = c;
              cur.jump(function (err) {
                if (err) { return done(err); }
                done();
              });
            });
          });
          it('should be success', function (done) {
            cur.remove(function (err) {
              if (err) { return done(err); }
              db.count(function (err, cnt) {
                if (err) { return done(err); }
                cnt.should.eql(1);
                done();
              });
            });
          });
          describe('and remove current record', function () {
            it('should be success', function (done) {
              cur.remove(function (err) {
                if (err) { return done(err); }
                db.count(function (err, cnt) {
                  if (err) { return done(err); }
                  cnt.should.eql(0);
                  done();
                });
              });
            });
            describe('and remove ...', function () {
              it('operation should be failed', function (done) {
                cur.remove(function (err) {
                  err.should.have.property('code');
                  err.code.should.eql(Error.NOREC);
                  done();
                });
              });
            });
          });
        });
      });
      describe('parameter check', function () {
        var cur;
        before(function (done) {
          Cursor.create(db, function (err, c) {
            if (err) { return done(err); }
            cur = c;
            done();
          });
        });
        describe('no specific', function () {
          it('operation should be occured exception', function (done) {
            try {
              cur.remove();
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
  // seize (async)
  //
  describe('seize', function () {
    describe('db not open', function () {
      it('operation should be failed', function (done) {
        Cursor.create(new DB(), function (err, cur) {
          if (err) { return done(err); }
          cur.seize(function (err, key, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.INVALID);
            done();
          });
        });
      });
    });
    describe('db open', function () {
      var db;
      before(function (done) {
        db = new DB();
        db.open({ path: '+', mode: DB.OWRITER + DB.OCREATE }, function (err) {
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
      describe('not registed record', function () {
        var cur;
        before(function (done) {
          Cursor.create(db, function (err, c) {
            if (err) { return done(err); }
            cur = c;
            done();
          });
        });
        it('operation should be failed', function (done) {
          cur.seize(function (err, key, value) {
            err.should.have.property('code');
            err.code.should.eql(Error.NOREC);
            done();
          });
        });
      });
      describe('registed 2 record', function () {
        before(function (done) {
          db.set({ key: 'key1', value: 'hello' }, function (err) {
            if (err) { return done(err); }
            db.set({ key: 'key2', value: 'world' }, function (err) {
              if (err) { return done(err); }
              done();
            });
          });
        });
        after(function (done) {
          db.clear(function (err) {
            if (err) { return done(err); }
            done();
          });
        });
        describe('seize current record', function () {
          var cur;
          before(function (done) {
            Cursor.create(db, function (err, c) {
              if (err) { return done(err); }
              cur = c;
              cur.jump(function (err) {
                if (err) { return done(err); }
                done();
              });
            });
          });
          it('should be success', function (done) {
            cur.seize(function (err, key, value) {
              if (err) { return done(err); }
              key.should.eql('key1');
              value.should.eql('hello');
              db.count(function (err, cnt) {
                if (err) { return done(err); }
                cnt.should.eql(1);
                done();
              });
            });
          });
          describe('and seize current record', function () {
            it('should be success', function (done) {
              cur.seize(function (err, key, value) {
                if (err) { return done(err); }
                key.should.eql('key2');
                value.should.eql('world');
                db.count(function (err, cnt) {
                  if (err) { return done(err); }
                  cnt.should.eql(0);
                  done();
                });
              });
            });
            describe('and seize ...', function () {
              it('operation should be failed', function (done) {
                cur.seize(function (err, key, value) {
                  err.should.have.property('code');
                  err.code.should.eql(Error.NOREC);
                  done();
                });
              });
            });
          });
        });
      });
      describe('parameter check', function () {
        var cur;
        before(function (done) {
          Cursor.create(db, function (err, c) {
            if (err) { return done(err); }
            cur = c;
            done();
          });
        });
        describe('no specific', function () {
          it('operation should be occured exception', function (done) {
            try {
              cur.seize();
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

