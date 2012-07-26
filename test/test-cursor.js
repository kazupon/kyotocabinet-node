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
var Visitor = kc.Visitor;
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
        //cur = null;
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
          //cur = null;
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
        //cur = null;
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
          //cur = null;
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
          //cur = null;
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
          //cur = null;
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
          //cur = null;
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
          //cur = null;
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


  //
  // set_value (async)
  //
  describe('set_value', function () {
    describe('db not open', function () {
      it('operation should be failed', function (done) {
        Cursor.create(new DB(), function (err, cur) {
          if (err) { return done(err); }
          cur.set_value('key1', function (err) {
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
      describe('set current record', function () {
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
          //cur = null;
          done();
        });
        it('should be `dio` value', function (done) {
          cur.set_value('dio', function (err) {
            if (err) { return done(err); }
            cur.get_value(function (err, value) {
              value.should.eql('dio');
              done();
            });
          });
        });
        describe('and set current record', function () {
          it('should be `the world` value', function (done) {
            cur.set_value('the world', function (err) {
              if (err) { return done(err); }
              cur.get_value(function (err, value) {
                value.should.eql('the world');
                done();
              });
            });
          });
        });
      });
      describe('set current record and step next to record', function () {
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
          //cur = null;
          done();
        });
        it('should be and `dio` value', function (done) {
          cur.set_value('dio', true, function (err) {
            if (err) { return done(err); }
            cur.get_value(function (err, value) {
              value.should.eql('world');
              done();
            });
          });
        });
        describe('and set current record next to record', function () {
          it('should be `brando` value', function (done) {
            cur.set_value('brando', true, function (err) {
              if (err) { return done(err); }
              done();
            });
          });
          describe('and set current record ...', function () {
            it('operation should be failed', function (done) {
              cur.set_value('muda', true, function (err) {
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
        describe('no specific', function () {
          it('operation should be occured exception', function (done) {
            try {
              cur.set_value();
            } catch (e) {
              error(e);
              done();
            }
          });
        });
        describe('no specific `value`', function () {
          it('operation should be occured exception', function (done) {
            try {
              cur.set_value(true, function (err) {});
            } catch (e) {
              error(e);
              done();
            }
          });
        });
        describe('specific `step` is not boolean type', function () {
          it('operation should be occured exception', function (done) {
            try {
              cur.set_value('hoge', 1111, function (err) {});
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
  // accept (async)
  //
  describe('accept', function () {
    describe('db not open', function () {
      //it('operation should be failed', function (done) {
      //  Cursor.create(new DB(), function (err, cur) {
      //    if (err) { return done(err); }
      //    cur.accept({
      //      visitor: function (key, value) { return Visitor.NOP; }
      //    }, function (err) {
      //      err.should.have.property('code');
      //      err.code.should.eql(Error.INVALID);
      //      done();
      //    });
      //  });
      //});
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
              db.set({ key: 'key3', value: 'hoge' }, function (err) {
                if (err) { return done(err); }
                done();
              });
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
      describe('read only', function () {
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
        describe('no step', function () {
          var visitor_call_cnt = 0;
          describe('get current record', function () {
            var key;
            var value;
            before(function (done) {
              cur.accept({
                visitor: function () {
                  visitor_call_cnt++;
                  key = arguments[0];
                  value = arguments[1];
                  return Visitor.NOP;
                },
                writable: false
              }, function (err) {
                if (err) { return done(err); }
                done();
              });
            });
            after(function (done) {
              visitor_call_cnt = 0;
              done();
            });
            it('should be `1` call `visitor`', function (done) {
              visitor_call_cnt.should.eql(1);
              done();
            });
            it('should be `key1` key', function (done) {
              key.should.eql('key1');
              done();
            });
            it('should be `hello` value', function (done) {
              value.should.eql('hello');
              done();
            });
            describe('get `key1` key record', function () {
              it('should be `hello` value', function (done) {
                db.get({ key: 'key1' }, function (err, value) {
                  if (err) { return done(err); }
                  value.should.eql('hello');
                  done();
                });
              });
            });
          });
          describe('update current record', function () {
            var key;
            var value;
            before(function (done) {
              cur.accept({
                visitor: function () {
                  visitor_call_cnt++;
                  key = arguments[0];
                  value = arguments[1];
                  return 'the';
                },
                writable: false
              }, function (err) {
                if (err) { return done(err); }
                done();
              });
            });
            after(function (done) {
              visitor_call_cnt = 0;
              done();
            });
            it('should be `1` call `visitor`', function (done) {
              visitor_call_cnt.should.eql(1);
              done();
            });
            it('should be `key1` key', function (done) {
              key.should.eql('key1');
              done();
            });
            it('should be `hello` value', function (done) {
              value.should.eql('hello');
              done();
            });
            describe('get `key1` key record', function () {
              it('should be `hello` value', function (done) {
                db.get({ key: 'key1' }, function (err, value) {
                  if (err) { return done(err); }
                  value.should.eql('hello');
                  done();
                });
              });
            });
          });
          describe('remove current record', function () {
            var key;
            var value;
            before(function (done) {
              cur.accept({
                visitor: function () {
                  visitor_call_cnt++;
                  key = arguments[0];
                  value = arguments[1];
                  return Visitor.REMOVE;
                },
                writable: false
              }, function (err) {
                if (err) { return done(err); }
                done();
              });
            });
            after(function (done) {
              visitor_call_cnt = 0;
              done();
            });
            it('should be `1` call `visitor`', function (done) {
              visitor_call_cnt.should.eql(1);
              done();
            });
            it('should be `key1` key', function (done) {
              key.should.eql('key1');
              done();
            });
            it('should be `hello` value', function (done) {
              value.should.eql('hello');
              done();
            });
            describe('get `key1` key record', function () {
              it('should be `hello` value', function (done) {
                db.get({ key: 'key1' }, function (err, value) {
                  if (err) { return done(err); }
                  value.should.eql('hello');
                  done();
                });
              });
            });
          });
        });
        describe('step', function () {
          var visitor_call_cnt = 0;
          describe('get current record', function () {
            var key;
            var value;
            before(function (done) {
              cur.accept({
                visitor: function () {
                  visitor_call_cnt++;
                  key = arguments[0];
                  value = arguments[1];
                  return Visitor.NOP;
                },
                writable: false,
                step: true
              }, function (err) {
                if (err) { return done(err); }
                done();
              });
            });
            it('should be `1` call `visitor`', function (done) {
              visitor_call_cnt.should.eql(1);
              done();
            });
            it('should be `key1` key', function (done) {
              key.should.eql('key1');
              done();
            });
            it('should be `hello` value', function (done) {
              value.should.eql('hello');
              done();
            });
            describe('get `key1` key record', function () {
              it('should be `hello` value', function (done) {
                db.get({ key: 'key1' }, function (err, value) {
                  if (err) { return done(err); }
                  value.should.eql('hello');
                  done();
                });
              });
            });
            describe('update current record', function () {
              before(function (done) {
                cur.accept({
                  visitor: function () {
                    visitor_call_cnt++;
                    key = arguments[0];
                    value = arguments[1];
                    return 'dio';
                  },
                  writable: false,
                  step: true
                }, function (err) {
                  if (err) { return done(err); }
                  done();
                });
              });
              it('should be `2` call `visitor`', function (done) {
                visitor_call_cnt.should.eql(2);
                done();
              });
              it('should be `key2` key', function (done) {
                key.should.eql('key2');
                done();
              });
              it('should be `world` value', function (done) {
                value.should.eql('world');
                done();
              });
              describe('get `key2` key record', function () {
                it('should be `world` value', function (done) {
                  db.get({ key: 'key2' }, function (err, value) {
                    if (err) { return done(err); }
                    value.should.eql('world');
                    done();
                  });
                });
              });
              describe('remove current record', function () {
                before(function (done) {
                  cur.accept({
                    visitor: function () {
                      visitor_call_cnt++;
                      key = arguments[0];
                      value = arguments[1];
                      return Visitor.REMOVE;
                    },
                    writable: false,
                    step: true
                  }, function (err) {
                    if (err) { return done(err); }
                    done();
                  });
                });
                it('should be `3` call `visitor`', function (done) {
                  visitor_call_cnt.should.eql(3);
                  done();
                });
                it('should be `key3` key', function (done) {
                  key.should.eql('key3');
                  done();
                });
                it('should be `hoge` value', function (done) {
                  value.should.eql('hoge');
                  done();
                });
                describe('get `key3` key record', function () {
                  it('should be `hoge` value', function (done) {
                    db.get({ key: 'key3' }, function (err, value) {
                      if (err) { return done(err); }
                      value.should.eql('hoge');
                      done();
                    });
                  });
                });
                describe('get record count', function () {
                  it('should be `3` count', function (done) {
                    db.count(function (err, cnt) {
                      if (err) { return done(err); }
                      cnt.should.eql(3);
                      done();
                    });
                  });
                });
              });
            });
          });
        });
      });
      describe('writable', function () {
        describe('no step', function () {
          var visitor_call_cnt = 0;
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
          describe('get current record', function () {
            var key;
            var value;
            before(function (done) {
              cur.accept({
                visitor: function () {
                  visitor_call_cnt++;
                  key = arguments[0];
                  value = arguments[1];
                  return Visitor.NOP;
                },
                writable: true
              }, function (err) {
                if (err) { return done(err); }
                done();
              });
            });
            after(function (done) {
              visitor_call_cnt = 0;
              done();
            });
            it('should be `1` call `visitor`', function (done) {
              visitor_call_cnt.should.eql(1);
              done();
            });
            it('should be `key1` key', function (done) {
              key.should.eql('key1');
              done();
            });
            it('should be `hello` value', function (done) {
              value.should.eql('hello');
              done();
            });
            describe('get `key1` key record', function () {
              it('should be `hello` value', function (done) {
                db.get({ key: 'key1' }, function (err, value) {
                  if (err) { return done(err); }
                  value.should.eql('hello');
                  done();
                });
              });
            });
          });
          describe('update current record', function () {
            var key;
            var value;
            before(function (done) {
              cur.accept({
                visitor: function () {
                  visitor_call_cnt++;
                  key = arguments[0];
                  value = arguments[1];
                  return 'the';
                },
                writable: true
              }, function (err) {
                if (err) { return done(err); }
                done();
              });
            });
            after(function (done) {
              visitor_call_cnt = 0;
              done();
            });
            it('should be `1` call `visitor`', function (done) {
              visitor_call_cnt.should.eql(1);
              done();
            });
            it('should be `key1` key', function (done) {
              key.should.eql('key1');
              done();
            });
            it('should be `hello` value', function (done) {
              value.should.eql('hello');
              done();
            });
            describe('get `key1` key record', function () {
              it('should be `the` value', function (done) {
                db.get({ key: 'key1' }, function (err, value) {
                  if (err) { return done(err); }
                  value.should.eql('the');
                  done();
                });
              });
            });
          });
          describe('remove current record', function () {
            var key;
            var value;
            before(function (done) {
              cur.accept({
                visitor: function () {
                  visitor_call_cnt++;
                  key = arguments[0];
                  value = arguments[1];
                  return Visitor.REMOVE;
                },
                writable: true
              }, function (err) {
                if (err) { return done(err); }
                done();
              });
            });
            after(function (done) {
              visitor_call_cnt = 0;
              done();
            });
            it('should be `1` call `visitor`', function (done) {
              visitor_call_cnt.should.eql(1);
              done();
            });
            it('should be `key1` key', function (done) {
              key.should.eql('key1');
              done();
            });
            it('should be `the` value', function (done) {
              value.should.eql('the');
              done();
            });
            describe('get `key1` key record', function () {
              it('should be `NOREC` error', function (done) {
                db.get({ key: 'key1' }, function (err, value) {
                  err.should.have.property('code');
                  err.code.should.eql(Error.NOREC);
                  done();
                });
              });
            });
          });
        });
        describe('step', function () {
          var visitor_call_cnt = 0;
          var cur;
          before(function (done) {
            db.set({ key: 'key1', value: 'hello' }, function (err) {
              if (err) { return done(err); }
              db.set({ key: 'key2', value: 'world' }, function (err) {
                if (err) { return done(err); }
                db.set({ key: 'key3', value: 'hoge' }, function (err) {
                  if (err) { return done(err); }
                  Cursor.create(db, function (err, c) {
                    if (err) { return done(err); }
                    cur = c;
                    cur.jump(function (err) {
                      if (err) { return done(err); }
                      done();
                    });
                  });
                });
              });
            });
          });
          describe('get current record', function () {
            var key;
            var value;
            before(function (done) {
              cur.accept({
                visitor: function () {
                  visitor_call_cnt++;
                  key = arguments[0];
                  value = arguments[1];
                  return Visitor.NOP;
                },
                writable: true,
                step: true
              }, function (err) {
                if (err) { return done(err); }
                done();
              });
            });
            it('should be `1` call `visitor`', function (done) {
              visitor_call_cnt.should.eql(1);
              done();
            });
            it('should be `key1` key', function (done) {
              key.should.eql('key1');
              done();
            });
            it('should be `hello` value', function (done) {
              value.should.eql('hello');
              done();
            });
            describe('get `key1` key record', function () {
              it('should be `hello` value', function (done) {
                db.get({ key: 'key1' }, function (err, value) {
                  if (err) { return done(err); }
                  value.should.eql('hello');
                  done();
                });
              });
            });
            describe('update current record', function () {
              before(function (done) {
                cur.accept({
                  visitor: function () {
                    visitor_call_cnt++;
                    key = arguments[0];
                    value = arguments[1];
                    return 'dio';
                  },
                  writable: true,
                  step: true
                }, function (err) {
                  if (err) { return done(err); }
                  done();
                });
              });
              it('should be `2` call `visitor`', function (done) {
                visitor_call_cnt.should.eql(2);
                done();
              });
              it('should be `key2` key', function (done) {
                key.should.eql('key2');
                done();
              });
              it('should be `world` value', function (done) {
                value.should.eql('world');
                done();
              });
              describe('get `key2` key record', function () {
                it('should be `dio` value', function (done) {
                  db.get({ key: 'key2' }, function (err, value) {
                    if (err) { return done(err); }
                    value.should.eql('dio');
                    done();
                  });
                });
              });
              describe('remove current record', function () {
                before(function (done) {
                  cur.accept({
                    visitor: function () {
                      visitor_call_cnt++;
                      key = arguments[0];
                      value = arguments[1];
                      return Visitor.REMOVE;
                    },
                    writable: true,
                    step: true
                  }, function (err) {
                    if (err) { return done(err); }
                    done();
                  });
                });
                it('should be `3` call `visitor`', function (done) {
                  visitor_call_cnt.should.eql(3);
                  done();
                });
                it('should be `key3` key', function (done) {
                  key.should.eql('key3');
                  done();
                });
                it('should be `hoge` value', function (done) {
                  value.should.eql('hoge');
                  done();
                });
                describe('get `key3` key record', function () {
                  it('should be `NOREC` error', function (done) {
                    db.get({ key: 'key3' }, function (err, value) {
                      err.should.have.property('code');
                      err.code.should.eql(Error.NOREC);
                      done();
                    });
                  });
                });
                describe('get record count', function () {
                  it('should be `2` count', function (done) {
                    db.count(function (err, cnt) {
                      if (err) { return done(err); }
                      cnt.should.eql(2);
                      done();
                    });
                  });
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
              cur.accept();
            } catch (e) {
              error(e);
              done();
            }
          });
        });
        describe('no specific `visitor`', function () {
          it('operation should be occured exception', function (done) {
            try {
              cur.accept({ writable: false, step: true }, function (err) {});
            } catch (e) {
              error(e);
              done();
            }
          });
        });
        describe('specific `visitor` is number type', function () {
          it('operation should be occured exception', function (done) {
            try {
              cur.accept({ visitor: 1, writable: false, step: true }, function (err) {});
            } catch (e) {
              error(e);
              done();
            }
          });
        });
        describe('specific `writable` is not boolean type', function () {
          it('operation should be occured exception', function (done) {
            try {
              cur.accept({
                visitor: function (key, value) { return Visitor.NOP; }, 
                writable: 11, 
                step: true 
              }, function (err) {});
            } catch (e) {
              error(e);
              done();
            }
          });
        });
        describe('specific `step` is not boolean type', function () {
          it('operation should be occured exception', function (done) {
            try {
              cur.accept({
                visitor: function (key, value) { return Visitor.NOP; }, 
                writable: false,
                step: 'string'
              }, function (err) {});
            } catch (e) {
              error(e);
              done();
            }
          });
        });
      });
      describe('concurrency', function () {
        describe('call count check', function () {
          var cur;
          var n = 1000
          before(function (done) {
            var recs = {};
            for (var i = 0; i < n; i++) {
              recs['key' + (i + 1)] = 'value' + (i + 1);
            }
            db.set_bulk({ recs: recs }, function (err) {
              if (err) { return done(err); }
              Cursor.create(db, function (err, c) {
                if (err) { return done(err); }
                cur = c;
                done();
              });
            });
          });
          it('should be called', function (done) {
            var rets = [];
            var id = setInterval(function () {
              db.get({ key: 'key33' }, function (err, value) {
                if (err) { return done(err); }
                var cnt = rets.pop();
                if (cnt && cnt === n) {
                  clearInterval(id);
                  done();
                }
              });
            }, 5);
            var cnt = 0;
            cur.jump(function (err) {
              if (err) { return done(err); }
              (function fn (cur, cb) {
                setTimeout(function () {
                  cur.accept({
                    visitor: function (key, value) {
                      cnt++;
                      key.should.be.a.ok;
                      value.should.be.a.ok;
                      return Visitor.NOP;
                    },
                    step: true
                  }, function (err) {
                    return (err ? cb() : fn(cur, cb));
                  });
                }, 5);
              })(cur, function () {
                cnt.should.eql(n);
                rets.push(cnt);
              });
            });
          });
        });
      });
    });
  });

});



