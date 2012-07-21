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
            console.log(err, key, value);
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


});

