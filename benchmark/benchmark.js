var kc = require('../lib/kyotocabinet');
var DB = kc.DB;
var Error = kc.Error;
var Visitor = kc.Visitor;
var format = require('util').format;
var log = console.log;
var error = console.error;


var usage = function () {
  var help = [
    'kyotocabinet for Node.js binding benchmark',
    'usage: benchmark.js [modes] [options]',
    '',
    'modes:',
    '  order                   Enable order database operation',
    '  wicked                  Enable wicked database operation',
    '',
    'options:',
    '  --rnd                   Enable rundom value option (for "order" mode)', 
    '  --etc                   Enable other database operation (for "order" mode)',
    '  --it                    Iteration count (for "wicked" mode)',
    '  --path                  Database file path',
    '  --rnum                  Record number',
  ].join('\n');
  log(help);
  process.exit(1);
};

var parseOptions = function (args, cb) {
  var options = {
    order: false,
    wicked: false,
    rnd: false,
    etc: false,
    it: 1,
    rnum: 0
  };

  var err = false;
  try {
    while (args.length) {
      var arg = args.shift();
      switch (arg) {
        case 'order':
          options.order = true;
          break;
        case 'wicked':
          options.wicked = true;
          break;
        case '--rnd':
          options.rnd = true;
          break;
        case '--etc':
          options.etc = true;
          break;
        case '--it':
          if (args.length && (arg = args.shift()) && !isNaN(arg)) {
            options.it = parseInt(arg, 10);
          }
          break;
        case '--rnum':
          if (args.length && (arg = args.shift()) && !isNaN(arg)) {
            options.rnum = parseInt(arg, 10);
          }
          break;
        case '--path':
          options.path = args.shift();
          break;
        default:
          err = true;
          break;
      }
    }
    if (options.order || options.wicked) {
      err ? cb(err) : cb(null, options);
    } else {
      cb(true);
    }
  } catch (e) {
    error('occured error : %s', e.message);
    cb(true);
  }
}

var setRecords = function (db, rnd, rnum, cb) {
  var cnt = 0;
  (function fn (db, rnd, rnum, cnt, cb) {
    var key = rnd ? parseInt(Math.random(rnum) * rnum).toString() : cnt.toString();
    db.set({ key: key, value: key }, function (err) {
      cnt++;
      if (err) { return cb(err); }
      return cnt === rnum ? cb() : fn(db, rnd, rnum, cnt, cb);
    });
  })(db, rnd, rnum, cnt, function (err) {
    return cb(err);
  });
};

var getRecords = function (db, rnd, rnum, cb) {
  var cnt = 0;
  (function fn (db, rnd, rnum, cnt, cb) {
    var key = rnd ? parseInt(Math.random(rnum) * rnum).toString() : cnt.toString();
    db.get({ key: key }, function (err, value) {
      cnt++;
      if (err && (!err.code || err.code !== Error.NOREC)) { return cb(err); }
      return cnt === rnum ? cb() : fn(db, rnd, rnum, cnt, cb);
    });
  })(db, rnd, rnum, cnt, function (err) {
    return cb(err);
  });
};

var removeRecords = function (db, rnd, rnum, cb) {
  var cnt = 0;
  (function fn (db, rnd, rnum, cnt, cb) {
    var key = rnd ? parseInt(Math.random(rnum) * rnum).toString() : cnt.toString();
    db.remove({ key: key }, function (err) {
      cnt++;
      if (err && (!err.code || err.code !== Error.NOREC)) { return cb(err); }
      return cnt === rnum ? cb() : fn(db, rnd, rnum, cnt, cb);
    });
  })(db, rnd, rnum, cnt, function (err) {
    return cb(err);
  });
};

var addRecords = function (db, rnd, rnum, cb) {
  var cnt = 0;
  (function fn (db, rnd, rnum, cnt, cb) {
    var key = rnd ? parseInt(Math.random(rnum) * rnum).toString() : cnt.toString();
    db.add({ key: key, value: key }, function (err) {
      cnt++;
      if (err && (!err.code || err.code !== Error.DUPREC)) { return cb(err); }
      return cnt === rnum ? cb() : fn(db, rnd, rnum, cnt, cb);
    });
  })(db, rnd, rnum, cnt, function (err) {
    return cb(err);
  });
};

var appendRecords = function (db, rnd, rnum, cb) {
  var cnt = 0;
  (function fn (db, rnd, rnum, cnt, cb) {
    var key = rnd ? parseInt(Math.random(rnum) * rnum).toString() : cnt.toString();
    db.append({ key: key, value: key }, function (err) {
      cnt++;
      if (err) { return cb(err); }
      return cnt === rnum ? cb() : fn(db, rnd, rnum, cnt, cb);
    });
  })(db, rnd, rnum, cnt, function (err) {
    return cb(err);
  });
};

var visitRecords = function (db, rnd, rnum, cb) {
  var cnt = 0;
  var cnter = 0;
  var visit_full = function (key, value) {
    cnter++;
    var ret = Visitor.NOP;
    if (rnd) {
      var num = parseInt(Math.random(7) * 7);
      ret = (num === 1 ? (cnter.toString() + cnter.toString()) : Visitor.REMOVE);
    }
    return ret;
  };
  (function fn (db, visit_full, rnd, rnum, cnt, cb) {
    var key = rnd ? parseInt(Math.random(rnum) * rnum).toString() : cnt.toString();
    db.accept({ key: key, visitor: { visit_full: visit_full }, writable: rnd }, function (err) {
      cnt++;
      if (err) { return cb(err); }
      return cnt === rnum ? cb() : fn(db, visit_full, rnd, rnum, cnt, cb);
    });
  })(db, visit_full, rnd, rnum, cnt, function (err) {
    return cb(err);
  });
};

var iterateRecords = function (db, rnd, rnum, cb) {
  var cnt = 0;
  var visit_full = function (key, value) {
    cnt++;
    var ret = Visitor.NOP;
    if (rnd) {
      var num = parseInt(Math.random(7) * 7);
      ret = (num === 1 ? (cnt.toString() + cnt.toString()) : Visitor.REMOVE);
    }
    return ret;
  };
  (function fn (db, visit_full, rnd, cb) {
    db.iterate({ visitor: { visit_full: visit_full }, writable: rnd }, function (err) {
      if (err && (!err.code || err.code === Error.NOREC)) {
        return cb();
      } else {
        return cb(err);
      }
      return fn(db, visit_full, rnd, cb);
    });
  })(db, visit_full, rnd, function (err) {
    return cb(err);
  });
};

var iterateRecordsByCursor = function (db, rnd, rnum, cb) {
  var cnt = 0;
  var visitor = function (key, value) {
    cnt++;
    var ret = Visitor.NOP;
    if (rnd) {
      var num = parseInt(Math.random(7) * 7);
      ret = (num === 1 ? (cnt.toString() + cnt.toString()) : Visitor.REMOVE);
    }
    return ret;
  };
  db.cursor(function (err, cur) {
    if (err) { return cb(err); }
    cur.jump(function (err) {
      (function fn (cur, visitor, rnd,  cb) {
        cur.accept({ visitor: visitor, writable: rnd, step: false }, function (err) {
          if (err && (!err.code || err.code !== Error.NOREC)) { return cb(err); }
          cur.step(function (err) {
            if (err && (!err.code || err.code === Error.NOREC)) {
              return cb();
            } else if (err) {
              return cb(err);
            } else {
              return fn(cur, visitor, rnd, cb);
            }
          });
        });
      })(cur, visitor, rnd, function (err) {
        return cb(err);
      });
    });
  });
};


var orderProc = function (opts, cb) {
  log('Order Test:');
  log('path=%s rnum=%d rnd=%s etc=%s', opts.path, opts.rnum, opts.rnd, opts.etc);

  var db = new DB();
  log('openning the database:');
  var time = process.hrtime();
  db.open({ path: opts.path, mode: DB.OWRITER + DB.OCREATE + DB.OTRUNCATE }, function (err) {
    if (err) {
      error('database open error: %s(%d)', err.message, err.code);
    }
    time = process.hrtime(time);
    log('time: %d sec %d nano sec ', time[0], time[1]);

    var etcProc1 = function (db, opts, cb) {
      var e;
      log('adding records:');
      var time = process.hrtime();
      addRecords(db, opts.rnd, opts.rnum, function (err) {
        if (err) {
          error('add records error: %s(%d)', err.message, err.code);
          e = err;
        }
        time = process.hrtime(time);
        log('time: %d sec %d nano sec ', time[0], time[1]);

        log('appending records:');
        time = process.hrtime();
        appendRecords(db, opts.rnd, opts.rnum, function (err) {
          if (err) {
            error('append records error: %s(%d)', err.message, err.code);
            e = err;
          }
          time = process.hrtime(time);
          log('time: %d sec %d nano sec ', time[0], time[1]);

          log('accepting visitors:');
          time = process.hrtime();
          visitRecords(db, opts.rnd, opts.rnum, function (err) {
            if (err) {
              error('accept visitors error: %s(%d)', err.message, err.code);
              e = err;
            }
            time = process.hrtime(time);
            log('time: %d sec %d nano sec ', time[0], time[1]);

            return cb(e);
          });
        });
      });
    };

    var etcProc2 = function (db, opts, cb) {
      var e;
      log('traversing the database by the inner iterator:');
      var time = process.hrtime();
      iterateRecords(db, opts.rnd, opts.rnum, function (err) {
        if (err) {
          error('iterate records error: %s(%d)', err.message, err.code);
          e = err;
        }
        time = process.hrtime(time);
        log('time: %d sec %d nano sec ', time[0], time[1]);

        log('traversing the database by the outer cursor:');
        time = process.hrtime();
        iterateRecordsByCursor(db, opts.rnd, opts.rnum, function (err) {
          if (err) {
            error('iterate records by cursor error: %s(%d)', err.message, err.code);
            e = err;
          }
          time = process.hrtime(time);
          log('time: %d sec %d nano sec ', time[0], time[1]);

          return cb(e);
        });
      });
    };

    var cleanProc = function (db, opts, cb) {
      var e;
      log('removing records:');
      var time = process.hrtime();
      removeRecords(db, opts.rnd, opts.rnum, function (err) {
        if (err) {
          error('remove records error: %s(%d)', err.message, err.code);
          e = err;
        }
        time = process.hrtime(time);
        log('time: %d sec %d nano sec ', time[0], time[1]);

        log('closing the database:');
        time = process.hrtime();
        db.close(function (err) {
          if (err) {
            error('database close error: %s(%d)', err.message, err.code);
            e = err;
          }
          time = process.hrtime(time);
          log('time: %d sec %d nano sec ', time[0], time[1]);

          return cb(e);
        });
      });
    };

    log('setting records:');
    time = process.hrtime();
    setRecords(db, opts.rnd, opts.rnum, function (err) {
      if (err) {
        error('database set error: %s(%d)', err.message, err.code);
      }
      time = process.hrtime(time);
      log('time: %d sec %d nano sec ', time[0], time[1]);

      if (opts.etc) {
        etcProc1(db, opts, function (err) {
          log('getting records:');
          time = process.hrtime();
          getRecords(db, opts.rnd, opts.rnum, function (err) {
            if (err) {
              error('database get error: %s(%d)', err.message, err.code);
            }
            time = process.hrtime(time);
            log('time: %d sec %d nano sec ', time[0], time[1]);

            etcProc2(db, opts, function (err) {
              cleanProc(db, opts, function (err) {
              });
            });
          });
        });
      } else {
        log('getting records:');
        time = process.hrtime();
        getRecords(db, opts.rnd, opts.rnum, function (err) {
          if (err) {
            error('database get error: %s(%d)', err.message, err.code);
          }
          time = process.hrtime(time);
          log('time: %d sec %d nano sec ', time[0], time[1]);

          cleanProc(db, opts, function (err) {
          });
        });
      }
    });
  });
};

var wickedProc = function (opts) {
  log('Wicked Test:');
  log('path=%s rnum=%d it=%d', opts.path, opts.rnum, opts.it);

  var cnt = 0;
  var rnd = false;
  var visit_full = function (key, value) {
    cnt++;
    var ret = Visitor.NOP;
    if (rnd) {
      var num = parseInt(Math.random(7) * 7);
      ret = (num === 1 ? cnt.toString() : Visitor.REMOVE);
    }
    return ret;
  };
  var visit_empty = function (key) {
    return visit_full(key, key);
  };
  var visitor = {
    visit_full: visit_full,
    visit_empty: visit_empty
  };

  var db = new DB();
  var itcnt = 0;
  (function fn (db, opts, itcnt, cb) {
    var rnum = opts.rnum;
    var it = opts.it;

    log('iteration %d/%d:', (itcnt + 1), it);
    var time = process.hrtime();
    var mode = DB.OWRITER + DB.OCREATE;
    if (itcnt === 0) {
      mode = mode + DB.OTRUNCATE;
    }
    db.open({ path: opts.path, mode: mode }, function (err) {
      if (err) {
        error('database open error: %s(%d)', err.message, err.code);
      }
      db.cursor(function (err, cur) {
        if (err) {
          error('create cursor error: %s(%d)', err.message, err.code);
        }
        var reccnt = 0;
        (function proc (db, cur, recnum, reccnt, cb) {
          //log('proc %d/%d', reccnt, recnum);
          var tran = parseInt(Math.random(100) * 100) === 1;
          var operateDB = function (db, cur, visitor, recnum, cb) {
            var key = parseInt(Math.random(recnum) * recnum).toString();
            var cmd = parseInt(Math.random(12) * 12);
            switch (cmd) {
              case 1:
                db.set({ key: key, value: key }, function (err) {
                  return cb(err);
                });
                break;
              case 2:
                db.add({ key: key, value: key }, function (err) {
                  return ((err && (!err.code || err.code !== Error.DUPREC) || err) ? cb(err) : cb());
                });
                break;
              case 3:
                db.replace({ key: key, value: key }, function (err) {
                  return ((err && (!err.code || err.code !== Error.NOREC) || err) ? cb(err) : cb());
                });
                break;
              case 4:
                db.append({ key: key, value: key }, function (err) {
                  return cb(err);
                });
                break;
              case 5:
                if (parseInt(Math.random(2) * 2) === 1) {
                  var num = parseInt(Math.random(10) * 10);
                  db.increment({ key: key, num: num }, function (err) {
                    return ((err && (!err.code || err.code !== Error.LOGIC) || err) ? cb(err) : cb());
                  });
                } else {
                  var num = Math.random(10) * 10;
                  db.increment_double({ key: key, num: num }, function (err) {
                    return ((err && (!err.code || err.code !== Error.LOGIC) || err) ? cb(err) : cb());
                  });
                }
                break;
              case 6:
                db.cas({ key: key, oval: key, nval: key }, function (err) {
                  return ((err && (!err.code || err.code !== Error.LOGIC) || err) ? cb(err) : cb());
                });
                break;
              case 7:
                db.remove({ key: key }, function (err) {
                  return ((err && (!err.code || err.code !== Error.NOREC) || err) ? cb(err) : cb());
                });
                break;
              case 8:
                db.accept({ key: key, visitor: visitor, writable: true }, function (err) {
                  return cb(err);
                });
                break;
              case 9:
                if (parseInt(Math.random(10) * 10) === 1) {
                  if (parseInt(Math.random(4) * 4) === 1) {
                    cur.jump_back(function (err) {
                      return ((err && (!err.code || err.code !== Error.NOREC) || err) ? cb(err) : cb());
                    });
                  } else {
                    cur.jump(function (err) {
                      return ((err && (!err.code || err.code !== Error.NOREC) || err) ? cb(err) : cb());
                    });
                  }
                } else {
                  var afterProc = function (db, cur, recnum, cb) {
                    var match_num = (recnum / 50) + 1;
                    if (parseInt(Math.random(2) * 2) === 1) {
                      cur.step(function (err) {
                        return ((err && (!err.code || err.code !== Error.NOREC) || err) ? cb(err) : cb());
                      });
                    } else if (parseInt(Math.random(match_num) * match_num) === 1) {
                      db.match_prefix({ prefix: '1', max: parseInt(Math.random(10) * 10) }, function (err, keys) {
                        return cb(err);
                      });
                    } else if (parseInt(Math.random(match_num) * match_num) === 1) {
                      db.match_regex({ regex: /1/, max: parseInt(Math.random(10) * 10) }, function (err, keys) {
                        return ((err && (!err.code || err.code !== Error.LOGIC) || err) ? cb(err) : cb());
                      });
                    } else if (parseInt(Math.random(match_num) * match_num) === 1) {
                      db.match_similar({ origin: '1', max: parseInt(Math.random(10) * 10) }, function (err, keys) {
                        return cb(err);
                      });
                    } else {
                      return cb();
                    }
                  };
                  var sub_cmd = parseInt(Math.random(6) * 6);
                  switch (sub_cmd) {
                    case 1:
                      cur.get_key(function (err, key) {
                        if ((err && (!err.code || err.code !== Error.NOREC) || err)) {
                          return cb(err);
                        }
                        afterProc(db, cur, recnum, function (err) {
                          return cb(err);
                        });
                      });
                      break;
                    case 2:
                      cur.get_value(function (err, value) {
                        if ((err && (!err.code || err.code !== Error.NOREC) || err)) {
                          return cb(err);
                        }
                        afterProc(db, cur, recnum, function (err) {
                          return cb(err);
                        });
                      });
                      break;
                    case 3:
                      cur.get(function (err, key, value) {
                        if ((err && (!err.code || err.code !== Error.NOREC) || err)) {
                          return cb(err);
                        }
                        afterProc(db, cur, recnum, function (err) {
                          return cb(err);
                        });
                      });
                      break;
                    case 4:
                      cur.remove(function (err) {
                        if ((err && (!err.code || err.code !== Error.NOREC) || err)) {
                          return cb(err);
                        }
                        afterProc(db, cur, recnum, function (err) {
                          return cb(err);
                        });
                      });
                      break;
                    default:
                      cur.accept({
                        visitor: visitor.visit_full, 
                        writable: true, 
                        step: (parseInt(Math.random(2) * 2) === 1)
                      }, function (err) {
                        if ((err && (!err.code || err.code !== Error.NOREC) || err)) {
                          return cb(err);
                        }
                        afterProc(db, cur, recnum, function (err) {
                          return cb(err);
                        });
                      });
                      break;
                  }
                }
                break;
              default:
                db.get({ key: key }, function (err, value) {
                  return ((err && (!err.code || err.code !== Error.NOREC) || err) ? cb(err) : cb());
                });
                break;
            }
          };

          if (tran) {
            db.begin_transaction((parseInt(Math.random(recnum) * recnum) === 1), function (err) {
              if (err) {
                error('begin_transaction error: %s(%d)', err.message, err.code);
                tran = false;
              }
              operateDB(db, cur, visitor, recnum, function (err) {
                if (tran) {
                  db.end_transaction((parseInt(Math.random(10) * 10) > 1), function (err) {
                    if (err) {
                      error('end_transaction error: %s(%d)', err.message, err.code);
                    }
                    reccnt++;
                    return (recnum === reccnt ? cb(err) : proc(db, cur, recnum, reccnt, cb));
                  });
                } else {
                  reccnt++;
                  return (recnum === reccnt ? cb(err) : proc(db, cur, recnum, reccnt, cb));
                }
              });
            });
          } else {
            operateDB(db, cur, visitor, recnum, function (err) {
              reccnt++;
              return (recnum === reccnt ? cb(err) : proc(db, cur, recnum, reccnt, cb));
            });
          }

        })(db, cur, opts.rnum, reccnt, function (err) {
          db.close(function (err) {
            if (err) {
              error('database close error: %s(%d)', err.message, err.code);
            }
            time = process.hrtime(time);
            log('time: %d sec %d nano sec ', time[0], time[1]);
            itcnt++;
            return (itcnt === it ? cb(err) : fn(db, opts, itcnt, cb));
          });
        });
      });
    });

  })(db, opts, itcnt, function (err) {
  });
};

var main = function (opts) {
  if (opts.order) {
    return orderProc(opts);
  } else if (opts.wicked) {
    return wickedProc(opts);
  } else {
    return usage();
  }
};


if (require.main === module) {
  parseOptions(process.argv.slice(2), function (err, opts) {
    if (err) {
      return usage();
    }
    return main(opts);
  });
}
