"use strict";

var kyotocabinet = require('bindings')('kyotocabinet.node');
var DB = kyotocabinet.DB;
var Cursor = kyotocabinet.Cursor; 
var MapReduce = kyotocabinet.MapReduce;


Object.defineProperty(DB.prototype, 'cursor', {
  enumerable: true,
  configurable: false,
  writable: false,
  value: function () {
    var cb = arguments[0];
    Cursor.create(this, function (err, cur) {
      cb && cb(err, cur);
    });
    return this;
  }
});

Object.defineProperty(DB.prototype, 'transaction', {
  enumerable: true,
  configurable: false,
  writable: false,
  value: function () {
    var args = arguments;
    if (args.length === 0) {
      throw new TypeError('Bad Argument');
    }
    if (args.length === 1 &&
        (typeof(args[0]) !== 'boolean' & typeof(args[0]) !== 'function')) {
      throw new TypeError('Bad Argument');
    }

    var hard = false;
    var cb = null;

    if (args.length === 1) {
      if (typeof(args[0]) === 'boolean') {
        hard = args[0];
      } else {
        cb = args[0];
      }
    } else {
      hard = args[0];
      cb = args[1];
    }

    var self = this;
    return this.begin_transaction(hard, function (err) {
      if (err) { return cb(err, function () {}); }
      cb(null, function (commit) {
        var commit = (commit === undefined ? true : commit);
        self.end_transaction(commit, function (err) {
          if (err) { throw err; }
        });
      });
    });
  }
});

Object.defineProperty(DB.prototype, 'mapreduce', {
  enumerable: true,
  configurable: false,
  writable: false,
  value: function () {
    var args = arguments;
    if (args.length == 0) {
      throw new TypeError('Bad Argument');
    }
    if (args.length == 1 && typeof(args[0]) !== 'object') {
      throw new TypeError('Bad Argument');
    }
    var map = args[0].map;
    if (!map || typeof(map) !== 'function') {
      throw new TypeError('Bad Argument');
    }
    var reduce = args[0].reduce;
    if (!reduce || typeof(reduce) !== 'function') {
      throw new TypeError('Bad Argument');
    }
    var log = args[0].log;
    if (log && typeof(log) !== 'function') {
      throw new TypeError('Bad Argument');
    }
    var pre = args[0].pre;
    if (pre && typeof(pre) !== 'function') {
      throw new TypeError('Bad Argument');
    }
    var mid = args[0].mid;
    if (mid && typeof(mid) !== 'function') {
      throw new TypeError('Bad Argument');
    }
    var post = args[0].post;
    if (post && typeof(post) !== 'function') {
      throw new TypeError('Bad Argument');
    }
    var dbnum = args[0].dbnum || -1;
    if (typeof(dbnum) !== 'number') {
      throw new TypeError('Bad Argument');
    }
    var clim = args[0].clim || -1;
    if (typeof(clim) !== 'number') {
      throw new TypeError('Bad Argument');
    }
    var cbnum = args[0].cbnum || -1;
    if (typeof(cbnum) !== 'number') {
      throw new TypeError('Bad Argument');
    }
    var tmppath = args[0].tmppath || '';
    var opts = args[0].opts || 0;
    if (typeof(opts) !== 'number') {
      throw new TypeError('Bad Argument');
    }
    var cb = args[1];
    var mapreduce = new MapReduce(map, reduce, log, pre, mid, post, dbnum, clim, cbnum);
    mapreduce.emit = mapreduce.emit.bind(mapreduce); // HACK: set 'this' context to mapreduce.
    mapreduce.iter = mapreduce.iter.bind(mapreduce); // HACK: set 'this' context to mapreduce.
    mapreduce.execute(this, tmppath, opts, function (err) {
      cb && cb(err);
    });
    return this;
  }
});

module.exports = kyotocabinet;
