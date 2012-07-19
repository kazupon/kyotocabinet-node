"use strict";

var kyotocabinet = require('bindings')('kyotocabinet.node');
var DB = kyotocabinet.DB;
var Cursor = kyotocabinet.Cursor; 

Object.defineProperty(DB.prototype, 'cursor', {
  enumerable: true,
  configurable: false,
  get: function () {
    return new Cursor(this);
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

module.exports = kyotocabinet;
