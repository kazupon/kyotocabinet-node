// 
// import(s)
//

var assert = require('assert');
var format = require('util').format;
var EventEmitter = require('events').EventEmitter;


// 
// macro(s)
//

var promiser = function () {
  var args = arguments;
  return function () {
    var promise = new EventEmitter();
    process.nextTick(function () {
      promise.emit.apply(promise, args);
    });
    return promise;
  }
};

var emitter = function (cb) {
  var promise = new EventEmitter();
  process.nextTick(function () {
    cb(promise);
  });
  return promise;
};

var makeCheckConstantContext = function (obj, const_name, val) {
  var context = {};
  context = {
    topic: obj[const_name],
    'should be defined': function (val) {
      assert.isDefined(val);
    }
  };
  var _val = val;
  context[format('should be `%d`', _val)] = function (val) {
    assert.equal(_val, val);
  };
  return context;
};

var makeCheckConstantContexts = function (obj, const_pairs) {
  var batch = {};
  for (var const_name in const_pairs) {
    batch[format('`%s` constant', const_name)] = makeCheckConstantContext(obj, const_name, const_pairs[const_name]);
  }
  return batch;
};

module.exports = {
  promiser: promiser,
  emitter: emitter,
  makeCheckConstantContext: makeCheckConstantContext,
  makeCheckConstantContexts: makeCheckConstantContexts
};

