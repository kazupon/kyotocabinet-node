// 
// import(s)
//

var vows = require('vows');
var assert = require('assert');
var events = require('events');
var makeCheckConstantContext = require('./macro').makeCheckConstantContext;
var makeCheckConstantContexts = require('./macro').makeCheckConstantContexts;
var kc = require('../lib/kyotocabinet');
var DB = kc.DB;
var Visitor = kc.Visitor;


// 
// test(s)
//

var suite = vows.describe('kyotocabinet.Visitor tests');
suite.addBatch({
  'when create Visitor object by `new` operator': {
    topic: new Visitor(),
    'should create a Visitor object': function (vtr) {
      assert.isObject(vtr);
    }
  }
}).addBatch(makeCheckConstantContexts(Visitor, {
  'NOP': 0,
  'REMOVE': 1
})).export(module);
