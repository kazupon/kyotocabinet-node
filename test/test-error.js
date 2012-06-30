// 
// import(s)
//
var vows = require('vows');
var assert = require('assert');
var events = require('events');
var makeCheckConstantContext = require('./macro').makeCheckConstantContext;
var makeCheckConstantContexts = require('./macro').makeCheckConstantContexts;
var kc = require('../lib/kyotocabinet');


var suite = vows.describe('kyotocabinet.Error tests');
suite.addBatch({
}).addBatch(makeCheckConstantContexts(kc.Error, {
  'SUCCESS': 0,
  'NOIMPL': 1,
  'INVALID': 2,
  'NOREPOS': 3,
  'NOPERM': 4,
  'BROKEN': 5,
  'DUPREC': 6,
  'NOREC': 7,
  'LOGIC': 8,
  'SYSTEM': 9,
  'MISC': 15
})).export(module);
