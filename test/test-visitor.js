// 
// import(s)
//

var should = require('should');
var assert = require('assert');
var checkConstants = require('./macro').checkConstants;
var kc = require('../lib/kyotocabinet');
var DB = kc.DB;
var Visitor = kc.Visitor;


// 
// test(s)
//

describe('Visitor constants tests', function () {
  checkConstants(Visitor, {
    'NOP': 0,
    'REMOVE': 1
  });
});

describe('Visitor class tests', function () {
  var visitor;
  describe('when create Visitor object by `new` operator', function () {
    it('should create a Visitor object', function () {
      visitor = new Visitor();
      visitor.should.be.an.instanceOf(Visitor);
    });
  });
});
