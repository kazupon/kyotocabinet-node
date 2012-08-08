// 
// import(s)
//

var should = require('should');
var assert = require('assert');
var checkConstants = require('./macro').checkConstants;
var kc = require('../lib/kyotocabinet');


// 
// test(s)
//

describe('Error constants tests', function () {
  checkConstants(kc.Error, {
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
  });
});
