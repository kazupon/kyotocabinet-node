// 
// import(s)
//

var should = require('should');
var assert = require('assert');
var format = require('util').format;


// 
// macro(s)
//

var checkConstants = function (target, const_pairs) {
  for (var const_name in const_pairs) {
    var desc_name = format('`%s` constant', const_name);
    var expected_msg = format('should equal `%d`', const_pairs[const_name]);
    describe(desc_name, function () {
      it(expected_msg, function () {
        target[const_name].should.eql(const_pairs[const_name]);
      });
    });
  }
};

module.exports = {
  checkConstants: checkConstants
};

