var _ = require('underscore');
var mkdirp = require('mkdirp');
var fs = require('fs');
var path = require('path');

var checkToPath = module.exports.checkToPath = function (to, cb) {
	var splitPath = to.split(path.sep);
	var dirPath = _.initial(splitPath, 1).join(path.sep);

	fs.exists(dirPath, function(exists){
		return exists ? cb() : mkdirp(dirPath, cb);
	});
};
