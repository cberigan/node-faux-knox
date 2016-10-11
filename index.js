var fs = require('fs');
var async = require('async');
var utils = require(__dirname + '/utils');
var path = require('path');
var join = path.join;
var Js2Xml = require('js2xml').Js2Xml;
var xml2js = require('xml2js');
var url = require('url');
var relative = path.relative;
var uuid = require('node-uuid');
var querystring = require('querystring');
var nock = require('nock');
var http = require('http');
var path = require('path');
var defaults = require('object.defaults');

var Client = module.exports = function (config) {
	defaults(config,{bucket: __dirname,headers: {}})

	if (config.bucket[config.bucket.length - 1] !== path.sep) {
		config.bucket = config.bucket + path.sep;
	}

	this.config = config;
};

Client.prototype.getFile = function (uri, headers, callback) {
	var self = this;

	if (!callback && typeof(headers) == 'function') {
		callback = headers;
		headers = {};
	}
	var stream = fs.createReadStream(path.join(self.config.bucket,uri));
	function cancelLocalListeners() {
		stream.removeListener('error', bad);
		stream.removeListener('readable', good);
	}
	function bad(e) {
		cancelLocalListeners();
		if (e.code === 'ENOENT') {
			stream.statusCode = 404;
			stream.headers = {};
			return callback(null, stream);
		}
	}
	function good() {
		stream.headers = {};
		stream.statusCode = 200;
		cancelLocalListeners();
		return callback(null, stream);
	}
	stream.on('error', bad);
	stream.on('readable', good);
	return stream;
};

Client.prototype.putFile = function (from, to, headers, callback) {
	var self = this;

	if (typeof(callback) == 'undefined') {
		callback = headers;
	}

	async.series([function (cb) {
		utils.checkToPath(path.join(self.config.bucket, to), cb);
	}, function (cb) {
		fs.stat(from, cb);
	}], function (err) {
		if (err) {
			return callback(err);
		}
		var r = fs.createReadStream(from);
		var w = fs.createWriteStream(path.join(self.config.bucket,to));

		w.on('finish', function () {
			callback(null, {headers:{}, statusCode:200});
		});
		w.on('error', function (e) {
			callback(null, {headers:{}, statusCode:404});
		});
		r.pipe(w);
	});
};

Client.prototype.putBuffer = function (buffer, to, headers, callback) {
	var self = this;

	utils.checkToPath(path.join(self.config.bucket,to), function () {
		fs.writeFile(path.join(self.config.bucket,to), buffer, function (err) {
			if (err) {
				return callback(err);
			}

			return callback(null, {headers:{}, statusCode:200});
		});
	});
};

Client.prototype.deleteFile = function (file, callback) {
	var self = this;

	fs.unlink(path.join(self.config.bucket,file), function (err) {
		return callback(null, {headers:{}, statusCode: err ? 404 : 204});
	});
};

Client.prototype.copyFile = function (from, to, callback) {
	var self = this;

	utils.checkToPath(path.join(self.config.bucket,to), function () {
		var readStream = fs.createReadStream(path.join(self.config.bucket,from));
		var writeStream = fs.createWriteStream(path.join(self.config.bucket,to));
		var isDone = false;
		var done = function (err) {
			if (isDone) return;
			isDone = true;

			if (err) {
				return callback(err);
			}

			return callback(null, {headers:{}, statusCode:200});
		};

		readStream.on('error', done);
		writeStream.on('error', done);
		writeStream.on('close', function () {
			done();
		});
		readStream.pipe(writeStream);
	});
};

Client.prototype.list = function (options, cb) {
	var self = this;
	var baseDirectory = path.join(self.config.bucket,(options.prefix || path.sep));
	utils.checkToPath(baseDirectory, function() {
		var walk = require('walk');
		var walker = walk.walk(baseDirectory);
		var files = [];

		walker.on('file', function (root, stat, next) {
			files.push({
				Key: join(relative(self.config.bucket, root), stat.name),
				Size: stat.size,
				LastModified: stat.mtime
			});
			next();
		});

		walker.on('end', function () {
			cb(null, {Contents: files});
		});
	});
};

Client.prototype.get = function(filename, headers){
	return this.request('GET', filename,headers);
}

Client.prototype.request = function(method,filename,headers){
    var self = this;
    method = method.toUpperCase();
    var url_parts = url.parse(filename,true);
    var p = path.parse(url_parts.pathname);
    var unNormalizedName = path.join(p.dir,querystring.unescape(p.base));
	const host = 'www.knox-test.com';
    const hostname = 'http://' + host;
    if( method === 'GET'){
		nock(hostname)
        .get(filename)
        .once()
        .replyWithFile(200, path.join(self.config.bucket,filename));
    }else if (method === 'POST'){
        //check multipart upload initialize
        if(url_parts.query.hasOwnProperty('uploads')){
            //init multiupload structure
            if(!self.mup){
                self.mup = {};
            }
            var uniqueUploadId = uuid.v1();
            //apply upload id to mup object
            self.mup[uniqueUploadId] = {};
            //response with upload id
            var InitiateMultipartUploadResult = {
                Bucket: self.config.bucket,
                Key: unNormalizedName,
                UploadId: uniqueUploadId
            }
            var xmlResponse = new Js2Xml('InitiateMultipartUploadResult', InitiateMultipartUploadResult);
            nock(hostname)
            .post(filename)
            .once()
            .reply(200, xmlResponse.toString())
            
        }
        //check multipart upload complete
        else if(url_parts.query.hasOwnProperty('uploadId')){

			nock(hostname)
            .post(filename)
            .once()
            .reply(200, function(uri,body,callback){
				var uri_parts = url.parse(uri,true);
                var uploadId = uri_parts.query.uploadId;
                var s3path = unNormalizedName;
                var parser = new xml2js.Parser({});
                parser.parseString(body,function(err,reqXml){
                    var uploadPartNumbers = reqXml.CompleteMultipartUpload.Part.map(function(p){
                        return p.PartNumber[0];
                    });
                    async.until(
                        function () { 
                            return uploadPartNumbers.every(function(pn){
                                return self.mup[uploadId][pn] && self.mup[uploadId][pn].length > 0; 
                            });
                        },
                        function (cb) {
                            //noop
                            cb(null);
                        },
                        function (err, n) {
                            //process multipart upload
                            var data = reqXml.CompleteMultipartUpload.Part.map(function(p){
                                return self.mup[uploadId][p.PartNumber[0]];
                            }).reduce(function(prev,curr){
                                return Buffer.concat([prev, curr]);
                            });
							utils.checkToPath(path.join(self.config.bucket,s3path), function(){
								fs.writeFile(path.join(self.config.bucket,s3path), data, function(err){
									if(err){
										callback(err);
									}else{
										//generate xml response
										var CompleteMultipartUploadResult = {
											Location: encodeURIComponent(url.resolve(hostname,self.config.bucket,s3path)),
											Bucket: self.config.bucket,
											Key: s3path
										}
										var xmlResponse = new Js2Xml('CompleteMultipartUploadResult', CompleteMultipartUploadResult);
										callback(null,xmlResponse.toString());
									}
								});
							});
								
                        }
                    );
                });
			});
        }
    }else if(method === 'PUT'){
        //check if part of multipart upload

        if(url_parts.query.hasOwnProperty('partNumber') && url_parts.query.hasOwnProperty('uploadId')){
			nock(hostname)
            .put(filename)
            .once()
            .reply(200, function(uri, data) {
				var uri_parts = url.parse(uri,true);
                var s3path = unNormalizedName;
                var uploadId = uri_parts.query.uploadId;
                var partNumber = uri_parts.query.partNumber;
                self.mup[uploadId][partNumber] = new Buffer(data, 'hex');
			});
        }
    }
	//return http request object
	var options = {
        hostname: host,
        path: filename,
        port: 80,
        method: method
    };
	return http.request(options);
}

module.exports.createClient = function (config) {
	return new Client(config);
};
