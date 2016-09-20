// Generated by CoffeeScript 1.10.0
(function() {
  var Grid, GridStore, Promise, _, client, concat, mime, path, server, stream, util,
    extend = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
    hasProp = {}.hasOwnProperty;

  path = require('path');

  util = require('util');

  stream = require('stream');

  concat = require('concat-stream');

  mime = require('./mime');

  _ = require('lodash');

  Grid = require('gridfs-stream');

  Promise = require('bluebird');

  server = require('mongodb');

  client = server.MongoClient;

  GridStore = server.GridStore;

  module.exports = function(opts) {

    /*
    		opts:
    			uri:	'mongodb://user:password@localhost:27017/file'
    
    			or
    
    			scheme: "mongodb"
    			hosts: [
    				{host: 'host1', port: port1}
    				{host: 'host2', port: port2}
    				...
    			]
    			username: "username"
    			password: "password"
    			database: "db"
    			options:
    				authSource: "admin"
     */
    var mongodbUri, self;
    self = {
      opts: opts
    };
    mongodbUri = require('mongodb-uri');
    _.defaults(self.opts, {
      scheme: 'mongodb',
      bucket: GridStore.DEFAULT_ROOT_COLLECTION,
      port: 27017
    });
    self.conn = client.connect(self.opts.uri || mongodbUri.format(self.opts), {
      promiseLibrary: Promise
    }).then(function(db) {
      self.db = db;
      self.gfs = Promise.promisifyAll(Grid(self.db, server));
      return self.gridStore = function(fileId, mode, opts) {
        if (opts == null) {
          opts = {
            root: self.opts.bucket
          };
        }
        return Promise.promisifyAll(new GridStore(self.db, fileId, mode, opts));
      };
    });
    return {
      ls: function(dirname, cb) {
        return self.conn.then(function() {
          var index;
          index = {
            filename: 1,
            uploadDate: -1
          };
          return new Promise(function(resolve, reject) {
            return self.gfs.collection(self.opts.bucket).ensureIndex(index, function(err, indexName) {
              if (err) {
                return reject(err);
              }
              return self.gfs.collection(self.opts.bucket).distinct('filename', {
                'metadata.dirname': dirname
              }, function(err, files) {
                if (err) {
                  return reject(err);
                }
                return resolve(files);
              });
            });
          });
        }).then(function(files) {
          return cb(null, files);
        })["catch"](cb);
      },
      read: function(fd, cb) {
        return self.conn.then((function(_this) {
          return function() {
            return _this.find(fd, -1).then(function(file) {
              return self.gridStore(file._id, 'r', {
                root: self.opts.bucket
              }).openAsync().then(function(content) {
                var out;
                out = content.stream();
                out.on('error', Promise.reject);
                return out;
              });
            });
          };
        })(this)).then(function(stream) {
          return stream.pipe(concat(function(data) {
            return cb(null, data);
          }));
        })["catch"](cb);
      },

      /*
      	version:
      		null:
      			remove all versions of the specified file
      		n:
      			remove nth version of the specified file
      		[i1, i2, ...]:
      			remove all versions listed in the array
       */
      rm: function(fd, version) {
        if (version == null) {
          version = null;
        }
        if (version != null) {
          switch (true) {
            case typeof version === 'number':
              return this.find(fd, version).then(function(file) {
                return self.gfs.removeAsync({
                  _id: file._id,
                  root: self.opts.bucket
                });
              }).then(function() {
                var ret;
                ret = {};
                ret[fd] = version;
                return Promise.resolve(ret);
              });
            case Array.isArray(version):
              return Promise.map(version, (function(_this) {
                return function(v) {
                  return _this.rm(fd, v);
                };
              })(this));
            default:
              return Promise.reject("invalid version");
          }
        } else {
          return self.conn.then(function() {
            var result;
            result = self.gfs.files.find({
              filename: fd
            });
            return Promise.promisifyAll(result).toArrayAsync().then(function(files) {
              if (files.length === 0) {
                return Promise.reject(fd + " not found");
              }
              return Promise.map(files, function(file) {
                return self.gfs.removeAsync({
                  _id: file._id,
                  root: self.opts.bucket
                });
              });
            });
          });
        }
      },
      find: function(fd, version) {
        return self.conn.then(function() {
          return new Promise(function(resolve, reject) {
            return self.gfs.collection(self.opts.bucket).find({
              filename: fd
            }).limit(1).skip(Math.abs(version) - 1).sort({
              uploadDate: version < 0 ? -1 : 1
            }).next(function(err, file) {
              if (err) {
                return reject(err);
              }
              if (!file) {
                return reject("version " + version + " of " + fd + " not found");
              }
              return resolve(file);
            });
          });
        });
      },
      receive: function(opts) {
        var Receiver;
        Receiver = (function(superClass) {
          extend(Receiver, superClass);

          function Receiver(opts) {
            if (opts == null) {
              opts = {};
            }
            _.defaults(opts, {
              objectMode: true
            });
            Receiver.__super__.constructor.call(this, opts);
          }

          Receiver.prototype._write = function(__newFile, encoding, done) {
            var fd;
            fd = __newFile.fd;
            return self.conn.then((function(_this) {
              return function() {
                var metadata, out;
                metadata = _.extend(__newFile.metadata || {}, {
                  fd: fd,
                  dirname: __newFile.dirname || path.dirname(fd)
                });
                return out = __newFile.pipe(mime(fd)).on('mime', function(type) {
                  return out.pipe(self.gfs.createWriteStream({
                    filename: fd,
                    root: self.opts.bucket,
                    content_type: type,
                    metadata: metadata
                  })).once('open', function() {
                    return __newFile.extra = _.assign({
                      fileId: this.id
                    }, metadata);
                  }).once('close', function(file) {
                    return done(null, file);
                  }).once('error', function(err) {
                    this.end();
                    return done(err);
                  });
                });
              };
            })(this));
          };

          return Receiver;

        })(stream.Writable);
        return new Receiver;
      }
    };
  };

}).call(this);
