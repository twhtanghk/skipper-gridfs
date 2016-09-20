// Generated by CoffeeScript 1.10.0
(function() {
  var Magic, Mime, Promise, magic, mimetype, mmm, stream,
    bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; },
    extend = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
    hasProp = {}.hasOwnProperty;

  Promise = require('bluebird');

  stream = require('stream');

  mmm = require('mmmagic');

  Magic = mmm.Magic;

  magic = new Magic(mmm.MAGIC_MIME);

  mimetype = require('mime-types');

  Mime = (function(superClass) {
    extend(Mime, superClass);

    Mime.prototype.buffer = new Buffer(0);

    Mime.prototype.type = null;

    Mime.magicSize = 16384;

    function Mime(opts) {
      this.setType = bind(this.setType, this);
      this.on('finish', (function(_this) {
        return function() {
          return _this.check();
        };
      })(this));
      Mime.__super__.constructor.call(this, opts);
    }

    Mime.prototype._transform = function(chunk, encoding, cb) {
      this.push(chunk);
      if (Buffer.concat(this._readableState.buffer).length >= Mime.magicSize) {
        this.check();
      }
      return cb();
    };

    Mime.prototype.check = function() {
      if (typeof type === "undefined" || type === null) {
        return magic.detect(Buffer.concat(this._readableState.buffer), (function(_this) {
          return function(err, res) {
            var encoding, org, ref, type;
            ref = res.match(/^(.*); charset=(.*)$/), org = ref[0], type = ref[1], encoding = ref[2];
            return _this.setType(type);
          };
        })(this));
      }
    };

    Mime.prototype.setType = function(type) {
      if (!this.type) {
        this.type = type;
        return this.emit('mime', this.type);
      }
    };

    return Mime;

  })(stream.Transform);


  /*
  in: file name
  out: transformStream to emit 'mime' type
   */

  module.exports = function(name) {
    var ret, type;
    type = mimetype.lookup(name);
    ret = new Mime();
    if (type) {
      setTimeout(function() {
        return ret.setType(type);
      });
    }
    return ret;
  };

}).call(this);
