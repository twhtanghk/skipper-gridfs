Promise = require 'bluebird'
stream = require 'stream'
mmm = require 'mmmagic'
Magic = mmm.Magic
magic = new Magic mmm.MAGIC_MIME
mimetype = require 'mime-types'

class Mime extends stream.Transform
  buffer: new Buffer(0)
  type: null
  @magicSize: 16384

  constructor: (opts) ->
    @on 'finish', =>
      @check()
    super opts

  _transform: (chunk, encoding, cb) ->
    @push chunk
    if Buffer.concat(@_readableState.buffer).length >= Mime.magicSize
      @check()
    cb()

  check: ->
    if not type?
      magic.detect Buffer.concat(@_readableState.buffer), (err, res) =>
        [org, type, encoding] = res.match /^(.*); charset=(.*)$/
        @setType type

  setType: (type) =>
    if not @type
      @type = type
      @emit 'mime', @type

###
in: file name
out: transformStream to emit 'mime' type
###
module.exports = (name) ->
  type = mimetype.lookup(name)
  ret = new Mime()
  if type
    setTimeout ->
      ret.setType type
  return ret
