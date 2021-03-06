path = require 'path'
util = require 'util'
stream = require 'stream'
concat = require 'concat-stream'
mime = require './mime'
_ = require 'lodash'
Grid = require 'gridfs-stream'
Promise = require 'bluebird'

server = require 'mongodb'
client = server.MongoClient
GridStore = server.GridStore

module.exports = (opts) ->

	###
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
	###
	self = opts: opts
	mongodbUri = require 'mongodb-uri'
	_.defaults self.opts,
		scheme:	'mongodb'
		bucket:	GridStore.DEFAULT_ROOT_COLLECTION
		port:	27017
	self.conn = client.connect(self.opts.uri || mongodbUri.format(self.opts), promiseLibrary: Promise)
		.then (db) ->
			self.db = db
			self.gfs = Promise.promisifyAll Grid(self.db, server)
			self.gridStore = (fileId, mode, opts = root: self.opts.bucket) ->
				Promise.promisifyAll new GridStore self.db, fileId, mode, opts

	ls: (dirname, cb) ->
		self.conn
			.then ->
				index =
					filename: 	1
					uploadDate: -1
				new Promise (resolve, reject) ->
					self.gfs.collection(self.opts.bucket).ensureIndex index, (err, indexName) ->
						if err
							return reject err
						self.gfs.collection	self.opts.bucket
							.distinct 'filename', 'metadata.dirname': dirname, (err, files) ->
	                        	if err
	                        		return reject err
	                        	resolve files
			.then (files) ->
				cb null, files
			.catch cb

	# default to read last uploaded version
	read: (fd, cb) ->
		self.conn
			.then =>
				@find fd, -1
					.then (file) ->
						self.gridStore file._id, 'r', root: self.opts.bucket
							.openAsync()
							.then (content) ->
								out = content.stream()
								out.on 'error', Promise.reject
								return out
			.then (stream) ->
				stream.pipe concat (data) ->
					cb null, data
			.catch cb

	###
	version:
		null:
			remove all versions of the specified file
		n:
			remove nth version of the specified file
		[i1, i2, ...]:
			remove all versions listed in the array
	###
	rm: (fd, version = null) ->
		if version?
			switch true
				when typeof version == 'number'
					@find fd, version
						.then (file) ->
							self.gfs.removeAsync {_id: file._id, root: self.opts.bucket}
						.then ->
							ret = {}
							ret[fd] = version
							Promise.resolve ret
				when Array.isArray version
					Promise
						.map version, (v) =>
							@rm fd, v
				else
					Promise.reject "invalid version"
		else
			self.conn
				.then ->
					result = self.gfs.files
						.find filename: fd
					Promise.promisifyAll result
						.toArrayAsync()
						.then (files) ->
							if files.length == 0
								return Promise.reject "#{fd} not found"
							Promise.map files, (file) ->
								self.gfs.removeAsync {_id: file._id, root: self.opts.bucket}

	# return the specified version of file
	find: (fd, version) ->
		self.conn
			.then ->
				new Promise (resolve, reject) ->
					self.gfs
						.collection self.opts.bucket
						.find filename: fd
						.limit 1
						.skip Math.abs(version) - 1
						.sort { uploadDate: if version < 0 then -1 else 1 }
						.next (err, file) ->
							if err
								return reject err
							if !file
								return reject "version #{version} of #{fd} not found"
							return resolve file

	receive: (opts) ->

		class Receiver extends stream.Writable

			constructor: (opts = {}) ->
				_.defaults opts,
					objectMode: true

				super(opts)

			_write: (__newFile, encoding, done) ->
				fd = __newFile.fd

				self.conn
					.then =>
						metadata = _.extend __newFile.metadata || {},
							fd:			fd
							dirname:	__newFile.dirname || path.dirname(fd)

						out = __newFile
							.pipe mime fd

							.on 'mime', (type) ->

								out
									.pipe self.gfs.createWriteStream
										filename:	fd
										root:		self.opts.bucket
										content_type: type
										metadata: metadata
									.once 'open', ->
										__newFile.extra = _.assign fileId: @id, metadata
									.once 'close', (file) ->
										done null, file
									.once 'error', (err) ->
										@end()
										done err

		return new Receiver
