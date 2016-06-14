path = require 'path'
util = require 'util'
stream = require 'stream'
_ = require 'lodash'
concat = require 'concat-stream'
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
			self.gfs = Grid self.db, server
				
	ls: (dirname) ->
		self.conn
			.then ->
				index =	
					filename: 	1
					uploadDate: -1
				self.gfs.collection(self.opts.bucket).ensureIndex index, (err, indexName) ->
					if err
						return Promise.reject err
					self.gfs.collection	self.opts.bucket
						.distinct 'filename', 'metadata.dirname': dirname, (err, files) ->
                        	if err 
                        		return Promise.reject err
                        	return files
	        
	read: (fd) ->
		@readLastVersion fd, cb
						
	readLastVersion: (fd) ->
		@readVersion fd, -1, cb
		
	readVersion: (fd, version) ->
		self.conn
			.then ->
				self.gfs
					.collection self.opts.bucket
					.find filename: fd
					.limit -1
					.skip if version < 0 then Math.abs(version) - 1 else version
					.sort { uploadDate: if version < 0 then -1 else 1 } 
					.next (err, file) ->
						if err
							return Promise.reject err

						if !file
							err = new Error('ENOENT')
							_.extend err,
								name:		'Error (ENOENT)'
								code:		'ENOENT'
								status:		404
								message:	"No file exists in this mongo gridfs bucket with that file descriptor (#{fd})"
							return Promise.reject err
                    
						gridStore = new GridStore self.db, file._id, 'r', root: self.opts.bucket
						gridStore.open (err, gridStore) ->
							if err
								return reject err
                        
							stream = gridStore.stream()
							stream.pipe concat Promise.resolve
							stream.on 'error', Promise.reject
				
	rm: (fd) ->
		self.conn
			.then ->
				self.gfs.exist {filename: fd, root: self.opts.bucket}, (err, found) ->
					if err
						return Promise.reject err
					if not found
						return Promise.reject "#{fd} not found"
					self.gfs.remove {filename: fd, root: self.opts.bucket}, (err) ->
						if err
							return Promise.reject err
			
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
							
						@outs = self.gfs.createWriteStream
							filename:	fd
							root:		self.opts.bucket
							metadata: metadata								
						
						@outs.once 'open', ->
							__newFile.extra = _.assign fileId: @id, metadata
						
						@outs.once 'close', (file) ->
							done null, file
						
						# end downstream if error from upstream	
						__newFile.once 'error', (err) =>
							@outs?.end()
							Promise.reject err
						
						@outs.once 'error', Promise.reject
							
						__newFile.pipe @outs
						
					.catch (err) =>
						# end receiver stream and notify upstream if error from downstream
						@end()
						done err
		
		return new Receiver