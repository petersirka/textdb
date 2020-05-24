require('total.js');
const Path = require('path');
const Fs = require('fs');
const IMAGES = { jpg: 1, png: 1, gif: 1, svg: 1, jpeg: 1, heic: 1, heif: 1, webp: 1, tiff: 1, bmp: 1 };
const HEADERSIZE = 300;
const MKDIR = { recursive: true };
const STREAMOPTIONS = { start: 0, end: HEADERSIZE - 1, encoding: 'binary' };

function FileDB(name, directory) {
	var t = this;
	t.name = name;
	t.directory = directory;
	t.logger = directory + '/' + name + '.log';
	t.cache = {};
}

const FP = FileDB.prototype;

FP.makedirectory = function(id) {

	var val = (HASH(id, true) % 10000) + '';
	var diff = 4 - val.length;

	if (diff > 0) {
		for (var i = 0; i < diff; i++)
			val = '0' + val;
	}

	if (diff.length > 4)
		val = val.substring(0, 4);

	return Path.join(this.directory, val);
};

FP.save = function(id, name, filename, callback) {

	var self = this;
	var directory = self.makedirectory(id);
	var filenameto = Path.join(directory, id + '.file');

	var index = name.lastIndexOf('/');
	if (index !== -1)
		name = name.substring(index + 1);

	if (self.cache[directory]) {
		self.saveforce(id, name, filename, filenameto, callback);
	} else {
		Fs.mkdir(directory, MKDIR, function(err) {
			if (err)
				callback(err);
			else
				self.saveforce(id, name, filename, filenameto, callback);
		});
	}

	return self;
};

FP.saveforce = function(id, name, filename, filenameto, callback) {

	if (!callback)
		callback = NOOP;

	var self = this;
	var header = Buffer.alloc(HEADERSIZE);
	var reader = Fs.createReadStream(filename);
	var writer = Fs.createWriteStream(filenameto);

	var meta = { type: 'save', name: name, size: 0, width: 0, height: 0, ext: ext(name) };
	var tmp;

	writer.write(header, 'binary');

	if (IMAGES[meta.ext]) {
		reader.once('data', function(buffer) {
			switch (meta.ext) {
				case 'gif':
					tmp = framework_image.measureGIF(buffer);
					break;
				case 'png':
					tmp = framework_image.measurePNG(buffer);
					break;
				case 'jpg':
				case 'jpeg':
					tmp = framework_image.measureJPG(buffer);
					break;
				case 'svg':
					tmp = framework_image.measureSVG(buffer);
					break;
			}
		});
	}

	reader.pipe(writer);

	CLEANUP(writer, function() {

		Fs.open(filenameto, 'r+', function(err, fd) {

			if (err) {
				// Unhandled error
				callback(err);
				return;
			}

			if (tmp) {
				meta.width = tmp.width;
				meta.height = tmp.height;
			}

			meta.size = writer.bytesWritten - HEADERSIZE;

			// Header
			header.write('TextDB');

			// Storage type
			header.writeInt8(tmp ? 1 : 2, 10);

			// Compression
			header.writeInt8(0, 11);

			// File size
			header.writeInt32BE(meta.size, 12);

			// Width
			header.writeInt32BE(meta.width, 16);

			// Height
			header.writeInt32BE(meta.height, 20);

			if (meta.name.length > 250)
				meta.name = meta.name.substring(0, 250);

			// Name length
			header.writeInt8(meta.name.length, 24);
			header.write(meta.name, 25, 'ascii');

			// Update header
			Fs.write(fd, header, 0, header.length, 0, function(err) {
				if (err) {
					callback(err);
					Fs.close(fd, NOOP);
				} else {
					meta.id = id;
					meta.date = new Date();
					Fs.appendFile(self.logger, JSON.stringify(meta) + '\n', NOOP);
					Fs.close(fd, () => callback(null, meta));
				}
			});
		});
	});
};

FP.read = function(id, callback, nostream) {

	var self = this;
	var filename = Path.join(self.makedirectory(id), id + '.file');

	Fs.open(filename, 'r', function(err, fd) {

		if (err) {
			callback(err);
			return;
		}

		var buffer = Buffer.alloc(HEADERSIZE);
		Fs.read(fd, buffer, 0, HEADERSIZE, 0, function(err) {

			if (err) {
				callback(err);
				Fs.close(fd, NOOP);
				return;
			}

			var meta = {};
			meta.type = buffer.readInt8(10);
			meta.compression = buffer.readInt8(11);
			meta.size = buffer.readInt32BE(12);
			meta.width = buffer.readInt32BE(16);
			meta.height = buffer.readInt32BE(20);
			meta.name = buffer.toString('ascii', 25, 25 + buffer.readInt8(24));
			meta.ext = ext(meta.name);

			if (!nostream) {
				meta.stream = Fs.createReadStream(filename, { fd: fd, start: HEADERSIZE });
				CLEANUP(meta.stream, () => Fs.close(fd, NOOP));
			}

			callback(err, meta);
		});
	});

	return self;
};

FP.remove = function(id, callback) {
	var self = this;
	var filename = Path.join(self.makedirectory(id), id + '.file');
	Fs.unlink(filename, function(err) {
		!err && Fs.appendFile(self.logger, JSON.stringify({ type: 'remove', id: id, date: new Date() }) + '\n', NOOP);
		callback && callback(err);
	});
	return self;
};

FP.clear = function(callback) {

	var self = this;
	var count = 0;

	Fs.readdir(self.directory, function(err, response) {

		if (err)
			return callback(err);

		Fs.appendFile(self.logger, JSON.stringify({ type: 'clear', date: new Date() }) + '\n', NOOP);

		response.wait(function(item, next) {
			var dir = Path.join(self.directory, item);
			Fs.readdir(dir, function(err, response) {
				if (response instanceof Array) {
					count += response.length;
					response.wait((file, next) => Fs.unlink(Path.join(self.directory, item, file), next), () => Fs.unlink(dir, next));
				} else
					next();
			});
		}, () => callback(null, count));

	});

	return self;
};

FP.browse = function(callback) {
	var self = this;
	Fs.readdir(self.directory, function(err, response) {
		var files = [];
		response.wait(function(item, next) {
			Fs.readdir(Path.join(self.directory, item), function(err, response) {
				if (response instanceof Array) {
					response.wait(function(item, next) {
						self.read(item.substring(0, item.lastIndexOf('.')), function(err, meta) {
							meta && files.push(meta);
							next();
						}, true);
					}, next);
				} else
					next();
			});
		}, () => callback(null, files));
	});
	return self;
};

FP.count = function(callback) {
	var self = this;
	var count = 0;
	Fs.readdir(self.directory, function(err, response) {
		response.wait(function(item, next) {
			Fs.readdir(Path.join(self.directory, item), function(err, response) {
				if (response instanceof Array)
					count += response.length;
				next();
			});
		}, () => callback(null, count));
	});
	return self;
};

function ext(name) {
	var index = name.lastIndexOf('.');
	return index === -1 ? '' : name.substring(index + 1).toLowerCase();
}

var fd = new FileDB('images', 'images.fdb');

fd.clear();

// fd.save(UID(), 'logo.png', '/Users/petersirka/Desktop/logo.png', console.log);
// fd.count(console.log);
// fd.read('163685001cy61b', console.log, true);
// fd.remove('163688001ca61b', console.log);