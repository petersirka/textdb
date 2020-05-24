const Path = require('path');
const Fs = require('fs');
const IMAGES = { jpg: 1, png: 1, gif: 1, svg: 1, jpeg: 1, heic: 1, heif: 1, webp: 1, tiff: 1, bmp: 1 };
const HEADERSIZE = 300;

function FileDB(name, directory) {
	var t = this;
	t.name = name;
	t.directory = directory;
}

const FP = FileDB.prototype;

FP.makefilename = function(id) {
	// return this.directory + '/' +
	return this.directory + '/' + id + '.file';
};

FP.save = function(id, name, filename, callback) {
	writefile(name, filename, this.makefilename(id), filename, callback);
};

FP.read = function(id, callback, nostream) {

	var self = this;
	var filename = self.makefilename(id);

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

function ext(name) {
	var index = name.lastIndexOf('.');
	return index === -1 ? '' : name.substring(index + 1).toLowerCase();
}

function writefile(name, reader, filenameto, callback) {

	var header = Buffer.alloc(HEADERSIZE);
	var writer = Fs.createWriteStream(filenameto);
	var meta = { name: name, size: 0, width: 0, height: 0, ext: ext(name) };
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
				} else
					Fs.close(fd, () => callback(null, meta));
			});
		});
	});
}