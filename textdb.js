// Copyright 2020 (c) Peter Širka <petersirka@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

/**
 * @module TextDB
 * @version 1.0.0
 */

'use strict';

require('total.js');

const Fs = require('fs');
const Path = require('path');
const TextStreamReader = require('./stream');
const QueryBuilder = require('./builder').QueryBuilder;
const DELIMITER = '|';
const NEWLINEBUF = Buffer.from('\n', 'utf8');

const JSONBOOL = '":true ';
const NEWLINE = '\n';
const REGBOOL = /":true/g; // for updates of boolean types
const REGTESCAPE = /\||\n|\r/g;
const REGTUNESCAPE = /%7C|%0D|%0A/g;
const REGTESCAPETEST = /\||\n|\r/;
const BOOLEAN = { '1': 1, 'true': 1, 'on': 1 };
const TABLERECORD = { '+': 1, '-': 1, '*': 1 };
const MAXREADERS = 3;

const JSONBUFFER = 40;

function TableDB(name, directory) {

	var t = this;
	t.duration = [];
	t.filename = Path.join(directory, name + '.tdb');
	t.filenameLog = Path.join(directory, name + '.tlog');
	t.filenameBackup = Path.join(directory, name + '.tbk');
	t.name = name;
	t.$name = '$' + name;
	t.pending_reader = [];
	t.pending_reader2 = [];
	t.pending_update = [];
	t.pending_append = [];
	t.pending_remove = [];
	t.pending_streamer = [];
	t.pending_clean = [];
	t.pending_clear = [];
	t.pending_locks = [];

	t.step = 0;
	t.ready = false;
	t.$writting = false;
	t.$reading = 0;
	t.$allocations = true;

	t.next2 = function() {
		t.next(0);
	};

	Fs.createReadStream(t.filename, { end: 2048 }).once('data', function(chunk) {
		t.parseSchema(t, chunk.toString('utf8').split('\n', 1)[0].split(DELIMITER));
		t.ready = true;
		t.$header = Buffer.byteLength(t.stringifySchema(t)) + 1;
		t.next(0);
	}).on('error', function() {
		// NOOP
	});
}

function JsonDB(name, directory) {

	var t = this;

	t.filename = Path.join(directory, name + '.ndb');
	t.filenameLog = Path.join(directory, name + '.nlog');
	t.filenameBackup = Path.join(directory, name + '.nbk');

	t.duration = [];
	t.name = name;
	t.pending_update = [];
	t.pending_append = [];
	t.pending_reader = [];
	t.pending_remove = [];
	t.pending_reader2 = [];
	t.pending_streamer = [];
	t.pending_clean = [];
	t.pending_clear = [];
	t.pending_locks = [];
	t.step = 0;
	t.pending_drops = false;
	t.$timeoutmeta;
	t.$writting = false;
	t.$reading = 0;

	t.next2 = function() {
		t.next(0);
	};
}

const TD = TableDB.prototype;
const JD = JsonDB.prototype;

TD.memory = JD.memory = function(count, size) {
	var self = this;
	count && (self.buffercount = count + 1);      // def: 15 - count of stored documents in memory while reading/writing
	size && (self.buffersize = size * 1024);      // def: 32 - size of buffer in kB
	return self;
};

function prepareschema(schema) {
	return schema.replace(/;|,/g, DELIMITER).trim();
}

TD.alter = function(schema, callback) {

	var self = this;
	var parsed = {};

	if (self.$header) {
		self.ready = true;
		self.parseSchema(parsed, prepareschema(schema).split(DELIMITER));
		if (self.stringifySchema(self) !== self.stringifySchema(parsed))
			self.extend(schema, callback);
		else
			callback && callback();
		self.next(0);
	} else {
		self.parseSchema(self, prepareschema(schema).split(DELIMITER));
		var bschema = self.stringifySchema(self);
		self.$header = Buffer.byteLength(bschema) + 1;
		Fs.writeFileSync(self.filename, bschema + NEWLINE, 'utf8');
		self.ready = true;
		self.next(0);
		callback && callback();
	}
};

function next_operation(self, type) {
	self.next(type);
}

JD.insert = function() {
	var self = this;
	var builder = new QueryBuilder(self);
	self.pending_append.push(builder);
	setImmediate(next_operation, self, 1);
	return builder;
};

JD.update = function() {
	var self = this;
	var builder = new QueryBuilder(self);
	self.pending_update.push(builder);
	setImmediate(next_operation, self, 2);
	return builder;
};

TD.restore = JD.restore = function(filename, callback) {
	var self = this;

	U.wait(() => !self.type, function(err) {

		if (err)
			throw new Error('Database can\'t be restored because it\'s busy.');

		self.type = 9;

		// Restore
		F.restore(filename, self.directory, function(err, response) {
			self.type = 0;
			callback && callback(err, response);
		});

	});
	return self;
};

TD.backup = JD.backup = function(filename, callback) {

	var self = this;
	var list = [];
	var pending = [];

	pending.push(function(next) {
		F.path.exists(self.filename, function(e) {
			e && list.push(self.filename);
			next();
		});
	});

	pending.push(function(next) {
		F.path.exists(self.filenameLog, function(e) {
			e && list.push(self.filenameLog);
			next();
		});
	});

	pending.push(function(next) {
		F.path.exists(self.filenameBackup, function(e) {
			e && list.push(self.filenameBackup);
			next();
		});
	});

	pending.async(function() {
		if (list.length) {
			// Total.js Backup
			F.backup(filename, list, callback);
		} else
			callback('No files for backing up.');
	});

	return self;
};

TD.backups = JD.backups = function(callback, builder) {

	var self = this;
	var isTable = self instanceof TableDB;

	if (!builder)
		builder = new QueryBuilder(self);

	if (isTable && !self.ready) {
		setTimeout((self, callback, builder) => self.backups(callback, builder), 500, self, callback, builder);
		return builder;
	}

	var stream = Fs.createReadStream(self.filenameBackup);
	var output = [];
	var tmp = {};

	tmp.keys = self.$keys;

	stream.on('data', U.streamer(NEWLINEBUF, function(item, index) {

		var end = item.indexOf('|');
		var meta = item.substring(0, end).trim().parseJSON(true);

		tmp.line = item.substring(end + 1).trim();

		if (isTable)
			tmp.line = tmp.line.split('|');

		meta.id = index + 1;
		meta.item = self instanceof TableDB ? self.parseData(tmp) : tmp.line.parseJSON(true);

		// @TODO: missing sorting
		if (!builder.filterrule || builder.filterrule(meta, builder.filterarg))
			output.push(builder.prepare(meta));

	}), stream);

	CLEANUP(stream, () => callback(null, output));
	return builder;
};

JD.drop = function() {
	var self = this;
	self.pending_drops = true;
	setImmediate(next_operation, self, 7);
	return self;
};

TD.clear = JD.clear = function(callback) {
	var self = this;
	self.pending_clear.push(callback || NOOP);
	setImmediate(next_operation, self, 12);
	return self;
};

TD.clean = JD.clean = function(callback) {
	var self = this;
	self.pending_clean.push(callback || NOOP);
	setImmediate(next_operation, self, 13);
	return self;
};

TD.lock = JD.lock = function(callback) {
	var self = this;
	self.pending_locks.push(callback || NOOP);
	setImmediate(next_operation, self, 14);
	return self;
};

JD.remove = function() {
	var self = this;
	var builder = new QueryBuilder(self);
	self.pending_remove.push(builder);
	setImmediate(next_operation, self, 3);
	return builder;
};

JD.find = function(builder) {
	var self = this;
	if (builder instanceof QueryBuilder)
		builder.db = self;
	else
		builder = new QueryBuilder(self);
	self.pending_reader.push(builder);
	setImmediate(next_operation, self, 4);
	return builder;
};

JD.find2 = function(builder) {
	var self = this;
	if (builder instanceof QueryBuilder)
		builder.db = self;
	else
		builder = new QueryBuilder(self);
	self.pending_reader2.push(builder);
	setImmediate(next_operation, self, 11);
	return builder;
};

JD.stream = function(fn, arg, callback) {
	var self = this;

	if (typeof(arg) === 'function') {
		callback = arg;
		arg = null;
	}

	self.pending_streamer.push({ fn: fn, callback: callback, arg: arg || {} });
	setImmediate(next_operation, self, 10);
	return self;
};

//  1 append
//  2 update
//  3 remove
//  4 reader
//  7 drop
//  8 backup
//  9 restore
// 10 streamer
// 11 reader reverse
// 12 clear
// 13 clean
// 14 locks

const NEXTWAIT = { 7: true, 8: true, 9: true, 12: true, 13: true, 14: true };

JD.next = function(type) {

	if (type && NEXTWAIT[this.step])
		return;

	if (!this.$writting && !this.$reading) {

		if (this.step !== 12 && this.pending_clear.length) {
			this.$clear();
			return;
		}

		if (this.step !== 13 && this.pending_clean.length) {
			this.$clean();
			return;
		}

		if (this.step !== 7 && this.pending_drops) {
			this.$drop();
			return;
		}

		if (this.step !== 14 && this.pending_locks.length) {
			this.$lock();
			return;
		}
	}

	if (!this.$writting) {

		if (this.step !== 1 && this.pending_append.length) {
			this.$append();
			return;
		}

		if (this.step !== 2 && !this.$writting && this.pending_update.length) {
			this.$update();
			return;
		}

		if (this.step !== 3 && !this.$writting && this.pending_remove.length) {
			this.$remove();
			return;
		}

	}

	if (this.$reading < MAXREADERS) {

		// if (this.step !== 4 && this.pending_reader.length) {
		if (this.pending_reader.length) {
			this.$reader();
			return;
		}

		// if (this.step !== 11 && this.pending_reader2.length) {
		if (this.pending_reader2.length) {
			this.$reader3();
			return;
		}

		// if (this.step !== 10 && this.pending_streamer.length) {
		if (this.pending_streamer.length) {
			this.$streamer();
			return;
		}
	}

	if (this.step !== type) {
		this.step = 0;
		setImmediate(next_operation, this, 0);
	}
};

// ======================================================================
// FILE OPERATIONS
// ======================================================================

JD.$append = function() {
	var self = this;
	self.step = 1;

	if (!self.pending_append.length) {
		self.next(0);
		return;
	}

	self.$writting = true;

	self.pending_append.splice(0).limit(JSONBUFFER, function(items, next) {

		var json = '';
		var now = Date.now();

		for (var i = 0; i < items.length; i++) {
			var builder = items[i];
			json += JSON.stringify(builder.payload) + NEWLINE;
		}

		Fs.appendFile(self.filename, json, function(err) {

			err && F.error(err, 'NoSQL insert: ' + self.name);

			var diff = Date.now() - now;

			if (self.duration.push({ type: 'insert', duration: diff }) > 20)
				self.duration.shift();

			for (var i = 0; i < items.length; i++) {
				var builder = items[i];
				builder.duration = diff;
				builder.counter = builder.count = 1;
				builder.filterarg = builder.modifyarg = builder.payload = builder.$fields = builder.$fieldsremove = builder.db = builder.$TextReader = builder.$take = builder.$skip = undefined;
				builder.logrule && builder.logrule();
				builder.$callback && builder.$callback(err, builder);
			}

			next();
		});

	}, () => setImmediate(next_append, self));
};

function next_append(self) {
	self.$writting = false;
	self.next(0);
}

JD.$update = function() {

	var self = this;
	self.step = 2;

	if (!self.pending_update.length) {
		self.next(0);
		return self;
	}

	self.$writting = true;

	var filter = self.pending_update.splice(0);
	var filters = new TextReader();
	var fs = new TextStreamReader(self.filename);
	var change = false;

	filters.type = 'update';
	filters.db = self;

	for (var i = 0; i < filter.length; i++)
		filters.add(filter[i], true);

	if (self.buffersize)
		fs.buffersize = self.buffersize;

	if (self.buffercount)
		fs.buffercount = self.buffercount;

	var update = function(docs, doc, dindex, f) {
		f.modifyrule(docs[dindex], f.modifyarg);
		f.backuprule && f.backuprule(fs.docsbuffer[dindex].doc);
	};

	var updateflush = function(docs, doc, dindex) {

		doc = docs[dindex];

		var rec = fs.docsbuffer[dindex];
		var upd = JSON.stringify(doc).replace(REGBOOL, JSONBOOL);
		if (upd === rec.doc)
			return;

		!change && (change = true);
		var was = true;

		if (rec.doc.length === upd.length) {
			var b = Buffer.byteLength(upd);
			if (rec.length === b) {
				fs.write(upd + NEWLINE, rec.position);
				was = false;
			}
		}

		if (was) {
			var tmp = fs.remchar + rec.doc.substring(1) + NEWLINE;
			fs.write(tmp, rec.position);
			fs.write2(upd + NEWLINE);
		}
	};

	fs.ondocuments = function() {
		filters.compare2(JSON.parse('[' + fs.docs + ']', jsonparser), update, updateflush);
	};

	fs.$callback = function() {

		fs = null;
		self.$writting = false;
		self.next(0);

		for (var i = 0; i < filters.builders.length; i++) {
			var builder = filters.builders[i];
			builder.filterarg = builder.modifyarg = builder.payload = builder.$fields = builder.$fieldsremove = builder.db = builder.$TextReader = builder.$take = builder.$skip = undefined;
			builder.logrule && builder.logrule();
			builder.$callback && builder.$callback(null, builder);
		}

		// change && CLEANER[self.name] && (CLEANER[self.name] = 1);
	};

	fs.openupdate();
	return self;
};

JD.$reader = function() {

	var self = this;
	self.step = 4;

	if (!self.pending_reader.length) {
		self.next(0);
		return self;
	}

	var list = self.pending_reader.splice(0);
	self.$reading++;
	self.$reader2(self.filename, list, function() {
		self.$reading--;
		self.next(0);
	});
	return self;
};

JD.$reader2 = function(filename, items, callback, reader) {

	var self = this;
	var fs = new TextStreamReader(self.filename);
	var filters = new TextReader(items);

	filters.type = 'read2';
	filters.db = self;

	if (self.buffersize)
		fs.buffersize = self.buffersize;

	if (self.buffercount)
		fs.buffercount = self.buffercount;

	fs.ondocuments = function() {
		return filters.compare(JSON.parse('[' + fs.docs + ']', jsonparser));
	};

	fs.$callback = function() {
		filters.done();
		fs = null;
		callback();
	};

	if (reader)
		fs.openstream(reader);
	else
		fs.openread();

	return self;
};

JD.$reader3 = function() {

	var self = this;
	self.step = 11;

	if (!self.pending_reader2.length) {
		self.next(0);
		return self;
	}

	self.$reading++;

	var fs = new TextStreamReader(self.filename);
	var filters = new TextReader(self.pending_reader2.splice(0));

	filters.type = 'readreverse';
	filters.db = self;

	if (self.buffersize)
		fs.buffersize = self.buffersize;

	if (self.buffercount)
		fs.buffercount = self.buffercount;

	fs.ondocuments = function() {
		return filters.compare(JSON.parse('[' + fs.docs + ']', jsonparser));
	};

	fs.$callback = function() {
		filters.done();
		self.$reading--;
		fs = null;
		self.next(0);
	};

	fs.openreadreverse();
	return self;
};

JD.$streamer = function() {

	var self = this;
	self.step = 10;

	if (!self.pending_streamer.length) {
		self.next(0);
		return self;
	}

	self.$reading++;

	var filter = self.pending_streamer.splice(0);
	var length = filter.length;
	var count = 0;
	var fs = new TextStreamReader(self.filename);

	if (self.buffersize)
		fs.buffersize = self.buffersize;

	if (self.buffercount)
		fs.buffercount = self.buffercount;

	fs.ondocuments = function() {
		var docs = JSON.parse('[' + fs.docs + ']', jsonparser);
		for (var j = 0; j < docs.length; j++) {
			var json = docs[j];
			count++;
			for (var i = 0; i < length; i++)
				filter[i].fn(json, filter[i].arg, count);
		}
	};

	fs.$callback = function() {
		for (var i = 0; i < length; i++)
			filter[i].callback && filter[i].callback(null, filter[i].arg, count);
		self.$reading--;
		self.next(0);
		fs = null;
	};

	fs.openread();
	return self;
};

JD.$remove = function() {

	var self = this;
	self.step = 3;

	if (!self.pending_remove.length) {
		self.next(0);
		return;
	}

	self.$writting = true;

	var fs = new TextStreamReader(self.filename);
	var filter = self.pending_remove.splice(0);
	var filters = new TextReader(filter);
	var change = false;

	filters.type = 'remove';
	filters.db = self;

	if (self.buffersize)
		fs.buffersize = self.buffersize;

	if (self.buffercount)
		fs.buffercount = self.buffercount;

	var remove = function(docs, d, dindex, f) {
		f.backuprule && f.backuprule(fs.docsbuffer[dindex].doc);
		return 1;
	};

	var removeflush = function(docs, d, dindex) {
		var rec = fs.docsbuffer[dindex];
		!change && (change = true);
		fs.write(fs.remchar + rec.doc.substring(1) + NEWLINE, rec.position);
	};

	fs.ondocuments = function() {
		filters.compare2(JSON.parse('[' + fs.docs + ']', jsonparser), remove, removeflush);
	};

	fs.$callback = function() {
		filters.done();
		fs = null;
		self.$writting = false;
		self.next(0);
		// change && CLEANER[self.name] && (CLEANER[self.name] = 1);
	};

	fs.openupdate();
};

JD.$clear = function() {

	var self = this;
	self.step = 12;

	if (!self.pending_clear.length) {
		self.next(0);
		return;
	}

	var filter = self.pending_clear.splice(0);
	Fs.unlink(self.filename, function() {
		for (var i = 0; i < filter.length; i++)
			filter[i]();
		self.next(0);
	});
};

JD.$clean = function() {

	var self = this;
	self.step = 13;

	if (!self.pending_clean.length) {
		self.next(0);
		return;
	}

	var filter = self.pending_clean.splice(0);
	var length = filter.length;

	var fs = new TextStreamReader(self.filename);
	var writer = Fs.createWriteStream(self.filename + '-tmp');

	if (self.buffersize)
		fs.buffersize = self.buffersize;

	if (self.buffercount)
		fs.buffercount = self.buffercount;

	fs.divider = NEWLINE;

	fs.ondocuments = function() {
		writer.write(fs.docs + NEWLINE);
	};

	fs.$callback = function() {
		writer.end();
	};

	writer.on('finish', function() {
		Fs.rename(self.filename + '-tmp', self.filename, function() {
			for (var i = 0; i < length; i++)
				filter[i]();
			self.next(0);
			fs = null;
		});
	});

	fs.openread();
};

JD.$lock = function() {

	var self = this;
	self.step = 14;

	if (!self.pending_locks.length) {
		self.next(0);
		return;
	}

	var filter = self.pending_locks.splice(0);
	filter.wait(function(fn, next) {
		fn.call(self, next);
	}, function() {
		self.next(0);
	});
};

JD.$drop = function() {
	var self = this;
	self.step = 7;

	if (!self.pending_drops) {
		self.next(0);
		return;
	}

	self.pending_drops = false;
	var remove = [self.filename];

	remove.wait((filename, next) => Fs.unlink(filename, next), function() {
		self.next(0);
		self.free(true);
	}, 5);
};

TD.insert = function() {
	var self = this;
	var builder = new QueryBuilder(self);
	self.pending_append.push(builder);
	setImmediate(next_operation, self, 1);
	return builder;
};

TD.update = function() {
	var self = this;
	var builder = new QueryBuilder(self);
	self.pending_update.push(builder);
	setImmediate(next_operation, self, 2);
	return builder;
};

TD.remove = function() {
	var self = this;
	var builder = new QueryBuilder(self);
	self.pending_remove.push(builder);
	setImmediate(next_operation, self, 3);
	return builder;
};

TD.find = function(builder) {
	var self = this;
	if (builder)
		builder.db = self;
	else
		builder = new QueryBuilder(self);
	self.pending_reader.push(builder);
	setImmediate(next_operation, self, 4);
	return builder;
};

TD.find2 = function(builder) {
	var self = this;
	if (builder)
		builder.db = self;
	else
		builder = new QueryBuilder(self);
	self.pending_reader2.push(builder);
	setImmediate(next_operation, self, 11);
	return builder;
};

TD.stream = function(fn, arg, callback) {
	var self = this;
	if (typeof(arg) === 'function') {
		callback = arg;
		arg = null;
	}

	self.pending_streamer.push({ fn: fn, callback: callback, arg: arg || {} });
	setImmediate(next_operation, self, 10);
	return self;
};

TD.extend = function(schema, callback) {
	var self = this;
	self.lock(function(next) {

		var olds = self.$schema;
		var oldk = self.$keys;
		var oldl = self.$size;
		var oldh = Buffer.byteLength(self.stringifySchema(self) + NEWLINE);


		self.parseSchema(self, prepareschema(schema).split(DELIMITER));

		var meta = self.stringifySchema(self) + NEWLINE;
		var news = self.$schema;
		var newk = self.$keys;

		self.$schema = olds;
		self.$keys = oldk;

		var count = 0;
		var fs = new TextStreamReader(self.filename);
		var data = {};
		var tmp = self.filename + '-tmp';
		var writer = Fs.createWriteStream(tmp);

		if (self.buffersize)
			fs.buffersize = self.buffersize;

		if (self.buffercount)
			fs.buffercount = self.buffercount;

		writer.write(meta, 'utf8');
		writer.on('finish', function() {
			Fs.rename(tmp, self.filename, function() {
				next();
				callback && callback();
			});
		});

		data.keys = self.$keys;
		fs.start = oldh;
		fs.divider = '\n';

		var copy = [];

		for (var i = 0; i < newk.length; i++) {
			var key = newk[i];
			if (news[key].copy)
				copy.push(news[key]);
		}

		if (oldl)
			self.linesize = oldl;

		var size = self.$size;

		fs.ondocuments = function() {

			var lines = fs.docs.split(fs.divider);
			var items = [];

			self.$schema = olds;
			self.$keys = oldk;
			self.$size = oldl;

			for (var a = 0; a < lines.length; a++) {
				data.line = lines[a].split(DELIMITER);
				data.index = count++;

				var doc = self.parseData(data);

				if (copy.length) {
					for (var i = 0; i < copy.length; i++)
						doc[copy[i].name] = doc[copy[i].copy];
				}

				items.push(doc);
			}

			self.$schema = news;
			self.$keys = newk;

			self.$size = size;
			var buffer = '';
			for (var i = 0; i < items.length; i++)
				buffer += self.stringify(items[i], true) + NEWLINE;
			buffer && writer.write(buffer, 'utf8');
		};

		fs.$callback = function() {
			self.$schema = news;
			self.$keys = newk;
			self.$header = Buffer.byteLength(meta);
			writer.end();
			fs = null;
		};

		fs.openread();
	});

	return self;
};

TD.next = function(type) {

	if (!this.ready || (type && NEXTWAIT[this.step]))
		return;

	if (!this.$writting && !this.$reading) {

		if (this.step !== 12 && this.pending_clear.length) {
			this.$clear();
			return;
		}

		if (this.step !== 13 && this.pending_clean.length) {
			this.$clean();
			return;
		}

		if (this.step !== 7 && this.pending_drops) {
			this.$drop();
			return;
		}

		if (this.step !== 14 && this.pending_locks.length) {
			this.$lock();
			return;
		}
	}

	if (!this.$writting) {

		if (this.step !== 1 && this.pending_append.length) {
			this.$append();
			return;
		}

		if (this.step !== 2 && !this.$writting && this.pending_update.length) {
			this.$update();
			return;
		}

		if (this.step !== 3 && !this.$writting && this.pending_remove.length) {
			this.$remove();
			return;
		}
	}

	if (this.$reading < MAXREADERS) {

		// if (this.step !== 4 && this.pending_reader.length) {
		if (this.pending_reader.length) {
			this.$reader();
			return;
		}

		// if (this.step !== 11 && this.pending_reader2.length) {
		if (this.pending_reader2.length) {
			this.$reader3();
			return;
		}

		// if (this.step !== 10 && this.pending_streamer.length) {
		if (this.pending_streamer.length) {
			this.$streamer();
			return;
		}
	}

	if (this.step !== type) {
		this.step = 0;
		setImmediate(next_operation, this, 0);
	}
};

TD.$append = function() {
	var self = this;
	self.step = 1;

	if (!self.pending_append.length) {
		self.next(0);
		return;
	}

	self.$writting = true;

	self.pending_append.splice(0).limit(JSONBUFFER, function(items, next) {

		var data = '';
		var now = Date.now();

		for (var i = 0; i < items.length; i++) {
			var builder = items[i];
			data += self.stringify(builder.payload, true) + NEWLINE;
		}

		Fs.appendFile(self.filename, data, function(err) {
			err && F.error(err, 'Table insert: ' + self.name);

			var diff = Date.now() - now;

			if (self.duration.push({ type: 'insert', duration: diff }) > 20)
				self.duration.shift();

			for (var i = 0; i < items.length; i++) {
				var builder = items[i];
				builder.duration = diff;
				builder.counter = builder.count = 1;
				builder.filterarg = builder.modifyarg = builder.payload = builder.$fields = builder.$fieldsremove = builder.db = builder.$TextReader = builder.$take = builder.$skip = undefined;
				builder.logrule && builder.logrule();
				builder.$callback && builder.$callback(err, builder);
			}
			next();
		});

	}, () => setImmediate(next_append, self));
};

TD.$reader = function() {

	var self = this;

	self.step = 4;

	if (!self.pending_reader.length) {
		self.next(0);
		return self;
	}

	self.$reading++;

	var fs = new TextStreamReader(self.filename);
	var filters = new TextReader(self.pending_reader.splice(0));
	var data = {};
	var indexer = 0;

	filters.type = 'read';
	filters.db = self;

	fs.array = true;
	fs.start = self.$header;
	fs.linesize = self.$size;
	fs.divider = '\n';

	data.keys = self.$keys;

	if (self.buffersize)
		fs.buffersize = self.buffersize;

	if (self.buffercount)
		fs.buffercount = self.buffercount;

	fs.ondocuments = function() {

		var lines = fs.docs;
		var arr = [];

		for (var j = 0; j < lines.length; j++) {
			data.line = lines[j].split(DELIMITER);
			data.index = indexer++;
			arr.push(self.parseData(data));
		}

		return filters.compare(arr);
	};

	fs.$callback = function() {
		filters.done();
		fs = null;
		self.$reading--;
		self.next(0);
	};

	fs.openread();
	return self;
};

TD.$reader3 = function() {

	var self = this;

	self.step = 11;

	if (!self.pending_reader2.length) {
		self.next(0);
		return self;
	}

	self.$reading++;

	var fs = new TextStreamReader(self.filename);
	var filters = new TextReader(self.pending_reader2.splice(0));
	var data = {};
	var indexer = 0;

	filters.type = 'readreverse';
	filters.db = self;

	fs.array = true;
	fs.start = self.$header;
	fs.linesize = self.$size;
	fs.divider = '\n';
	data.keys = self.$keys;

	if (self.buffersize)
		fs.buffersize = self.buffersize;

	if (self.buffercount)
		fs.buffercount = self.buffercount;

	fs.ondocuments = function() {

		var lines = fs.docs;
		var arr = [];

		for (var j = 0; j < lines.length; j++) {
			data.line = lines[j].split(DELIMITER);
			if (TABLERECORD[data.line[0]]) {
				data.index = indexer++;
				arr.push(self.parseData(data));
			}
		}

		return filters.compare(arr);
	};

	fs.$callback = function() {
		filters.done();
		fs = null;
		self.$reading--;
		self.next(0);
	};

	fs.openreadreverse();
	return self;
};

TD.$update = function() {

	var self = this;
	self.step = 2;

	if (!self.pending_update.length) {
		self.next(0);
		return self;
	}

	self.$writting = true;

	var fs = new TextStreamReader(self.filename);
	var filter = self.pending_update.splice(0);
	var filters = new TextReader();
	var change = false;
	var indexer = 0;
	var data = { keys: self.$keys };

	filters.type = 'update';
	filters.db = self;

	for (var i = 0; i < filter.length; i++)
		filters.add(filter[i], true);

	fs.array = true;
	fs.start = self.$header;
	fs.linesize = self.$size;
	fs.divider = '\n';

	if (self.buffersize)
		fs.buffersize = self.buffersize;

	if (self.buffercount)
		fs.buffercount = self.buffercount;

	var update = function(docs, doc, dindex, f, findex) {
		// var rec = fs.docsbuffer[dindex];
		// var fil = filter[findex];
		f.modifyrule(docs[dindex], f.modifyarg);
		f.backuprule && f.backuprule(fs.docsbuffer[dindex].doc);
	};

	var updateflush = function(docs, doc, dindex) {

		doc = docs[dindex];

		var rec = fs.docsbuffer[dindex];
		var upd = self.stringify(doc, null, rec.length);

		if (upd === rec.doc)
			return;

		!change && (change = true);

		var b = Buffer.byteLength(upd);
		if (rec.length === b) {
			fs.write(upd + NEWLINE, rec.position);
		} else {
			var tmp = fs.remchar + rec.doc.substring(1) + NEWLINE;
			fs.write(tmp, rec.position);
			fs.write2(upd + NEWLINE);
		}
	};

	fs.ondocuments = function() {

		var lines = fs.docs;
		var arr = [];

		for (var a = 0; a < lines.length; a++) {
			data.line = lines[a].split(DELIMITER);
			data.length = lines[a].length;
			data.index = indexer++;
			arr.push(self.parseData(data, EMPTYOBJECT));
		}

		filters.compare2(arr, update, updateflush);
	};

	fs.$callback = function() {

		fs = null;
		self.$writting = false;
		self.next(0);

		for (var i = 0; i < filters.builders.length; i++) {
			var builder = filters.builders[i];
			builder.filterarg = builder.modifyarg = builder.payload = builder.$fields = builder.$fieldsremove = builder.db = builder.$TextReader = builder.$take = builder.$skip = undefined;
			builder.$callback && builder.$callback(null, builder);
		}

		// change && CLEANER[self.name] && (CLEANER[self.name] = 1);
	};

	fs.openupdate();
	return self;
};

TD.$remove = function() {

	var self = this;
	self.step = 3;

	if (!self.pending_remove.length) {
		self.next(0);
		return;
	}

	self.$writting = true;

	var fs = new TextStreamReader(self.filename);
	var filter = self.pending_remove.splice(0);
	var filters = new TextReader(filter);
	var change = false;
	var indexer = 0;

	filters.type = 'remove';
	filters.db = self;

	fs.array = true;
	fs.start = self.$header;
	fs.linesize = self.$size;
	fs.divider = '\n';

	if (self.buffersize)
		fs.buffersize = self.buffersize;

	if (self.buffercount)
		fs.buffercount = self.buffercount;

	var data = { keys: self.$keys };

	var remove = function(docs, d, dindex, f) {
		f.backuprule && f.backuprule(fs.docsbuffer[dindex].doc);
		return 1;
	};

	var removeflush = function(docs, d, dindex) {
		var rec = fs.docsbuffer[dindex];
		!change && (change = true);
		fs.write(fs.remchar + rec.doc.substring(1) + NEWLINE, rec.position);
	};

	fs.ondocuments = function() {

		var lines = fs.docs;
		var arr = [];

		for (var a = 0; a < lines.length; a++) {
			data.line = lines[a].split(DELIMITER);
			data.index = indexer++;
			arr.push(self.parseData(data));
		}

		filters.compare2(arr, remove, removeflush);
	};

	fs.$callback = function() {
		filters.done();
		fs = null;
		self.$writting = false;
		self.next(0);
		// change && CLEANER[self.$name] && (CLEANER[self.$name] = 1);
	};

	fs.openupdate();
};

TD.$clean = function() {

	var self = this;
	self.step = 13;

	if (!self.pending_clean.length) {
		self.next(0);
		return;
	}

	var filter = self.pending_clean.splice(0);
	var length = filter.length;

	var fs = new TextStreamReader(self.filename);
	var writer = Fs.createWriteStream(self.filename + '-tmp');

	writer.write(self.stringifySchema(self) + NEWLINE);

	fs.start = self.$header;
	fs.linesize = self.$size;
	fs.divider = NEWLINE;

	if (self.buffersize)
		fs.buffersize = self.buffersize;

	if (self.buffercount)
		fs.buffercount = self.buffercount;

	fs.ondocuments = function() {
		writer.write(fs.docs + NEWLINE);
	};

	fs.$callback = function() {
		writer.end();
	};

	writer.on('finish', function() {
		Fs.rename(self.filename + '-tmp', self.filename, function() {
			for (var i = 0; i < length; i++)
				filter[i]();
			self.next(0);
			fs = null;
		});
	});

	fs.openread();
};

TD.$clear = function() {

	var self = this;
	self.step = 12;

	if (!self.pending_clear.length) {
		self.next(0);
		return;
	}

	var filter = self.pending_clear.splice(0);
	Fs.unlink(self.filename, function() {
		for (var i = 0; i < filter.length; i++)
			filter[i]();

		Fs.appendFile(self.filename, self.stringifySchema(self) + NEWLINE, function() {
			self.next(0);
		});
	});
};

TD.$lock = function() {

	var self = this;
	self.step = 14;

	if (!self.pending_locks.length) {
		self.next(0);
		return;
	}

	var filter = self.pending_locks.splice(0);
	filter.wait(function(fn, next) {
		fn.call(self, next);
	}, function() {
		self.next(0);
	});
};

TD.$streamer = function() {

	var self = this;
	self.step = 10;

	if (!self.pending_streamer.length) {
		self.next(0);
		return self;
	}

	self.$reading++;

	var filter = self.pending_streamer.splice(0);
	var length = filter.length;
	var count = 0;
	var fs = new TextStreamReader(self.filename);
	var data = {};

	data.keys = self.$keys;

	fs.array = true;
	fs.start = self.$header;
	fs.divider = '\n';

	if (self.buffersize)
		fs.buffersize = self.buffersize;

	if (self.buffercount)
		fs.buffercount = self.buffercount;

	fs.ondocuments = function() {
		var lines = fs.docs;
		for (var a = 0; a < lines.length; a++) {
			data.line = lines[a].split(DELIMITER);
			data.index = count++;
			var doc = self.parseData(data);
			for (var i = 0; i < length; i++)
				filter[i].fn(doc, filter[i].arg, count);
		}
	};

	fs.$callback = function() {
		for (var i = 0; i < length; i++)
			filter[i].callback && filter[i].callback(null, filter[i]);
		self.$reading--;
		self.next(0);
		fs = null;
	};

	fs.openread();
	return self;
};

TD.allocations = function(enable) {
	this.$allocations = enable;
	return this;
};

TD.parseSchema = function(output, arr) {

	var sized = true;

	output.$schema = {};
	output.$keys = [];
	output.$size = 2;

	for (var i = 0; i < arr.length; i++) {
		var arg = arr[i].split(':');
		var type = 0;
		var T = (arg[1] || '').toLowerCase().trim();
		var size = 0;
		var copy = arg[0].match(/=.*$/g);

		if (copy) {
			arg[0] = arg[0].replace(copy, '').trim();
			copy = (copy + '').replace(/=/g, '').trim();
		}

		var index = T.indexOf('(');
		if (index != -1) {
			size = +T.substring(index + 1, T.lastIndexOf(')'));
			T = T.substring(0, index);
		}

		switch (T) {
			case 'number':
				type = 2;
				!size && (size = 16);
				break;
			case 'boolean':
			case 'bool':
				type = 3;
				size = 1;
				break;
			case 'date':
				type = 4;
				size = 13;
				break;
			case 'object':
				type = 5;
				size = 0;
				sized = false;
				break;
			case 'string':
			default:
				type = 1;
				if (!size)
					sized = false;
				break;
		}
		var name = arg[0].trim();
		output.$schema[name] = { name: name, type: type, pos: i, size: size, copy: copy };
		output.$keys.push(name);
		output.$size += size + 1;
	}

	if (sized) {
		output.$allocations = false;
		output.$size++; // newline
	} else
		output.$size = 0;

	return this;
};

TD.stringifySchema = function(schema) {

	var data = [];

	if (schema.$keys === undefined)
		throw new Error('FET');

	for (var i = 0; i < schema.$keys.length; i++) {

		var key = schema.$keys[i];
		var meta = schema.$schema[key];
		var type = 'string';

		switch (meta.type) {
			case 2:

				type = 'number';

				// string
				if (schema.$size && meta.size !== 16)
					type += '(' + (meta.size) + ')';

				break;

			case 3:
				type = 'boolean';
				break;
			case 4:
				type = 'date';
				break;
			case 5:
				type = 'object';
				break;
			default:
				// string
				if (meta.size)
					type += '(' + (meta.size) + ')';
				break;
		}

		data.push(key + ':' + type);
	}

	return data.join(DELIMITER);
};

TD.parseData = function(data, cache) {

	var self = this;
	var obj = {};
	var esc = data.line[0] === '*';
	var val, alloc;

	if (cache && !self.$size && data.keys.length === data.line.length - 2)
		alloc = data.line[data.line.length - 1].length;

	for (var i = 0; i < data.keys.length; i++) {
		var key = data.keys[i];

		if (cache && cache !== EMPTYOBJECT && cache[key] != null) {
			obj[key] = cache[key];
			continue;
		}

		var meta = self.$schema[key];
		if (meta == null)
			continue;

		var pos = meta.pos + 1;
		var line = data.line[pos];

		if (self.$size) {
			for (var j = line.length - 1; j > -1; j--) {
				if (line[j] !== ' ') {
					line = line.substring(0, j + 1);
					break;
				}
			}
		}

		switch (meta.type) {
			case 1: // String
				obj[key] = line;

				if (esc && obj[key])
					obj[key] = obj[key].replace(REGTUNESCAPE, regtescapereverse);

				break;
			case 2: // Number
				val = +line;
				obj[key] = val < 0 || val > 0 ? val : 0;
				break;
			case 3: // Boolean
				val = line;
				obj[key] = BOOLEAN[val] == 1;
				break;
			case 4: // Date
				val = line;
				obj[key] = val ? new Date(val[10] === 'T' ? val : +val) : null;
				break;
			case 5: // Object
				val = line;
				if (esc && val)
					val = val.replace(REGTUNESCAPE, regtescapereverse);
				obj[key] = val ? val.parseJSON(true) : null;
				break;
		}
	}

	alloc >= 0 && (obj.$$alloc = { size: alloc, length: data.length });
	return obj;
};

TD.stringify = function(doc, insert, byteslen) {

	var self = this;
	var output = '';
	var esc = false;
	var size = 0;

	for (var i = 0; i < self.$keys.length; i++) {
		var key = self.$keys[i];
		var meta = self.$schema[key];
		var val = doc[key];

		switch (meta.type) {
			case 1: // String

				if (self.$size) {
					switch (typeof(val)) {
						case 'number':
							val = val + '';
							break;
						case 'boolean':
							val = val ? '1' : '0';
							break;
						case 'object':
							val = JSON.stringify(val);
							break;
					}

					if (val.length > meta.size)
						val = val.substring(0, meta.size);
					else
						val = val.padRight(meta.size, ' ');

					// bytes
					var diff = meta.size - Buffer.byteLength(val);
					if (diff > 0) {
						for (var j = 0; j < diff; j++)
							val += ' ';
					}

				} else {
					val = val ? val : '';
					if (meta.size && val.length > meta.sized)
						val = val.substring(0, meta.size);
					size += 4;
				}

				break;
			case 2: // Number
				val = (val || 0) + '';
				if (self.$size) {
					if (val.length < meta.size)
						val = val.padRight(meta.size, ' ');
				} else
					size += 2;
				break;

			case 3: // Boolean
				val = (val == true ? '1' : '0');
				break;

			case 4: // Date
				val = val ? val instanceof Date ? val.getTime() : val : '';
				if (self.$size)
					val = (val + '').padRight(meta.size, ' ');
				else if (!val)
					size += 10;
				break;

			case 5: // Object
				val = val ? JSON.stringify(val) : '';
				size += 4;
				break;
		}

		if (!esc && (meta.type === 1 || meta.type === 5)) {
			val += '';
			if (REGTESCAPETEST.test(val)) {
				esc = true;
				val = val.replace(REGTESCAPE, regtescape);
			}
		}

		output += DELIMITER + val;
	}

	if (self.$size && (insert || byteslen)) {
		output += DELIMITER;
	} else if (doc.$$alloc) {
		var l = output.length;
		var a = doc.$$alloc;
		if (l <= a.length) {
			var s = (a.length - l) - 1;
			if (s > 0) {
				output += DELIMITER.padRight(s, '.');
				if (byteslen) {
					var b = byteslen - Buffer.byteLength(output);
					if (b > 0) {
						b--;
						for (var i = 0; i < b; i++)
							output += '.';
					} else {
						var c = s - b;
						if (c > 0)
							output = output.substring(0, (output.length + b) - 1);
					}
				}
			} else if (s === 0)
				output += DELIMITER;
			else
				insert = true;
		} else
			insert = true;
	} else
		insert = true;

	if (insert && size && self.$allocations)
		output += DELIMITER.padRight(size, '.');

	return (esc ? '*' : '+') + output;
};

function regtescapereverse(c) {
	switch (c) {
		case '%0A':
			return '\n';
		case '%0D':
			return '\r';
		case '%7C':
			return '|';
	}
	return c;
}

function regtescape(c) {
	switch (c) {
		case '\n':
			return '%0A';
		case '\r':
			return '%0D';
		case '|':
			return '%7C';
	}
	return c;
}

// ======================================================
// Helper functions
// ======================================================

function jsonparser(key, value) {
	return typeof(value) === 'string' && value.isJSONDate() ? new Date(value) : value;
}

function TextReader(builder) {
	var self = this;
	self.ts = Date.now();
	self.cancelable = true;
	self.builders = [];
	self.canceled = 0;
	builder && self.add(builder);
}

TextReader.prototype.add = function(builder) {
	var self = this;
	if (builder instanceof Array) {
		for (var i = 0; i < builder.length; i++)
			self.add(builder[i]);
	} else {
		builder.$TextReader = self;
		if (builder.$sortname)
			self.cancelable = false;
		self.builders.push(builder);
	}
	return self;
};

TextReader.prototype.compare2 = function(docs, custom, done) {
	var self = this;

	for (var i = 0; i < docs.length; i++) {

		var doc = docs[i];
		if (doc === EMPTYOBJECT)
			continue;

		if (self.builders.length === self.canceled)
			return false;

		var is = false;

		for (var j = 0; j < self.builders.length; j++) {

			var builder = self.builders[j];
			if (builder.canceled)
				continue;

			builder.scanned++;

			if (builder.filterrule(doc, builder.filterarg)) {

				builder.count++;

				if (!builder.$sortname && ((builder.$skip && builder.$skip >= builder.count) || (builder.$take && builder.$take <= builder.counter)))
					continue;

				!is && (is = true);

				builder.counter++;

				var canceled = builder.canceled;
				var c = custom(docs, doc, i, builder, j);

				if (builder.$take === 1) {
					builder.canceled = true;
					self.canceled++;
				} else if (!canceled && builder.canceled)
					self.canceled++;

				if (c === 1)
					break;
				else
					continue;
			}
		}

		is && done && done(docs, doc, i, self.builders);
	}
};

TextReader.prototype.compare = function(docs) {

	var self = this;
	for (var i = 0; i < docs.length; i++) {

		var doc = docs[i];
		if (self.builders.length === self.canceled)
			return false;

		for (var j = 0; j < self.builders.length; j++) {

			var builder = self.builders[j];

			if (builder.canceled)
				continue;

			builder.scanned++;

			if (builder.filterrule(doc, builder.filterarg)) {

				builder.count++;

				if (!builder.$sortname && ((builder.$skip && builder.$skip >= builder.count) || (builder.$take && builder.$take <= builder.counter)))
					continue;

				builder.counter++;

				if (builder.scalarrule)
					builder.scalarrule(doc, builder.scalararg);
				else
					builder.push(doc);

				if (self.cancelable && !builder.$sortname && builder.response.length === builder.$take) {
					builder.canceled = true;
					self.canceled++;
				}
			}
		}
	}
};

TextReader.prototype.callback = function(builder) {
	var self = this;
	for (var i = 0; i < builder.response.length; i++)
		builder.response[i] = builder.prepare(builder.response[i]);
	builder.logrule && builder.logrule();
	builder.filterarg = builder.modifyarg = builder.payload = builder.$fields = builder.$fieldsremove = builder.db = builder.$TextReader = builder.$take = builder.$skip = undefined;
	builder.$callback(null, builder);
	return self;
};

TextReader.prototype.done = function() {
	var self = this;
	var diff = Date.now() - self.ts;

	if (self.db.duration.push({ type: self.type, duration: diff }) > 20)
		self.db.duration.shift();

	for (var i = 0; i < self.builders.length; i++) {
		self.builders[i].duration = diff;
		self.callback(self.builders[i]);
	}
	self.canceled = 0;
	return self;
};

exports.JsonDB = function(name, directory) {
	return new JsonDB(name, directory);
};

exports.TableDB = function(name, directory) {
	return new TableDB(name, directory);
};
