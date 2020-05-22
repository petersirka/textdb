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
 * @module NoSQL
 * @version 1.0.0
 */

'use strict';

require('total.js');

const Fs = require('fs');
const Path = require('path');
const NoSQLStream = require('./nosqlstream');
const QueryBuilder = require('./builder').QueryBuilder;
const DELIMITER = '|';

const EXTENSION = '.nosql';
const EXTENSION_TABLE = '.table';
const EXTENSION_LOG = '.nosql-log';
const JSONBOOL = '":true ';
const NEWLINE = '\n';
const REGBOOL = /":true/g; // for updates of boolean types
const REGTESCAPE = /\||\n|\r/g;
const REGTUNESCAPE = /%7C|%0D|%0A/g;
const REGTESCAPETEST = /\||\n|\r/;
const BOOLEAN = { '1': 1, 'true': 1, 'on': 1 };
const TABLERECORD = { '+': 1, '-': 1, '*': 1 };

const COMPARER = global.Intl ? global.Intl.Collator().compare : function(a, b) {
	return a.removeDiacritics().localeCompare(b.removeDiacritics());
};

const CACHE = {};
const JSONBUFFER = 40;
var CLEANER = {};

function Table(name) {

	var t = this;
	t.filename = name + EXTENSION_TABLE;
	t.name = name;
	t.$name = '$' + name;
	t.pending_reader = [];
	t.pending_reader2 = [];
	t.pending_update = [];
	t.pending_append = [];
	t.pending_reader = [];
	t.pending_remove = [];
	t.pending_streamer = [];
	t.pending_clean = [];
	t.pending_clear = [];
	t.pending_locks = [];

	t.step = 0;
	t.ready = false;
	t.$free = true;
	t.$writting = false;
	t.$reading = false;
	t.$allocations = true;

	t.next2 = function() {
		t.next(0);
	};

	Fs.createReadStream(t.filename, { end: 1200 }).once('data', function(chunk) {
		t.parseSchema(chunk.toString('utf8').split('\n', 1)[0].split(DELIMITER));
		t.ready = true;
		t.$header = Buffer.byteLength(t.stringifySchema()) + 1;
		t.next(0);
	}).on('error', function() {
		// NOOP
	});
}

function Database(name) {

	var self = this;
	self.filename = name + EXTENSION;
	self.filenameLog = EXTENSION_LOG;

	self.name = name;
	self.pending_update = [];
	self.pending_append = [];
	self.pending_reader = [];
	self.pending_remove = [];
	self.pending_reader2 = [];
	self.pending_streamer = [];
	self.pending_clean = [];
	self.pending_clear = [];
	self.pending_locks = [];
	self.step = 0;
	self.pending_drops = false;
	self.$timeoutmeta;
	self.$free = true;
	self.$writting = false;
	self.$reading = false;

	self.next2 = function() {
		self.next(0);
	};
}

const TP = Table.prototype;
const DP = Database.prototype;

TP.memory = DP.memory = function(count, size) {
	var self = this;
	count && (self.buffercount = count + 1);      // def: 15 - count of stored documents in memory while reading/writing
	size && (self.buffersize = size * 1024);  // def: 32 - size of buffer in kB
	return self;
};

TP.alter = function(schema) {

	var self = this;

	if (self.$header) {

		if (schema) {
			self.parseSchema(schema.replace(/;|,/g, DELIMITER).trim().split(DELIMITER));
			schema = self.stringifySchema();
		}

		self.ready = true;

		if (schema && self.stringifySchema() !== schema) {
			self.$header = Buffer.byteLength(self.stringifySchema()) + 1;
			self.extend(schema);
		} else
			self.$header = Buffer.byteLength(schema) + 1;

		self.next(0);

	} else {
		self.parseSchema(schema.replace(/;|,/g, DELIMITER).trim().split(DELIMITER));
		var bschema = self.stringifySchema();
		self.$header = Buffer.byteLength(bschema) + 1;
		Fs.writeFileSync(self.filename, bschema + NEWLINE, 'utf8');
		self.ready = true;
		self.next(0);
	}
};

function next_operation(self, type) {
	self.next(type);
}

DP.insert = function(doc) {
	var self = this;
	var builder = new QueryBuilder(self);
	var json = doc.$$schema ? doc.$clean() : doc;
	self.pending_append.push({ doc: JSON.stringify(json).replace(REGBOOL, JSONBOOL), raw: doc, builder: builder });
	setImmediate(next_operation, self, 1);
	return builder;
};

DP.update = function() {
	var self = this;
	var builder = new QueryBuilder(self);
	self.pending_update.push(builder);
	setImmediate(next_operation, self, 2);
	return builder;
};

DP.restore = function(filename, callback) {
	var self = this;

	U.wait(() => !self.type, function(err) {

		if (err)
			throw new Error('Database can\'t be restored because it\'s busy.');

		self.type = 9;

		F.restore(filename, F.path.root(), function(err, response) {
			self.type = 0;
			callback && callback(err, response);
		});

	});
	return self;
};

DP.backup = function(filename, callback) {

	var self = this;
	var list = [];
	var pending = [];

	pending.push(function(next) {
		F.path.exists(self.filename, function(e) {
			e && list.push(Path.join(CONF.directory_databases, self.name + EXTENSION));
			next();
		});
	});

	pending.push(function(next) {
		F.path.exists(self.filenameLog, function(e) {
			e && list.push(Path.join(CONF.directory_databases, self.name + EXTENSION_LOG));
			next();
		});
	});

	pending.async(function() {
		if (list.length)
			F.backup(filename, list, callback);
		else
			callback(new Error('No files for backuping.'));
	});

	return self;
};

DP.drop = function() {
	var self = this;	self.pending_drops = true;
	setImmediate(next_operation, self, 7);
	return self;
};

TP.clear = DP.clear = function(callback) {
	var self = this;
	self.pending_clear.push(callback || NOOP);
	setImmediate(next_operation, self, 12);
	return self;
};

TP.clean = DP.clean = function(callback) {
	var self = this;
	self.pending_clean.push(callback || NOOP);
	setImmediate(next_operation, self, 13);
	return self;
};

TP.lock = DP.lock = function(callback) {
	var self = this;
	self.pending_locks.push(callback || NOOP);
	setImmediate(next_operation, self, 14);
	return self;
};

DP.remove = function() {
	var self = this;
	var builder = new QueryBuilder(self);
	self.pending_remove.push(builder);
	setImmediate(next_operation, self, 3);
	return builder;
};

DP.find = function(builder) {
	var self = this;
	if (builder instanceof QueryBuilder)
		builder.db = self;
	else
		builder = new QueryBuilder(self);
	self.pending_reader.push(builder);
	setImmediate(next_operation, self, 4);
	return builder;
};

DP.find2 = function(builder) {
	var self = this;
	if (builder instanceof QueryBuilder)
		builder.db = self;
	else {
		builder = new QueryBuilder(self);
		builder.$options.notall = true;
	}
	self.pending_reader2.push(builder);
	setImmediate(next_operation, self, 11);
	return builder;
};

DP.stream = function(fn, arg, callback) {
	var self = this;

	if (typeof(arg) === 'function') {
		callback = arg;
		arg = null;
	}

	self.pending_streamer.push({ fn: fn, callback: callback, arg: arg || {} });
	setImmediate(next_operation, self, 10);
	return self;
};

DP.scalar = function(type, field) {
	return this.find().scalar(type, field);
};

DP.count = function() {
	var self = this;
	var builder = new QueryBuilder(self);
	self.pending_reader.push(builder);
	setImmediate(next_operation, self, 4);
	return builder;
};

DP.one = DP.read = function() {
	var self = this;
	var builder = new QueryBuilder(self);
	builder.first();
	self.pending_reader.push(builder);
	setImmediate(next_operation, self, 4);
	return builder;
};

DP.one2 = DP.read2 = function() {
	var self = this;
	var builder = new QueryBuilder(self);
	builder.first();
	self.pending_reader2.push(builder);
	setImmediate(next_operation, self, 11);
	return builder;
};

//  1 append
//  2 update
//  3 remove
//  4 reader
//  5 views
//  6 reader views
//  7 drop
//  8 backup
//  9 restore
// 10 streamer
// 11 reader reverse
// 12 clear
// 13 clean
// 14 locks

const NEXTWAIT = { 7: true, 8: true, 9: true, 12: true, 13: true, 14: true };

DP.next = function(type) {

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

	if (!this.$reading) {

		if (this.step !== 4 && this.pending_reader.length) {
			this.$reader();
			return;
		}

		if (this.step !== 11 && this.pending_reader2.length) {
			this.$reader3();
			return;
		}

		if (this.step !== 10 && this.pending_streamer.length) {
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

DP.$append = function() {
	var self = this;
	self.step = 1;

	if (!self.pending_append.length) {
		self.next(0);
		return;
	}

	self.$writting = true;

	self.pending_append.splice(0).limit(JSONBUFFER, function(items, next) {

		var json = '';
		for (var i = 0, length = items.length; i < length; i++) {
			json += items[i].doc + NEWLINE;
		}

		Fs.appendFile(self.filename, json, function(err) {

			err && F.error(err, 'NoSQL insert: ' + self.name);

			for (var i = 0, length = items.length; i < length; i++) {
				var callback = items[i].builder.$callback;
				callback && callback(err, 1);
			}

			next();
		});

	}, () => setImmediate(next_append, self));
};

function next_append(self) {
	self.$writting = false;
	self.next(0);
}

DP.$update = function() {

	var self = this;
	self.step = 2;

	if (!self.pending_update.length) {
		self.next(0);
		return self;
	}

	self.$writting = true;

	var filter = self.pending_update.splice(0);
	var filters = new NoSQLReader();
	var fs = new NoSQLStream(self.filename);
	var change = false;

	for (var i = 0; i < filter.length; i++)
		filters.add(filter[i], true);

	if (self.buffersize)
		fs.buffersize = self.buffersize;

	if (self.buffercount)
		fs.buffercount = self.buffercount;

	var update = function(docs, doc, dindex, f, findex) {
		// var rec = fs.docsbuffer[dindex];
		// var fil = filter[findex];
		f.modifyrule(docs[dindex], f.modifyarg);
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
			builder.$nosqlreader = undefined;
			builder.$callback && builder.$callback(null, builder);
		}

		change && CLEANER[self.name] && (CLEANER[self.name] = 1);
	};

	fs.openupdate();
	return self;
};

DP.$reader = function() {

	var self = this;
	self.step = 4;

	if (!self.pending_reader.length) {
		self.next(0);
		return self;
	}

	var list = self.pending_reader.splice(0);
	self.$reading = true;
	self.$reader2(self.filename, list, function() {
		self.$reading = false;
		self.next(0);
	});
	return self;
};

DP.$reader2 = function(filename, items, callback, reader) {

	var self = this;
	var fs = new NoSQLStream(self.filename);
	var filters = new NoSQLReader(items);

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

DP.$reader3 = function() {

	var self = this;
	self.step = 11;

	if (!self.pending_reader2.length) {
		self.next(0);
		return self;
	}

	self.$reading = true;

	var fs = new NoSQLStream(self.filename);
	var filters = new NoSQLReader(self.pending_reader2.splice(0));

	if (self.buffersize)
		fs.buffersize = self.buffersize;

	if (self.buffercount)
		fs.buffercount = self.buffercount;

	fs.ondocuments = function() {
		return filters.compare(JSON.parse('[' + fs.docs + ']', jsonparser));
	};

	fs.$callback = function() {
		filters.done();
		self.$reading = false;
		fs = null;
		self.next(0);
	};

	fs.openreadreverse();
	return self;
};

DP.$streamer = function() {

	var self = this;
	self.step = 10;

	if (!self.pending_streamer.length) {
		self.next(0);
		return self;
	}

	self.$reading = true;

	var filter = self.pending_streamer.splice(0);
	var length = filter.length;
	var count = 0;
	var fs = new NoSQLStream(self.filename);

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
				filter[i].fn(json, filter[i].repository, count);
		}
	};

	fs.$callback = function() {
		for (var i = 0; i < length; i++)
			filter[i].callback && filter[i].callback(null, filter[i].repository, count);
		self.$reading = false;
		self.next(0);
		fs = null;
	};

	fs.openread();
	return self;
};

DP.$remove = function() {

	var self = this;
	self.step = 3;

	if (!self.pending_remove.length) {
		self.next(0);
		return;
	}

	self.$writting = true;

	var fs = new NoSQLStream(self.filename);
	var filter = self.pending_remove.splice(0);
	var filters = new NoSQLReader(filter);
	var change = false;

	if (self.buffersize)
		fs.buffersize = self.buffersize;

	if (self.buffercount)
		fs.buffercount = self.buffercount;

	var remove = function(docs, d, dindex, f) {
		// var rec = fs.docsbuffer[dindex];
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
		change && CLEANER[self.name] && (CLEANER[self.name] = 1);
	};

	fs.openupdate();
};

DP.$clear = function() {

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

DP.$clean = function() {

	var self = this;
	self.step = 13;

	if (!self.pending_clean.length) {
		self.next(0);
		return;
	}

	var filter = self.pending_clean.splice(0);
	var length = filter.length;
	var now = Date.now();

	CLEANER[self.name] = undefined;
	CONF.nosql_logger && PRINTLN('NoSQL embedded "{0}" cleaning (beg)'.format(self.name));

	var fs = new NoSQLStream(self.filename);
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
			CONF.nosql_logger && PRINTLN('NoSQL embedded "{0}" cleaning (end, {1}s)'.format(self.name, (((Date.now() - now) / 1000) >> 0)));
			for (var i = 0; i < length; i++)
				filter[i]();
			self.next(0);
			fs = null;
		});
	});

	fs.openread();
};

DP.$lock = function() {

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

DP.$drop = function() {
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

TP.insert = function(doc) {
	var self = this;
	var builder = new QueryBuilder(self);
	self.pending_append.push({ doc: doc, builder: builder });
	setImmediate(next_operation, self, 1);
	return builder;
};

TP.update = function() {
	var self = this;
	var builder = new QueryBuilder(self);
	self.pending_update.push(builder);
	setImmediate(next_operation, self, 2);
	return builder;
};

TP.remove = function() {
	var self = this;
	var builder = new QueryBuilder(self);
	self.pending_remove.push(builder);
	setImmediate(next_operation, self, 3);
	return builder;
};

TP.find = function(builder) {
	var self = this;	if (builder)
		builder.db = self;
	else
		builder = new QueryBuilder(self);
	self.pending_reader.push(builder);
	setImmediate(next_operation, self, 4);
	return builder;
};

TP.find2 = function(builder) {
	var self = this;
	if (builder)
		builder.db = self;
	else
		builder = new QueryBuilder(self);
	self.pending_reader2.push(builder);
	setImmediate(next_operation, self, 11);
	return builder;
};

TP.stream = function(fn, repository, callback) {
	var self = this;
	if (typeof(repository) === 'function') {
		callback = repository;
		repository = null;
	}

	self.pending_streamer.push({ fn: fn, callback: callback, repository: repository || {} });
	setImmediate(next_operation, self, 10);
	return self;
};

TP.extend = function(schema, callback) {
	var self = this;
	self.lock(function(next) {

		var olds = self.$schema;
		var oldk = self.$keys;
		var oldl = self.$size;
		var oldh = Buffer.byteLength(self.stringifySchema() + NEWLINE);

		self.parseSchema(schema.replace(/;|,/g, DELIMITER).trim().split(DELIMITER));

		var meta = self.stringifySchema() + NEWLINE;
		var news = self.$schema;
		var newk = self.$keys;
		self.$schema = olds;
		self.$keys = oldk;

		var count = 0;
		var fs = new NoSQLStream(self.filename);
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


TP.throwReadonly = function() {
	throw new Error('Table "{0}" doesn\'t contain any schema'.format(this.name));
};

TP.scalar = function(type, field) {
	return this.find().scalar(type, field);
};

TP.next = function(type) {

	if (!this.ready || (type && NEXTWAIT[this.step]))
		return;

	if (!this.$writting && !this.$reading) {

		if (this.step !== 12 && this.pending_clear.length) {
			this.$clear();
			return;
		}

		if (this.step !== 13 && this.pending_clean.length) {
			console.log('OK');
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

	if (!this.$reading) {

		if (this.step !== 4 && this.pending_reader.length) {
			this.$reader();
			return;
		}

		if (this.step !== 11 && this.pending_reader2.length) {
			this.$reader3();
			return;
		}

		if (this.step !== 10 && this.pending_streamer.length) {
			this.$streamer();
			return;
		}
	}

	if (this.step !== type) {
		this.step = 0;
		setImmediate(next_operation, this, 0);
	}
};

TP.$append = function() {
	var self = this;
	self.step = 1;

	if (!self.pending_append.length) {
		self.next(0);
		return;
	}

	self.$writting = true;

	self.pending_append.splice(0).limit(JSONBUFFER, function(items, next) {

		var data = '';

		for (var i = 0, length = items.length; i < length; i++)
			data += self.stringify(items[i].doc, true) + NEWLINE;

		Fs.appendFile(self.filename, data, function(err) {
			err && F.error(err, 'Table insert: ' + self.name);
			for (var i = 0, length = items.length; i < length; i++) {
				// items[i].builder.$options.log && items[i].builder.log();
				var callback = items[i].builder.$callback;
				callback && callback(err, 1);
			}
			next();
		});

	}, () => setImmediate(next_append, self));
};

TP.$reader = function() {

	var self = this;

	self.step = 4;

	if (!self.pending_reader.length) {
		self.next(0);
		return self;
	}

	self.$reading = true;

	var fs = new NoSQLStream(self.filename);
	var filters = new NoSQLReader(self.pending_reader.splice(0));
	var data = {};
	var indexer = 0;

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
		self.$reading = false;
		self.next(0);
	};

	fs.openread();
	return self;
};

TP.$reader3 = function() {

	var self = this;

	self.step = 11;

	if (!self.pending_reader2.length) {
		self.next(0);
		return self;
	}

	self.$reading = true;

	var fs = new NoSQLStream(self.filename);
	var filters = new NoSQLReader(self.pending_reader2.splice(0));
	var data = {};
	var indexer = 0;

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
		self.$reading = false;
		self.next(0);
	};

	fs.openreadreverse();
	return self;
};

TP.$update = function() {

	var self = this;
	self.step = 2;

	if (!self.pending_update.length) {
		self.next(0);
		return self;
	}

	self.$writting = true;

	var fs = new NoSQLStream(self.filename);
	var filter = self.pending_update.splice(0);
	var filters = new NoSQLReader();
	var change = false;
	var indexer = 0;
	var data = { keys: self.$keys };

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
			builder.$nosqlreader = undefined;
			builder.$callback && builder.$callback(null, builder);
		}

		change && CLEANER[self.name] && (CLEANER[self.name] = 1);
	};

	fs.openupdate();
	return self;
};

TP.$remove = function() {

	var self = this;
	self.step = 3;

	if (!self.pending_remove.length) {
		self.next(0);
		return;
	}

	self.$writting = true;

	var fs = new NoSQLStream(self.filename);
	var filter = self.pending_remove.splice(0);
	var filters = new NoSQLReader(filter);
	var change = false;
	var indexer = 0;

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
		// var rec = fs.docsbuffer[dindex];
		// f.builder.$options.backup && f.builder.$backupdoc(rec.doc);
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
		change && CLEANER[self.$name] && (CLEANER[self.$name] = 1);
	};

	fs.openupdate();
};

TP.$clean = function() {

	var self = this;
	self.step = 13;

	if (!self.pending_clean.length) {
		self.next(0);
		return;
	}

	var filter = self.pending_clean.splice(0);
	var length = filter.length;
	var now = Date.now();

	CLEANER[self.$name] = undefined;
	CONF.nosql_logger && PRINTLN('NoSQL Table "{0}" cleaning (beg)'.format(self.name));

	var fs = new NoSQLStream(self.filename);
	var writer = Fs.createWriteStream(self.filename + '-tmp');

	writer.write(self.stringifySchema() + NEWLINE);

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
			CONF.nosql_logger && PRINTLN('NoSQL Table "{0}" cleaning (end, {1}s)'.format(self.name, (((Date.now() - now) / 1000) >> 0)));
			for (var i = 0; i < length; i++)
				filter[i]();
			self.next(0);
			fs = null;
		});
	});

	fs.openread();
};

TP.$clear = function() {

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

		Fs.appendFile(self.filename, self.stringifySchema() + NEWLINE, function() {
			self.next(0);
		});
	});
};

TP.$lock = function() {

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

TP.$streamer = function() {

	var self = this;
	self.step = 10;

	if (!self.pending_streamer.length) {
		self.next(0);
		return self;
	}

	self.$reading = true;

	var filter = self.pending_streamer.splice(0);
	var length = filter.length;
	var count = 0;
	var fs = new NoSQLStream(self.filename);
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
				filter[i].fn(doc, filter[i].repository, count);
		}
	};

	fs.$callback = function() {
		for (var i = 0; i < length; i++)
			filter[i].callback && filter[i].callback(null, filter[i]);
		self.$reading = false;
		self.next(0);
		fs = null;
	};

	fs.openread();
	return self;
};

TP.allocations = function(enable) {
	this.$allocations = enable;
	return this;
};

TP.parseSchema = function() {
	var self = this;
	var arr = arguments[0] instanceof Array ? arguments[0] : arguments;
	var sized = true;

	self.$schema = {};
	self.$keys = [];
	self.$size = 2;

	for (var i = 0; i < arr.length; i++) {
		var arg = arr[i].split(':');
		var type = 0;
		var T = (arg[1] || '').toLowerCase().trim();
		var size = 0;

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
		self.$schema[name] = { type: type, pos: i, size: size };
		self.$keys.push(name);
		self.$size += size + 1;
	}

	if (sized) {
		self.$allocations = false;
		self.$size++; // newline
	} else
		self.$size = 0;

	return self;
};

TP.stringifySchema = function() {

	var self = this;
	var data = [];

	for (var i = 0; i < self.$keys.length; i++) {

		var key = self.$keys[i];
		var meta = self.$schema[key];
		var type = 'string';

		switch (meta.type) {
			case 2:

				type = 'number';

				// string
				if (self.$size && meta.size !== 16)
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

TP.parseData = function(data, cache) {

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

TP.stringify = function(doc, insert, byteslen) {

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

function NoSQLReader(builder) {
	var self = this;
	self.ts = Date.now();
	self.cancelable = true;
	self.builders = [];
	self.canceled = 0;
	builder && self.add(builder);
}

NoSQLReader.prototype.add = function(builder) {
	var self = this;
	if (builder instanceof Array) {
		for (var i = 0; i < builder.length; i++)
			self.add(builder[i]);
	} else {
		builder.$nosqlreader = self;
		if (builder.$sortname)
			self.cancelable = false;
		self.builders.push(builder);
	}
	return self;
};

NoSQLReader.prototype.compare2 = function(docs, custom, done) {
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

NoSQLReader.prototype.compare = function(docs) {

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

				if (self.cancelable && !builder.$sortname && builder.items.length === builder.$take) {
					builder.canceled = true;
					self.canceled++;
				}
			}
		}
	}
};

NoSQLReader.prototype.callback = function(builder) {
	var self = this;
	for (var i = 0; i < builder.items.length; i++)
		builder.items[i] = builder.transform(builder.items[i]);
	builder.$nosqlreader = undefined;
	builder.$callback(null, builder);
	return self;
};

NoSQLReader.prototype.done = function() {
	var self = this;
	var diff = Date.now() - self.ts;
	for (var i = 0; i < self.builders.length; i++) {
		self.builders[i].duration = diff;
		self.callback(self.builders[i]);
	}
	self.canceled = 0;
	return self;
};

function nosql() {
	var database = new Database('test');
	// database.insert({ id: UID(), name: GUID(30), price: U.random(100, 50), date: new Date() });
	// database.find().filter('true').callback(console.log);
	// database.update().filter('item.id==="161256001hl61b"').modify('item.price=100020').callback(console.log);
	// database.remove().filter('item.id==="161256001hl61b"').callback(console.log);
	// database.clean();
}

function table() {
	var database = new Table('test');

	// database.alter('id:string,price:number,name:string,date:Date');
	// database.insert({ id: UID(), name: GUID(30), price: U.random(100, 50), date: new Date() });
	// database.find().filter('true').callback(console.log);
	// database.update().filter('item.id==="161295001rj61b"').modify('item.price=101').callback(console.log);
	// database.remove().filter('item.id==="161295001rj61b"').callback(console.log);
	// database.clean(console.log);

	database.find().filter(true).take(5).sort('price', true).callback(console.log);
	// <for (var i = 0; i < 10000; i++)
	// 	database.insert({ id: UID(), name: GUID(30), price: U.random(100, 50), date: new Date() });
}

// table();
// nosql();
table();