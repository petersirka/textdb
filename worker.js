const Fs = require('fs');

var STATS = { TYPE: 'stats' };
var counter = 0;
var instance;
var TYPE = process.argv[2];

switch (TYPE) {
	case 'nosql':
		instance = require('./textdb').JsonDB(process.argv[3], process.argv[4]);
		break;
	case 'table':
		instance = require('./textdb').TableDB(process.argv[3], process.argv[4]);
		break;
	case 'binary':
		instance = require('./filedb').FileDB(process.argv[3], process.argv[4]);
		break;
}

process.on('message', function(msg) {
	switch (msg.TYPE) {
		case 'find':
			instance.find().assign(msg.builder).callback(function(err, builder) {
				builder.TYPE = 'response';
				process.send(builder);
			});
			break;
		case 'find2':
			instance.find2().assign(msg.builder).callback(function(err, builder) {
				builder.TYPE = 'response';
				process.send(builder);
			});
			break;
		case 'insert':
			instance.insert().assign(msg.builder).callback(function(err, builder) {
				builder.TYPE = 'response';
				process.send(builder);
			});
			break;
		case 'update':
			instance.update().assign(msg.builder).callback(function(err, builder) {
				builder.TYPE = 'response';
				process.send(builder);
			});
			break;
		case 'remove':
			instance.remove().assign(msg.builder).callback(function(err, builder) {
				builder.TYPE = 'response';
				process.send(builder);
			});
			break;
		case 'alter':
			instance.alter(msg.schema, err => process.send({ id: msg.id, err: err }));
			break;
		case 'clean':
			instance.clean(err => process.send({ id: msg.id, err: err }));
			break;
		case 'clear':
			instance.clear(err => process.send({ id: msg.id, err: err }));
			break;
		case 'drop':
			instance.drop();
			setTimeout(() => process.kill(0), 5000);
			break;

		case 'filedb_save':
			instance.save(msg.id, msg.builder.name, msg.builder.filename, function(err, meta) {
				if (!meta)
					meta = {};
				meta.id = msg.id;
				if (err)
					meta.err = err + '';
				meta.TYPE = 'response';
				process.send(meta);
			});
			break;
		case 'filedb_read':
			break;
	}
});

function statresponse(err, stat) {
	if (stat)
		STATS.size = stat.size;
}

function measure() {

	if (counter++ > 100000000)
		counter = 0;

	STATS.pendingread = instance.pending_reader.length + instance.pending_reader2.length + instance.pending_streamer.length;
	STATS.pendingwrite = instance.pending_update.length + instance.pending_append.length + instance.pending_remove.length;
	STATS.memory = (process.memoryUsage().heapUsed / 1024 / 1024).floor(2);
	STATS.duration = instance.duration;

	if (counter === 1 || counter % 120 && TYPE !== 'binary')
		Fs.stat(instance.filename, statresponse);

	process.send(STATS);
}

setTimeout(function() {
	process.send({ TYPE: 'ready' });
	measure();
}, 100);

setInterval(measure, 5000);