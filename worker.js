const DB = require('./textdb');
var instance;

switch (process.argv[2]) {
	case 'nosql':
		instance = new DB.JsonDB(process.argv[3], process.argv[4]);
		break;
	case 'table':
		instance = new DB.TableDB(process.argv[3], process.argv[4]);
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
			instance.alter(err => process.send({ id: msg.id, err: err }));
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
	}
});

setTimeout(function() {
	process.send('db:ready');
}, 100);