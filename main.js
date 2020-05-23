const Fork = require('child_process').fork;
var INSTANCES = {};

exports.init = function(type, name, directory, callback) {
	var key = (type + '_' + name + '_directory').hash(true) + '';
	var instance = INSTANCES[key] = Fork('./worker.js', [type, name, directory]);
	instance.$key = key;
	instance.callbacks = {};
	instance.on('message', function(msg) {
		switch (msg.TYPE) {
			case 'stats':
				instance.stats = msg;
				instance.emit('stats', msg);
				break;
			case 'ready':
				callback && callback();
				break;
			case 'response':
				var cb = msg.id ? instance.callbacks[msg.id] : null;
				if (cb) {
					delete instance.callbacks[msg.id];
					cb(msg);
				}
				break;
		}
	});
	prepare(instance);
	return instance;
};

exports.kill = function(instance) {
	if (instance.$key) {
		instance.kill();
		delete INSTANCES[instance.$key];
		instance.$key = null;
	}
};

function prepare(instance) {

	instance.cmd_find = function(builder, callback) {
		builder.id = GUID(10);

		if (callback)
			instance.callbacks[builder.id] = callback;

		instance.send({ TYPE: 'find', builder: builder });
	};

	instance.cmd_find2 = function(builder, callback) {
		builder.id = GUID(10);

		if (callback)
			instance.callbacks[builder.id] = callback;

		instance.send({ TYPE: 'find2', builder: builder });
	};

	instance.cmd_remove = function(builder, callback) {
		builder.id = GUID(10);

		if (callback)
			instance.callbacks[builder.id] = callback;

		instance.send({ TYPE: 'find2', builder: builder });
	};

	instance.cmd_update = function(builder, callback) {
		builder.id = GUID(10);

		if (callback)
			instance.callbacks[builder.id] = callback;

		instance.send({ TYPE: 'update', builder: builder });
	};

	instance.cmd_insert = function(builder, callback) {
		builder.id = GUID(10);

		if (callback)
			instance.callbacks[builder.id] = callback;

		instance.send({ TYPE: 'insert', builder: builder });
	};

	instance.cmd_alter = function(schema, callback) {
		var id = GUID(10);
		if (callback)
			instance.callbacks[id] = callback;
		instance.send({ TYPE: 'alter', id: id, schema: schema });
	};

	instance.cmd_clean = function(callback) {
		var id = GUID(10);
		if (callback)
			instance.callbacks[id] = callback;
		instance.send({ TYPE: 'clean', id: id });
	};

	instance.cmd_clear = function(callback) {
		var id = GUID(10);
		if (callback)
			instance.callbacks[id] = callback;
		instance.send({ TYPE: 'clear', id: id });
	};

}