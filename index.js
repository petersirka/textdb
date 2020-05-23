require('total.js');
const Fork = require('child_process').fork;
var INSTANCES = {};

exports.run = function(type, name, directory) {
	var key = (type + '_' + name + '_directory').hash(true) + '';
	var instance = INSTANCES[key] = Fork('./worker.js', [type, name, directory], { cwd: directory });
	instance.$key = key;
	instance.callbacks = {};
	instance.on('message', function(msg) {

		if (msg === 'db:ready') {
			instance.ready && instance.ready();
			return;
		}

		if (msg.id) {
			var cb = instance.callbacks[msg.id];
			if (cb) {
				delete instance.callbacks[msg.id];
				cb(msg);
			}
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

	instance.cmd_alter = function(alter, callback) {
		var id = GUID(10);
		if (callback)
			instance.callbacks[id] = callback;
		instance.send({ TYPE: 'alter', id: id, alter: alter });
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