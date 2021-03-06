require('total.js');

const Main = require('./main');
const Path = require('path');
const COLLECTIONS = {};
const DIRECTORY = Path.join(process.cwd(), 'databases/');
var DBCACHE = {};

NEWSCHEMA('Collections', function(schema) {

	schema.define('icon', 'String(20)');
	schema.define('name', String, true);

	schema.setQuery(function($) {
		var collections = PREF.collections || {};
		var keys = Object.keys(collections);
		var output = [];

		for (var i = 0; i < keys.length; i++) {
			var item = collections[keys[i]];
			var obj = {};
			obj.id = item.id;
			obj.icon = item.icon;
			obj.name = item.name;
			obj.tokens = item.tokens;
			obj.databases = [];
			var ref = COLLECTIONS[item.id];
			for (var j = 0; j < item.databases.length; j++) {
				var item2 = item.databases[j];
				var obj2 = {};
				obj2.type = item2.type;
				obj2.id = item2.id;
				obj2.icon = item2.icon;
				obj2.name = item2.name;

				if (ref && ref.databases[obj2.id])
					obj2.stats = ref.databases[obj2.id].instance.stats;

				obj.databases.push(obj2);
			}

			output.push(obj);
		}

		$.callback(output);
	});

	schema.setInsert(function($) {

		var model = $.clean();
		model.id = UID();
		model.dtcreated = new Date();
		model.databases = [];
		model.tokens = [GUID(30)];

		var collections = PREF.collections || {};
		collections[model.id] = model;

		PREF.set('collections', collections);
		$.success(model.id);
		reloadcollections();
	});

	schema.setUpdate(function($) {

		var model = $.clean();
		var collections = PREF.collections || EMPTYOBJECT;

		if (!collections[$.id]) {
			$.invalid('error-collections-404');
			return;
		}

		var col = collections[$.id];

		col.name = model.name;
		col.icon = model.icon;
		col.dtupdated = new Date();

		PREF.set('collections', collections);
		$.success($.id);
		reloadcollections();
	});

	schema.setRemove(function($) {

		var collections = PREF.collections || EMPTYOBJECT;

		if (!collections[$.id]) {
			$.invalid('error-collections-404');
			return;
		}

		delete collections[$.id];
		PREF.set('collections', collections);
		$.success();
		reloadcollections();
	});

});

NEWSCHEMA('Collections/Databases', function(schema) {

	schema.define('type', ['nosql', 'table', 'binary'])('nosql');
	schema.define('name', 'Lower(30)', true);
	schema.define('schema', String);
	schema.define('allocations', Boolean);

	schema.setInsert(function($) {

		var collections = PREF.collections || EMPTYOBJECT;
		var model = $.clean();

		if (!collections[$.id]) {
			$.invalid('error-collections-404');
			return;
		}

		model.id = UID();
		model.dtcreated = new Date();
		collections[$.id].databases.push(model);
		PREF.set('collections', collections);
		$.success(model.id);
		reloadcollections();
	});

	schema.setUpdate(function($) {

		var collections = PREF.collections || EMPTYOBJECT;
		var model = $.clean();

		if (!collections[$.id]) {
			$.invalid('error-collections-404');
			return;
		}

		var db = collections[$.id];
		var database = db.databases.findItem('id', $.dbid);
		if (!database) {
			$.invalid('error-databases-404');
			return;
		}

		database.dtupdated = new Date();
		database.tokens = model.tokens;
		database.schema = model.schema;
		database.allocations = model.allocations;

		PREF.set('collections', collections);
		$.success(model.id);
		reloadcollections();
	});

	schema.setRemove(function($) {

		var collections = PREF.collections || EMPTYOBJECT;
		var model = $.clean();

		if (!collections[model.projectid]) {
			$.invalid('error-collections-404');
			return;
		}

		var db = collections[model.projectid];
		var index = db.databases.findIndex('id', $.id);
		if (index === -1) {
			$.invalid('error-databases-404');
			return;
		}

		db.databases.splice(index, 1);

		PREF.set('collections', collections);
		$.success(model.id);
		reloadcollections();
	});
});

ROUTE('GET       /');
ROUTE('GET       /collections/                           *Collections --> @query');
ROUTE('POST      /collections/                           *Collections --> @insert');
ROUTE('POST      /collections/{id}/                      *Collections --> @update');
ROUTE('DELETE    /collections/{id}/                      *Collections --> @remove');

ROUTE('POST      /collections/{id}/databases/            *Collections/Databases --> @insert');
ROUTE('POST      /collections/{id}/databases/{dbid}/     *Collections/Databases --> @update');
ROUTE('DELETE    /collections/{id}/databases/{dbid}/     *Collections/Databases --> @remove');

WEBSOCKET('/', function() {

	var self = this;

	self.autodestroy();

	self.on('open', function(client) {
		var col = COLLECTIONS[DBCACHE[client.query.token] || ''];
		if (!col)
			client.close(4004);
	});

	self.on('message', function(client, message) {

		var callback = function(response, err) {
			client && client.send({ id: message.id, err: err, response: response });
		};

		var col = COLLECTIONS[DBCACHE[client.query.token] || ''];
		if (!col) {
			callback(null, 'Token is invalid');
			client.close(4004);
			return;
		}

		var db = col.databases[DBCACHE[message.db] || ''];
		if (db)
			query(db.instance, message, callback);
		else
			callback(null, 'Database "{0}" not found'.format(message.db));

	});

}, ['json']);

function query(instance, message, callback) {
	switch (message.command) {
		case 'find':
			instance.cmd_find(message.builder, callback);
			break;
		case 'find2':
			instance.cmd_find2(message.builder, callback);
			break;
		case 'insert':
			instance.cmd_insert(message.builder, callback);
			break;
		case 'update':
			instance.cmd_update(message.builder, callback);
			break;
		case 'remove':
			instance.cmd_remove(message.builder, callback);
			break;
		case 'clear':
			instance.cmd_clear(callback);
			break;
		case 'clean':
			instance.cmd_clean(callback);
			break;
		case 'alter':
			instance.cmd_alter(message.schema, callback);
			break;
		case 'stats':
			callback(instance.stats);
			break;
		default:
			callback(null, 'Command not found');
			break;
	}
}

ROUTE('POST /collections/{token}/databases/{name}/query/', function() {
	var self = this;
	var col = COLLECTIONS[DBCACHE[self.id] || ''];
	if (col) {
		var db = col.databases[DBCACHE[self.params.name] || ''];
		if (db)
			query(db.instance, self.body, self.callback());
		else
			self.invalid('error-databases-404');
	} else
		self.invalid('error-collections-404');
});

function reloadcollections() {

	var collections = PREF.collections || EMPTYOBJECT;
	DBCACHE = {};

	Object.keys(collections).wait(function(key, next) {
		var col = collections[key];
		var instance = COLLECTIONS[key];

		if (instance) {
			// check update

		} else {
			instance = {};
			instance.id = col.id;
			instance.name = col.name;
			instance.schema = col.schema;
			instance.type = col.type;
			instance.databases = {};
			COLLECTIONS[key] = instance;
		}

		for (var i = 0; i < col.tokens.length; i++)
			DBCACHE[col.tokens[i]] = col.id;

		var stamp = GUID(10);

		col.databases.wait(function(item, next) {
			DBCACHE[item.name] = item.id;
			var db = instance.databases[item.id];
			if (db) {
				db.stamp = stamp;
				if (item.type === 'table' && db.schema !== item.schema)
					db.instance.cmd_alter(db.schema);
				next();
			} else {
				var dir = item.type === 'binary' ? Path.join(DIRECTORY, col.id, item.id + '.fdb') : Path.join(DIRECTORY, col.id);
				PATH.mkdir(dir);
				db = instance.databases[item.id] = {};
				db.stamp = stamp;
				db.dir = dir;
				db.instance = Main.init(item.type, item.id, dir, function() {
					if (item.type === 'table' && item.schema)
						db.instance.cmd_alter(item.schema);
					next();
				});
			}

			var keys = Object.keys(instance.databases);
			for (var i = 0; i < keys.length; i++) {
				var key = keys[i];
				var db = instance.databases[key];
				if (db.stamp !== stamp) {
					Main.kill(db.instance);
					delete instance.databases[key];
				}
			}
		}, next);
	});
}

/*
function nosql() {
	var database = new DB.JsonDB('test', '');
	// database.insert({ id: UID(), name: GUID(30), price: U.random(100, 50), date: new Date() });
	// database.find().filter('true').callback(console.log);
	// database.update().filter('item.id==="161256001hl61b"').modify('item.price=100020').callback(console.log);
	// database.remove().filter('item.id==="161256001hl61b"').callback(console.log);
	// database.clean();

	// database.find().filter('true').take(5).fields('id,price').sort('price', true).callback(console.log).log({ user: 'asdlkjsadljas' });
	// database.update().filter('item.id==="161324003ek61b"').modify('item.price=1').backup({ user: 'Peter' });
	database.find().filter('true').take(5).scalar('arg.price=Math.max(arg.price || 0, item.price)', {}).callback(console.log);
	// for (var i = 0; i < 10000; i++)
	//  	database.insert({ id: UID(), name: GUID(30), price: U.random(100, 50), date: new Date() });

	//database.backups(console.log);
}

function table() {
	var database = new DB.Table('test');

	// database.alter('id:string,price:number,name:string,date:Date');
	// database.insert({ id: UID(), name: GUID(30), price: U.random(100, 50), date: new Date() });
	// database.find().filter('true').callback(console.log);
	// database.update().filter('item.id==="161295001rj61b"').modify('item.price=101').callback(console.log);
	// database.remove().filter('item.id==="161295001rj61b"').callback(console.log);
	// database.clean(console.log);

	database.update().filter('item.id==="161324003ek61b"').modify('item.price=1').backup({ user: 'Peter' });

	// database.find().filter('true').fields('id,price').take(5).sort('price', true).callback(console.log);

	// for (var i = 0; i < 10000; i++)
	// 	database.insert({ id: UID(), name: GUID(30), price: U.random(100, 50), date: new Date() });
}

// table();
nosql();
*/

/*
var instance = Main.init('nosql', 'skuska', '', function() {
	console.log('OK');
	// instance.cmd_insert({ payload: { id: UID(), name: GUID(30), price: U.random(100, 50), date: new Date() }}, console.log);
	// instance.cmd_alter('id:string,name:string,price:number,date:Date');
	// instance.cmd_find2({ filter: 'doc', fields: 'id,price' }, console.log);
	// instance.cmd_find2({ filter: 'doc', fields: 'id,price' }, console.log);
	// instance.cmd_find2({ filter: 'doc', fields: 'id,price' }, console.log);
});
*/

ON('ready', reloadcollections);
F.http('debug');