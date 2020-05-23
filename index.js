require('total.js');

const Main = require('./main');
const Path = require('path');
const COLLECTIONS = {};
const DIRECTORY = Path.join(process.cwd(), 'databases/');

NEWSCHEMA('Collections', function(schema) {

	schema.define('name', String, true);
	schema.define('token', '[String]');

	schema.setInsert(function($) {

		var model = $.clean();
		model.id = UID();
		model.dtcreated = new Date();
		model.databases = [];

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

		col.token = model.token;
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
	schema.define('replication', '[String]');

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
		var database = db.databases.findItem('id', $.id);
		if (!database) {
			$.invalid('error-databases-404');
			return;
		}

		database.dtupdated = new Date();
		database.replication = model.replication;
		database.token = model.token;
		database.schema = model.schema;
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

ROUTE('GET       /collections/                           *Collections --> @query');
ROUTE('POST      /collections/                           *Collections --> @insert');
ROUTE('POST      /collections/{id}/                      *Collections --> @update');
ROUTE('DELETE    /collections/{id}/                      *Collections --> @remove');

ROUTE('POST      /collections/{id}/databases/            *Collections/Databases --> @insert');
ROUTE('POST      /collections/{id}/databases/{name}/     *Collections/Databases --> @update');
ROUTE('DELETE    /collections/{id}/databases/{name}/     *Collections/Databases --> @remove');

ROUTE('POST /collections/{id}/databases/{name}/query/', function() {
	var self = this;

	var col = COLLECTIONS[self.id];
	if (!col) {
		self.invalid('error-collections-404');
		return;
	}

	var db = col.databases[self.params.name];
	if (!db) {
		self.invalid('error-databases-404');
		return;
	}

	// self.body.command = '';
	// self.body.builder = {};
	var instance = db.instance;

	switch (self.body.command) {
		case 'find':
			instance.cmd_find(self.body.builder, self.callback());
			break;
		case 'find2':
			instance.cmd_find2(self.body.builder, self.callback());
			break;
		case 'insert':
			instance.cmd_insert(self.body.builder, self.callback());
			break;
		case 'update':
			instance.cmd_update(self.body.builder, self.callback());
			break;
		case 'remove':
			instance.cmd_remove(self.body.builder, self.callback());
			break;
		case 'clear':
			instance.cmd_clear(self.done(true));
			break;
		case 'clean':
			instance.cmd_clean(self.done(true));
			break;
		case 'alter':
			instance.cmd_alter(self.body.schema, self.done(true));
			break;
		case 'stats':
			self.json(instance.stats);
			break;
		default:
			self.invalid('error-command');
			break;
	}
});

function reloadcollections() {

	var collections = PREF.collections || EMPTYOBJECT;
	var keys = Object.keys(collections);

	for (var i = 0; i < keys.length; i++) {
		var key = keys[i];
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
			instance.replication = col.replication;
			instance.databases = {};
			COLLECTIONS[key] = instance;
		}

		var stamp = GUID(10);

		for (var i = 0; i < col.databases.length; i++) {
			var item = col.databases[i];
			var db = instance.databases[item.name];
			if (db) {
				db.stamp = stamp;
				if (item.type === 'table' && db.schema !== item.schema)
					db.instance.cmd_alter(db.schema);
			} else {
				var dir = Path.join(DIRECTORY, col.name);
				PATH.mkdir(dir);
				db = instance.databases[item.name] = {};
				db.stamp = stamp;
				db.dir = dir;
				db.instance = Main.init(item.type, item.name, dir);
			}
		}

		keys = Object.keys(instance.databases);
		for (var i = 0; i < keys.length; i++) {
			var key = keys[i];
			var db = instance.databases[key];
			if (db.stamp !== stamp) {
				Main.kill(db.instance);
				delete instance.databases[key];
			}
		}
	}
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