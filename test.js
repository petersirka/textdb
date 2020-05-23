// const DB = require('./textdb');
const Main = require('./index');

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

var instance = Main.init('nosql', 'skuska', '', function() {
	console.log('OK');
	// instance.cmd_insert({ payload: { id: UID(), name: GUID(30), price: U.random(100, 50), date: new Date() }}, console.log);
	// instance.cmd_alter('id:string,name:string,price:number,date:Date');
	// instance.cmd_find2({ filter: 'doc', fields: 'id,price' }, console.log);
	// instance.cmd_find2({ filter: 'doc', fields: 'id,price' }, console.log);
	// instance.cmd_find2({ filter: 'doc', fields: 'id,price' }, console.log);
});