// Dependencies
const DButils = require('./utils');

// Dependencies
function QueryBuilder() {

	var t = this;
	t.items = [];
	t.count = 0;
	t.counter = 0;
	t.scanned = 0;
	t.filterarg = EMPTYOBJECT;
	t.modifyarg = EMPTYOBJECT;
	t.$take = 1000;
	t.$skip = 0;

	// t.$fields
	// t.$sortname
	// t.$sortasc
}

QueryBuilder.prototype.fields = function(value) {
	var self = this;
	// @TODO: cache it
	self.$fields = value.split(',').trim();
	return self;
};

QueryBuilder.prototype.transform = function(doc) {

	var self = this;
	if (!self.$fields)
		return doc;

	var obj = {};

	// @TODO: add a custom transformation
	for (var i = 0; i < self.$fields.length; i++) {
		var name = self.$fields[i];
		obj[name] = doc[name];
	}

	return obj;
};

QueryBuilder.prototype.push = function(item) {
	var self = this;
	if (self.$sortname)
		return DButils.sort(self, item);
	self.items.push(item);
	return true;
};

QueryBuilder.prototype.take = function(take) {
	this.$take = take;
	return this;
};

QueryBuilder.prototype.skip = function(skip) {
	this.$skip = skip;
	return this;
};

QueryBuilder.prototype.sort = function(field, desc) {
	var self = this;
	self.$sortname = field;
	self.$sortasc = desc !== true;
	return self;
};

QueryBuilder.prototype.filter = function(rule, arg) {
	var self = this;
	if (arg)
		self.filterarg = arg;
	self.filterrule = new Function('item', 'arg', 'return ' + rule);
	return self;
};

function modifyrule(doc) {
	return doc;
}

QueryBuilder.prototype.modify = function(rule, arg) {
	var self = this;
	if (arg)
		self.modifyarg = arg;
	self.modifyrule = rule ? new Function('item', 'arg', rule) : modifyrule;
	return self;
};

QueryBuilder.prototype.scalar = function(rule, arg) {
	var self = this;
	if (arg)
		self.scalararg = arg;
	self.scalarrule = new Function('item', 'arg', rule);
	return self;
};

QueryBuilder.prototype.callback = function(fn) {
	var self = this;
	self.$callback = fn;
	return self;
};

exports.QueryBuilder = QueryBuilder;