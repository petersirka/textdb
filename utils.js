// Dependencies
const COMPARER = global.Intl.Collator().compare;

exports.sort = function(builder, item) {
	var length = builder.items.length;
	if (length < builder.$take) {
		length = builder.items.push(item);

		var type = builder.$sorttype;
		if (!type) {
			if (item[builder.$sortname] instanceof Date) {
				type = builder.$sorttype = 4;
			} else {
				switch (typeof(item[builder.$sortname])) {
					case 'number':
						type = 2;
						break;
					case 'boolean':
						type = 3;
						break;
					case 'string':
						type = 2;
						break;
					default:
						return true;
				}
				builder.$sorttype = type;
			}
		}
		if (length >= builder.$take)
			builder.items.sort((a, b) => sortcompare(type, builder, a, b));
		return true;
	} else
		return chunkysort(builder, item);
};

function sortcompare(type, builder, a, b) {
	var va = a[builder.$sortname];
	var vb = b[builder.$sortname];
	var vr = 0;

	switch (type) {
		case 1: // string
			vr = va && vb ? COMPARER(va, vb) : va && !vb ? -1 : 1;
			break;
		case 2: // number
			vr = va != null && vb != null ? (va < vb ? -1 : 1) : va != null && vb == null ? -1 : va === vb ? 0 : 1;
			break;
		case 3: // boolean
			vr = va === true && vb === false ? -1 : va === false && vb === true ? 1 : 0;
			break;
		case 4: // Date
			vr = va != null && vb != null ? (va < vb ? -1 : 1) : va != null && vb == null ? -1 : 1;
			break;
	}
	return builder.$sortasc ? vr : (vr * -1);
}

function chunkysort(builder, item) {

	var beg = 0;
	var length = builder.items.length;
	var tmp = length - 1;
	var type = builder.$sorttype;

	var sort = sortcompare(type, builder, item, builder.items[tmp]);
	if (sort !== -1)
		return;

	tmp = builder.items.length / 2 >> 0;
	sort = sortcompare(type, builder, item, builder.items[tmp]);

	if (sort !== -1)
		beg = tmp + 1;

	for (var i = beg; i < length; i++) {
		var old = builder.items[i];
		var sort = sortcompare(type, builder, item, old);
		if (sort === -1) {
			for (var j = length - 1; j > i; j--)
				builder.items[j] = builder.items[j - 1];
			builder.items[i] = item;
		}
		return true;
	}
}