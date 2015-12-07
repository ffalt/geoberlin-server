var fs = require('fs');
var path = require('path');
var async = require('async');
var zlib = require('zlib');
var status = require('node-status');

var GeoBerlin = require('./lib/geoberlin');

var config = require('./config');
var datafile = path.resolve('./data/data.tsv.gz');
var geoberlin = new GeoBerlin(config);

var showstatus = true;
var tsv_head = ['strnr', 'hausnr', 'name', 'nummer', 'addresse', 'plz', 'bezirk_name', 'bezirk_nr', 'ortsteil_name', 'ortsteil_nr', 'strasse_nr',
	'strasse_abschnitt_nr', 'karten', 'soldner_x', 'soldner_y', 'etrs89_x', 'etrs89_y', 'stat_gebiet', 'stat_block', 'einschulungsbezirk', 'verkehrsflaeche',
	'verkehrsteilflaeche', 'mittelbereich', 'prognoseraum_name', 'prognoseraum_nr', 'bezirksregion_name', 'bezirksregion_nr', 'planungsraum_name',
	'planungsraum_nr', 'finanzamt_nr', 'finanzamt_addr', 'lon', 'lat', 'url'];


var parse = function (cb) {
	console.log('[loading file]', datafile);
	var buffer = fs.readFileSync(datafile);
	console.log('[unpacking]', datafile);
	zlib.unzip(buffer, function (err, buffer) {
		if (err) return cb(err);
		console.log('[import data]');
		var tsv = buffer.toString().split('\n');
		var total = status.addItem('total', {max: tsv.length, color: 'cyan'});
		var entries = status.addItem('entries', {type: ['bar', 'percentage'], max: tsv.length});
		var err_count = status.addItem('err', {color: 'red', label: 'errors'});
		if (showstatus) {
			status.start({invert: false});
		}

		var bulkInsert = function (list, cb) {
			geoberlin.store(list, function (err) {
				if (err) {
					err_count.inc(list.length);
					console.error(err);
				} else {
					entries.inc(list.length);
				}
				cb();
			});
		};

		var bulk = [];
		async.forEachSeries(tsv, function (row, then) {
			total.inc();
			var cols = row.split('\t');
			var o = {};
			tsv_head.forEach(function (col, i) {
				if (col === 'lat') {
					o.location = o.location || {};
					o.location.lat = parseFloat(cols[i]);
				} else if (col === 'lon') {
					o.location = o.location || {};
					o.location.lon = parseFloat(cols[i]);
				} else {
					o[col] = cols[i];
				}
			});
			if (isNaN(o.location.lat) || isNaN(o.location.lon)) {
				o.location = null;
			}
			bulk.push(o);
			if (bulk.length > 999) {
				bulkInsert(bulk, function () {
					bulk = [];
					then();
				});
			} else {
				then();
			}
		}, function () {
			if (bulk.length > 0) {
				bulkInsert(bulk, function () {
					cb();
				});
			} else {
				cb();
			}
		});
	});
};

geoberlin.init(function (err) {
	if (err) return console.trace(err);
	geoberlin.reset(function (err) {
		if (err) return console.trace(err);
		parse(function (err) {
			if (err) return console.trace(err);
			geoberlin.close(function (err) {
				if (err) return console.trace(err);
				console.log('[done]');
				process.exit();
			});
		});
	});
});

