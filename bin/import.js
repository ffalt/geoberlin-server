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
	var ids = {};
	zlib.unzip(buffer, function (err, buffer) {
		if (err) return cb(err);
		console.log('[import data]');
		var tsv = buffer.toString().split('\n');
		var status_total = status.addItem('total', {type: ['bar', 'percentage'], max: tsv.length});
		var status_entries = status.addItem('entries', {color: 'cyan', label: 'rejected'});
		var status_dups = status.addItem('dups', {color: 'yellow', label: 'rejected'});
		var status_rejected = status.addItem('rejected', {color: 'yellow', label: 'rejected'});
		var status_errors = status.addItem('errors', {color: 'red', label: 'errors'});
		if (showstatus) {
			status.start({invert: false});
		}

		var bulkInsert = function (list, cb) {
			geoberlin.store(list, function (err) {
				if (err) {
					status_errors.inc();
					status_rejected.inc(list.length);
					console.error(err);
				} else {
					status_entries.inc(list.length);
				}
				cb();
			});
		};

		var bulk = [];
		async.forEachSeries(tsv, function (row, then) {
			status_total.inc();
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
				status_rejected.inc();
				return then();
				//o.location = null;
			}

			if (!o.name) {
				status_rejected.inc();
				return then();
			}

			if (ids[o.strnr + '-' + o.hausnr]) {
				status_dups.inc();
				return then();
			} else ids[o.strnr + '-' + o.hausnr] = true;

			o.slug = geoberlin.slugify(o.name);

			if (isNaN(o.hausnr)) {
				var n = o.hausnr.match(/\d*/)[0];
				o.hausnr_nr = parseInt(n);
				o.hausnr_suffix = o.hausnr.slice(n.length);
			} else {
				o.hausnr_nr = parseInt(o.hausnr);
			}

			bulk.push(o);
			if (bulk.length > 1999) {
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

