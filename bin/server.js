#!/usr/bin/env node
'use strict';

var express = require('express');
var path = require('path');
var bodyParser = require('body-parser');
var GeoBerlin = require('./lib/geoberlin');

var config = require('./config');

var geoberlin = new GeoBerlin(config);

var app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: false}));

if (config.demo) {
	var demopath = path.resolve(config.demo);
	app.use(express.static(demopath));
	app.get('/', function (req, res) {
		res.sendFile(path.join(__dirname, path.join(demopath, 'index.html')));
	});
}

var processrequest = function (req, res, action) {
	action(req.query, function (err, result) {
		if (err) {
			console.log(err.message || err.toString());
			return res.sendStatus(504);
		}
		res.json(result);
	});
};

app.get(config.apipath + 'autocomplete', function (req, res) {
	processrequest(req, res, geoberlin.autocomplete);
});
app.get(config.apipath + 'search', function (req, res) {
	processrequest(req, res, geoberlin.autocomplete);
});
app.get(config.apipath + 'near', function (req, res) {
	processrequest(req, res, geoberlin.findnear);
});
app.get(config.apipath + 'get', function (req, res) {
	processrequest(req, res, geoberlin.get);
});

geoberlin.init(function (error) {
	if (error) {
		console.trace(error);
	} else {
		app.listen(config.listen.port, config.listen.host, function () {
			console.log('running on http://' + config.listen.host + ':' + config.listen.port);
		});
	}
});
