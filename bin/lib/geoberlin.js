var async = require('async');
var path = require('path');
var debug = require('debug')('geoberlin:coder');
var elasticsearch = require('elasticsearch');

function GeoBerlin(config) {
	var me = this;
	var client = new elasticsearch.Client(config.elasticsearch.config);

	var searchStreet = function (query, cb) {
		var queries = [];
		if (query.strasse) {
			queries.push({
				//prefix: {name: query.text} //https://www.elastic.co/guide/en/elasticsearch/guide/current/prefix-query.html
				//fuzzy: {name: query.text} // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-fuzzy-query.html
				//term: {name: query.text} // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html
				//wildcard: {name: query.text + '*'}
				match_phrase_prefix: {name: query.strasse} //https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html
				//match: {name: query.text} //https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html
			});
		}
		if (query.slug) {
			queries.push({
				match_phrase_prefix: {slug: query.slug}
			});
		}
		if (query.hausnr) {
			queries.push({
				match_phrase_prefix: {hausnr: query.hausnr}
			});
		}
		if (query.plz) {
			queries.push({
				match_phrase_prefix: {plz: query.plz}
			});
		}
		if (query.bezirk) {
			queries.push({
				match_phrase_prefix: {bezirk_name: query.bezirk}
			});
		}
		var q = queries.length === 1 ? queries[0] : {bool: {must: queries}};

		client.search({
			index: config.elasticsearch.index,
			type: 'address',
			fields: ['name', 'plz'],
			size: 100,
			body: {
				query: q,
				aggs: {
					names: {
						terms: {
							field: 'strnr',
							size: 100
						},
						'aggs': {
							top_names_hits: {
								top_hits: {
									'_source': {'include': ['strnr', 'slug', 'name', 'bezirk_name', 'location']},
									'size': 1
								}
							}
						}
					}
				}
			}
		}).then(function (resp) {
			var result = resp.aggregations.names.buckets.map(function (b) {
				return b.top_names_hits.hits.hits.map(function (h) {
					var feature = {
						'type': 'Feature',
						'properties': {
							'id': h._source.strnr,
							'layer': 'street',
							'slug': h._source.slug,
							'name': h._source.name,
							'country_a': 'DEU',
							'country': 'Deutschland',
							'region': h._source.bezirk_name,
							'label': h._source.name + ' , ' + h._source.bezirk_name
						},
						'geometry': {
							'type': 'Point',
							'coordinates': [h._source.location.lon, h._source.location.lat]
						}
					};
					return feature;
				})[0];
				//debug('[search street]', query, result.length);
			});
			result.sort(function (a, b) {
				if (a.properties.name < b.properties.name) return -1;
				if (a.properties.name > b.properties.name) return 1;
				return 0;
			});
			cb(null, result);
		}, function (err) {
			cb(err);
		});
	};

	var listStreet = function (query, cb) {
		var queries = [];
		if (query.strnr) {
			queries.push({
				match: {strnr: query.strnr} //https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html
			});
		}
		if (query.hausnr) {
			queries.push({
				match_phrase_prefix: {
					hausnr: {query: query.hausnr, 'max_expansions': 5000}
				}
			});
		}
		var q = queries.length === 1 ? queries[0] : {bool: {must: queries}};
		client.search({
			index: config.elasticsearch.index,
			type: 'address',
			size: 50,
			_source: ['name', 'plz', 'hausnr', 'strnr', 'location.*', 'bezirk_name'],
			body: {
				sort: ['hausnr_nr', 'hausnr_suffix'],
				query: q
			}
		}).then(function (resp) {
			var result = resp.hits.hits.map(function (h) {
				var feature = {
					'type': 'Feature',
					'properties': {
						'id': h._source.strnr,
						'layer': 'address',
						'name': h._source.name,
						'country_a': 'DEU',
						'country': 'Deutschland',
						'region': h._source.bezirk_name,
						'label': h._source.name + ' ' + h._source.hausnr + ', ' + h._source.plz + ' ' + h._source.bezirk_name,
						'housenr': h._source.hausnr,
						'postcode': h._source.plz
					},
					'geometry': {
						'type': 'Point',
						'coordinates': [h._source.location.lon, h._source.location.lat]
					}
				};
				return feature;
			});
			cb(null, result);
		}, function (err) {
			console.trace(err);
			cb(err);
		});
	};

	var analyzeQuery = function (text, cb) {

		if (!me.special_streets) {
			return me.findSpecialStreets(function (err, streets) {
				if (err) return cb(err);
				me.special_streets = streets.map(function (s) {
					return s.toLowerCase();
				});
				analyzeQuery(text, cb);
			});
		}

		var parts = text.split(',');
		var front = parts[0];
		var back;
		if (parts.length > 1) back = parts.slice(1).join(' ');
		var result = {};

		var p = front.split(/[ ;]+/).filter(function (s) {
			return s.length > 0;
		});
		var last = '';

		var isSpecialStreetWithNr = function (text_with_nr) {
			for (var i = 0; i < me.special_streets.length; i++) {
				if (me.special_streets[i].indexOf(text_with_nr) === 0) return true;
			}
			return false;
		};

		p.forEach(function (part, i) {
			if (isNaN(part)) {
				if (last === 'hausnr' && part.length === 1) {
					//space zwischen einer hausnr und dem buchstaben einer hausnummer z.b. '13 a'
					result.hausnr = result.hausnr + part;
				} else {
					if (last === '' || last === 'strasse') {
						if (part.match(/^[1-9]{1}\d{0,4}[a-zA-Z]{1}$/)) {
							//ne hausnummer mit einem buchstaben z.b. '13a'
							result.hausnr = (result.hausnr || '') + part;
							last = 'hausnr';
						} else {
							//noch keine straße oder es kam noch nichts anderes z.B. 'Am Weinberg'
							result.strasse = ((result.strasse || '') + ' ' + part).trim();
							last = 'strasse';
						}
					} else {
						//text hinter einer straße mit hausnummer oder hinter einem komma oder hinter einer postleitzahl
						result.bezirk = ((result.bezirk || '') + ' ' + part).trim();
						last = 'bezirk';
					}
				}
			} else {
				//ist ne zahl
				if (part.length === 5) {
					//hey, ne postleitzahl, nachfolgende postleitzahlen werden ignoriert
					result.plz = result.plz || part;
					last = 'plz';
				} else {
					//gehört die zahl zum straßenname ?
					if (result.hausnr === undefined && (i === 1) && isSpecialStreetWithNr((p[i - 1] + ' ' + part).toLowerCase())) {
						result.strasse = ((result.strasse || '') + ' ' + part).trim();
						last = 'strasse';
					} else {
						//hausnummer, nachfolgende zahlen werden ignoriert
						result.hausnr = result.hausnr || part;
						last = 'hausnr';
					}
				}
			}
		});

		if (back) {
			var f = back.split(/[ ;]+/).filter(function (s) {
				return s.length > 0;
			});
			f.forEach(function (part) {
				if (isNaN(part)) {
					//text hinter einem komma wird nur al bezirk gewertet
					result.bezirk = ((result.bezirk || '') + ' ' + part).trim();
				} else if (part.length === 5) {
					//hey, ne postleitzahl, nachfolgende postleitzahlen werden ignoriert
					result.plz = result.plz || part;
				}
			});
		}

		cb(null, result);
	};

	var packageResults = function (query, list, parsed) {
		var result = {
			'geocoding': {
				'version': '0.1',
				'parsed': parsed,
				'engine': {'name': 'GeoBerlin', 'author': 'DSST', 'version': '1.0'},
				'timestamp': (new Date()).valueOf()
			},
			'type': 'FeatureCollection', 'features': list
		};
		return result;
	};

	me.slugify = function (streetname) {
		return streetname.replace(/[-\. ]/g, '').replace(/ß/g, 'ss').toLowerCase();
	};

	me.get = function (query, cb) {
		client.search({
			index: config.elasticsearch.index,
			type: 'address',
			body: {
				query: {
					bool: {
						must: [
							{match: {strnr: query.id}},
							{match: {hausnr: query.housenr}}
						]
					}
				}
			}
		}).then(function (resp) {
			var result = resp.hits.hits.map(function (h) {
				var feature = {
					'type': 'Feature',
					'properties': {
						'id': h._source.strnr,
						'layer': 'address',
						'name': h._source.name,
						'country_a': 'DEU',
						'country': 'Deutschland',
						'region': h._source.bezirk_name,
						'label': h._source.name + ' ' + h._source.hausnr + ', ' + h._source.plz + ' ' + h._source.bezirk_name,
						'housenr': h._source.hausnr,
						'postcode': h._source.plz,
						'details': h._source
					},
					'geometry': {
						'type': 'Point',
						'coordinates': [h._source.location.lon, h._source.location.lat]
					}
				};
				return feature;
			});
			cb(null, packageResults(query, result));
		}, function (err) {
			console.trace(err);
			cb(err);
		});
	};

	me.findSpecialStreets = function (cb) {
		client.search({
			index: config.elasticsearch.index,
			type: 'address',
			_source: ['name', 'hausnr'],
			body: {
				query: {
					regexp: {
						name: '.*[0-9].*'
					}
				},
				aggs: {
					names: {
						terms: {
							field: 'name',
							size: 10000
						},
						'aggs': {
							top_names_hits: {
								top_hits: {
									'_source': {'include': ['strnr', 'name']},
									'size': 1
								}
							}
						}
					}
				}
			}
		}).then(function (resp) {
			var result = resp.aggregations.names.buckets.map(function (b) {
				return b.top_names_hits.hits.hits.map(function (h) {
					return h._source.name;
				})[0];
			});
			cb(null, result);
		}, function (err) {
			cb(err);
		});
	};

	me.findNear = function (query, cb) {
		function estimateDistanceInMeter(lat1, lon1, lat2, lon2) {
			var p = 0.017453292519943295;    // Math.PI / 180
			var c = Math.cos;
			var a = 0.5 - c((lat2 - lat1) * p) / 2 +
				c(lat1 * p) * c(lat2 * p) *
				(1 - c((lon2 - lon1) * p)) / 2;
			return Math.round(12742 * Math.asin(Math.sqrt(a)) * 1000); // 2 * R; R = 6371 km
		}

		var lat = parseFloat(query.lat);
		var lon = parseFloat(query.lon);
		client.search({
			index: config.elasticsearch.index,
			type: 'address',
			size: 50,
			_source: ['name', 'plz', 'hausnr', 'strnr', 'location.*', 'bezirk_name'],
			body: {
				query: {
					'filtered': {
						'query': {
							'match_all': {}
						},
						'filter': {
							'geo_distance': {
								'distance': (query.acc || 25) + 'm',
								'distance_type': 'plane',
								'location': {
									'lat': lat,
									'lon': lon
								}
							}
						}
					}
				}
			}
		}).then(function (resp) {
			var result = resp.hits.hits.map(function (h) {
				var feature = {
					'type': 'Feature',
					'properties': {
						'id': h._source.strnr,
						'layer': 'address',
						'name': h._source.name,
						'country_a': 'DEU',
						'country': 'Deutschland',
						'region': h._source.bezirk_name,
						'label': h._source.name + ' ' + h._source.hausnr + ', ' + h._source.plz + ' ' + h._source.bezirk_name,
						'housenr': h._source.hausnr,
						'postcode': h._source.plz,
						'distance': estimateDistanceInMeter(lat, lon, parseFloat(h._source.location.lat), parseFloat(h._source.location.lon))
					},
					'geometry': {
						'type': 'Point',
						'coordinates': [h._source.location.lon, h._source.location.lat]
					}
				};
				return feature;
			});
			result.sort(function (a, b) {
				return a.properties.distance - b.properties.distance;
			});
			cb(null, packageResults(query, result));
		}, function (err) {
			console.trace(err);
			cb(err);
		});
	};

	me.autocomplete = function (query, cb) {
		analyzeQuery(query.text, function (err, parts) {
			if (err) return cb(err);

			if (!parts.strasse) return cb(null, []);

			parts.slug = me.slugify(parts.strasse);

			var searches = [
				['slug', 'hausnr', 'plz', 'bezirk'],
				['slug', 'hausnr', 'bezirk'],
				['slug', 'hausnr', 'plz'],
				['slug', 'hausnr'],
				['slug', 'plz'],
				['slug']
			];

			//filter out not available searches
			searches = searches.filter(function (search) {
				for (var i = 0; i < search.length; i++) {
					if (!parts[search[i]]) return false;
				}
				return true;
			});

			//process searches, exit if one matches
			async.forEachSeries(searches, function (search, then) {
				//copy parameters of this search
				var q = {};
				search.forEach(function (s) {
					q[s] = parts[s];
				});
				//let's go! search!
				searchStreet(q, function (err, results) {
					if (err) return cb(err);
					if (results.length === 0) {
						//no results, try next
						return then();
					}
					if (results.length === 1) {
						//one street found, let's get housenumbers
						parts.strnr = results[0].properties.id;
						return listStreet(parts, function (err, results) {
							if (err) return cb(err);
							cb(null, packageResults(query, results, parts));
						});
					}
					if ((results[0].properties.slug === parts.slug) && parts.hausnr) {
						//we have a winner (there may be other streets starting with the same slug, but we are already searching the house nr
						parts.strnr = results[0].properties.id;
						return listStreet(parts, function (err, results) {
							if (err) return cb(err);
							cb(null, packageResults(query, results, parts));
						});
					}
					cb(null, packageResults(query, results, parts));
				});
			}, function () {
				//'all searches failed, nothing found'
				cb(null, []);
			});

		});
	};

	me.store = function (address, cb) {

		if (Array.isArray(address)) {
			var ops = [];
			address.forEach(function (item, idx) {
				ops.push({create: {_index: config.elasticsearch.index, _type: 'address'}});
				ops.push(item);
			});

			client.bulk({
				body: ops,
				ignore: [409]
			}).then(function (resp) {
				cb();
			}, function (err) {
				cb(err);
			});

		} else {
			client.index({
				type: 'address',
				index: config.elasticsearch.index,
				body: address
			}, cb);
		}
	};

	var registerMapping = function (cb) {
		var mapping = {
			address: {
				properties: {
					name: {
						'analyzer': 'analyzer_startswith',
						'type': 'string'
					},
					slug: {
						'analyzer': 'analyzer_startswith',
						'type': 'string'
					},
					hausnr: {
						'analyzer': 'analyzer_startswith',
						'type': 'string'
					},
					bezirk_name: {
						'analyzer': 'analyzer_startswith',
						'type': 'string'
					},
					location: {
						'type': 'geo_point'
					}
				}
			}
		};

		var settings = {
			'analysis': {
				'analyzer': {
					'analyzer_startswith': {
						'type': 'custom',
						'tokenizer': 'keyword',
						'filter': [
							'lowercase',
							'trim'
						]
					}
				}
			}
		};

		client.indices.refresh({index: config.elasticsearch.index}, function (err, res) {
			console.log('[refresh index]', '\t', config.elasticsearch.index, err || '\t', res);
			client.indices.close({index: config.elasticsearch.index}, function (err, res) {
				console.log('[close index]', '\t', config.elasticsearch.index, err || '\t', res);
				client.indices.putSettings({index: config.elasticsearch.index, body: settings}, function (err, res) {
					console.log('[put settings]', '\t', config.elasticsearch.index, err || '\t', res);
					client.indices.putMapping({index: config.elasticsearch.index, type: 'address', body: mapping}, function (err, res) {
						console.log('[put mapping]', '\t', config.elasticsearch.index, err || '\t', res);
						client.indices.open({index: config.elasticsearch.index}, function (err, res) {
							console.log('[open index]', '\t', config.elasticsearch.index, err || '\t', res);
							cb();
						});
					});
				});
			});
		});
	};

	var dropDD = function (cb) {
		client.indices.delete({index: config.elasticsearch.index}, function (err, res) {
			console.log('[delete index]', '\t', config.elasticsearch.index, err || '\t', res);
			client.indices.create({index: config.elasticsearch.index}, function (err, res) {
				console.log('[create index]', '\t', config.elasticsearch.index, err || '\t', res);
				cb();
			});
		});
	};

	me.reset = function (cb) {
		dropDD(function () {
			registerMapping(function () {
				cb();
			});
		});
	};

	me.init = function (cb) {
		client.ping({
			requestTimeout: Infinity
		}, function (error) {
			if (error) {
				cb('elasticsearch cluster is down!');
			} else {
				cb();
			}
		});
	};

	me.close = function (cb) {
		cb();
	};
}

module.exports = GeoBerlin;
