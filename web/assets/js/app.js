var app = angular.module('App', []);

app.controller('AppController', function ($scope) {

	if (!settings) {
		return console.log('You must create a config.js file!');
	}

	var map = L.map('map', {maxZoom: 18, minZoom: 10}).setView([52.5, 13.4], 12);
	L.tileLayer(
		settings.map.url, {
			attribution: settings.map.attribution
		}).addTo(map);

	var geocoder = new BerlinGeoCoder({
		apikey: settings.api.key,
		url: settings.api.url
	});

	L.control.geocoder({
		geocoder: geocoder,
		bounds: false,
		//bounds: L.latLngBounds(bounds),
		title: 'Suche',
		placeholder: 'Suche nach Straße & Hausnr.',
		position: 'topright',
		locate: true,
		expanded: true,
		fullWidth: false,
		pointIcon: 'assets/bower_components/geoberlin-client/images/point_icon.png',
		polygonIcon: 'assets/bower_components/geoberlin-client/images/polygon_icon.png',
		onResults: function (features, header) {
			$scope.$apply(function () {
				$scope.features = features;
				$scope.header = header;
			});
			return false;
		},
		onMarkResult: function (feature) {
			//console.log(feature);
			return false;
		},
		onSelectResult: function (feature) {
			if (feature.properties.id && feature.properties.housenr) {
				geocoder.get(feature.properties, function (err, results) {
					$scope.$apply(function () {
						$scope.features = results.features;
					});
				});
			}
			return false;
		}
	}).addTo(map);

});
