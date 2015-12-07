NodeJs server: Geoberlin Berlin Geocoder

uses the data scraped by https://github.com/yetzt/berlin-geodata-scraper and builds a Street GeoCoding Service based on ElasticSearch 

run 'npm install' in 'bin'
copy 'bin/config.js.dist' to 'bin/config.js' and fill in your settings
 
run 'bower install' in web/assets
copy 'web/assets/js/config.js.dist' to 'web/assets/js/config.js' and fill in your settings

run 'node import.js' in bin for importing geodata into elasticsearch
run 'node server.js' in bin to serve
