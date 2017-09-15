'use strict';

var app = require('./webserver')();
var request = require('request');

// 1. List datasets
app.get('/datasets', function(req, res) {
  if (req.headers['x-secret'] !== process.env.CUMULIO_SECRET)
    return res.status(403).end('Given plugin secret does not match Cumul.io plugin secret.');

  request.get({
    uri: 'https://api.citybik.es/v2/networks',
    json: true
  }, function(error, networks) {
    if (error)
      return res.status(500).end('Internal Server Error');
    var datasets = networks.body.networks.map(function(network) {
      return {
        id: network.id,
        name: {en: `${network.name} ${network.location.city}`},
        description: {en: `Real-time availability of bike sharing network ${network.name} in ${network.location.city}`},
        columns: [
          {id: 'station_name', name: {en: 'Station name'}, type: 'hierarchy'},
          {id: 'last_update', name: {en: 'Last update'}, type: 'datetime'},
          {id: 'latitude', name: {en: 'Latitude'}, type: 'numeric'},
          {id: 'longitude', name: {en: 'Longitude'}, type: 'numeric'},
          {id: 'free_bikes', name: {en: 'Free bikes'}, type: 'numeric'},
          {id: 'empty_slots', name: {en: 'Empty slots'}, type: 'numeric'}
        ]
      }
    });
    return res.status(200).json(datasets);
  });
});

// 2. Retrieve data slices
app.post('/query', function(req, res) {
  if (req.headers['x-secret'] !== process.env.CUMULIO_SECRET)
    return res.status(403).end('Given plugin secret does not match Cumul.io plugin secret.');

  request.get({
    uri: `https://api.citybik.es/v2/networks/${req.body.id}`,
    json: true
  }, function(error, stations) {
    if (error)
      return res.status(500).end('Internal Server Error');
    var stations = stations.body.network.stations.map(function(station) {
      return [station.name, station.timestamp, station.latitude, station.longitude, station.free_bikes, station.empty_slots];
    });
    return res.status(200).json(stations);
  });
});