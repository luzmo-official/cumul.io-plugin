'use strict';

var async = require('async');
var { MongoClient, ObjectID } = require('mongodb');
var schema = require('mongodb-schema');
var app = require('./webserver')();

// Set local caching
var cache = {cached_at: null, cache: null, interval: 15 * 60 * 1000};

// 1. List datasets
app.get('/datasets', function(req, res) {
  if (req.headers['x-secret'] !== process.env.CUMULIO_SECRET)
    return res.status(403).end('Given plugin secret does not match Cumul.io plugin secret.');
  getSchema(req, function(error, schema) {
    if (error)
      return res.status(500).end('MongoDB internal server error.');
    return res.status(200).json(schema);
  });
});

// 2. Retrieve data slices
app.post('/query', function(req, res) {
  if (req.headers['x-secret'] !== process.env.CUMULIO_SECRET)
    return res.status(403).end('Given plugin secret does not match Cumul.io plugin secret.');
  
  MongoClient.connect(`mongodb://${req.headers['x-key']}:${req.headers['x-token']}@${process.env.MONGO_URI}`, function(error, db) {
    if (error)
      return res.status(500).end('MongoDB database could not be reached.');
    var table = req.body.id.split('.');
    if (table.length !== 2)
      return res.status(400).end('Requested MongoDB dataset has invalid format (expected: database.collection)');

    getSchema(req, function(error, schema) {
      if (error)
        return res.status(500).end('MongoDB internal server error');

      var data = db.collection(table[1])
        .find(toMongoFilters(req.body.filters))
        .toArray(function(error, data) {
          if (error)
            return res.status(500).end('MongoDB internal server error.');

          // Flatten the data & convert to a sparse array
          var metadata = schema.find((a) => a.id === req.body.id).columns;
          data = data.map(flatten).map((row) => metadata.map((column) => row[column.id]))
          return res.status(200).json(data);

        });
    });
  });
});

// Retrieve MongoDB schema
function getSchema(req, callback) {
  if (cache.cached_at > (new Date()).getTime() - cache.interval)
    return callback(null, cache.cache);

  MongoClient.connect(`mongodb://${req.headers['x-key']}:${req.headers['x-token']}@${process.env.MONGO_URI}`, function(error, db) {
    if (error)
      return callback(error);

    db.collections(function(error, collections) {
      if (error)
        return callback(error);
      async.map(collections, function(collection, next) {
        inferSchema(collection.find().limit(200), function(error, schema) {
          if (error)
            return next(error);
          return next(null, {
            id: collection.namespace,
            name: {en: collection.collectionName},
            description: {en: `All documents within the collection ${collection.collectionName} (${collection.namespace})`},
            columns: schema
          });
        });
      }, function(error, datasets) {
        if (error)
          return callback(error);
        cache.cached_at = (new Date()).getTime();
        cache.cache = datasets;
        return callback(null, cache.cache);
      });
    });
  });
}

// Infer the schema of a MongoDB collection from the first N rows
function inferSchema(data, callback) {
  var columns = [];
  schema(data, {storeValues: false}, function(error, schema) {
    if (error)
      return callback(error);
    function addField(field) {
      if (Array.isArray(field.type) && field.type.length > 0)
        field.type = field.type[0];
      if (field.type === 'Document')
        return field.types[0].fields.forEach(addField);
      else if (field.bsonType === 'Document')
        return field.fields.forEach(addField);
      else if (field.type === 'Array')
        return field.types[0].types.forEach(addField);
      else
        columns.push({
          id: field.path,
          name: {en: field.path},
          type: toCumulioType(field.type)
        });
    }
    schema.fields.forEach(addField);
    return callback(null, columns);
  });
}

// Flatten a MongoDB resultset
function flatten(row) {
  var obj = {};
  function walk(key, value) {
    while (Array.isArray(value) && value.length > 0)
      value = value[0];
    if (Array.isArray(value))
      value = null;
    if (!(value instanceof ObjectID) && !(value instanceof Date) && !(value === null) && typeof value === 'object')
      Object.keys(value).forEach((subkey) => walk(key + '.' + subkey, value[subkey]));
    else
      obj[key] = value;
  }
  Object.keys(row).forEach((key) => walk(key, row[key]));
  return obj;
}

// MongoDB -> Cumul.io type conversion
function toCumulioType(mongoType) {
  switch(mongoType) {
    case 'Number': return 'numeric';
    case 'Date': return 'datetime';
    case 'Timestamp': return 'datetime';
    default: return 'hierarchy';
  }
}

// Cumul.io -> MongoDB filters
function toMongoFilters(filters) {
  var conditional = {};
  filters.forEach((filter) => {
    if (!conditional[filter.column_id])
      conditional[filter.column_id] = {};
    switch(filter.expression) {
      case 'in':          conditional[filter.column_id]['$in'] = filter.value; break;
      case 'not in':      conditional[filter.column_id]['$nin'] = filter.value; break;
      case '<':           conditional[filter.column_id]['$lt'] = filter.value[0]; break;
      case '<=':          conditional[filter.column_id]['$lte'] = filter.value[0]; break;
      case '>':           conditional[filter.column_id]['$gt'] = filter.value[0]; break;
      case '>=':          conditional[filter.column_id]['$gte'] = filter.value[0]; break;
      case '=':           conditional[filter.column_id] = filter.value[0]; break;
      case 'is null':     conditional[filter.column_id] = null; break;
      case 'is not null': conditional[filter.column_id]['$ne'] = filter.value[0]; break;
    }
  });
  return conditional;
}