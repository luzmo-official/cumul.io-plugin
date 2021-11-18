const moment = require('moment');
const stream = require('stream');
const Transform = stream.Transform;
const util = require('./../util/util.js');

class ResultsGenerator {

  static generateDatasets(indexesToColumns){
    const datasets = [];
    Object.keys(indexesToColumns).forEach((indexName) => {
      const columns = [];
      Object.keys(indexesToColumns[indexName]).forEach((columnName) => {
        columns.push({
          id: columnName.toLowerCase(),
          name: {
            en: columnName,
            nl: columnName,
            fr: columnName
          },
          type: indexesToColumns[indexName][columnName].type
        });
      });
      datasets.push({
        id: indexName.toLowerCase(),
        name: {
          en: indexName,
          nl: indexName,
          fr: indexName
        },
        columns: columns
      });
    });

    return datasets;
  }

  static transformStream ( query, incomingStream, index, columns, schema, pushdown) {
    let transformStream = null;
    if (pushdown){
      if (util.isEmpty(query.body.aggs) || util.isEmpty(query.body.aggs.all) || util.isEmpty(query.body.aggs.all.composite))
        transformStream = new StreamTransformResultsGeneratorPushdownNoGroups(index, columns, schema);
      else
        transformStream = new StreamTransformResultsGeneratorPushdown(index, columns, schema, null);
    }
    else
      transformStream = new StreamTransformResultsGeneratorNonPushdown(index, columns, schema);

    const copyStream = new CopyStream();
    incomingStream.pipe(transformStream)
      .pipe(copyStream, { end: false });

    copyStream.write('[');
    transformStream.on('end', () => {
      copyStream.end(']');
    });
    return copyStream;
  }

  static transformElasticEntityToRow(entity, dataset, columns, schema) {
    return columns.map((c) => {
      const columnInfo = util.getColumnInfo(dataset, c.column_id, schema);
      const colName = columnInfo.columnName;
      const colType = columnInfo.type;
      if (c.column_id !== '*' && colType === 'datetime'){
        if (util.isEmpty(this.fetchNested(colType, colName.split('.'), entity.fields)))
          return null;
        else
          return this.fetchNested(colType, colName.split('.'), entity.fields)[0];
      }
      else
        return this.fetchNested(colType, colName.split('.'), entity._source);
    });
  }

  static transformElasticPushdownBucketToRow(bucket, dataset, columns, schema) {
    const row = [];
    columns.forEach((col, index) => {
      const colQueryName = `col${index}`;
      if (col.column_id === '*' || col.aggregation === 'count')
        row.push(bucket.doc_count);
      else if (!util.isEmpty(bucket[colQueryName])){
        if (!util.isEmpty(bucket[colQueryName].buckets))
          row.push(bucket[colQueryName].buckets[0].key);
        else
          row.push(bucket[colQueryName].value);
      }
      else
        row.push(bucket.key[colQueryName]);
    });
    return row;
  }

  static transformElasticPushdownNoGroupsToRow(result, dataset, columns, schema) {
    const row = [];
    columns.forEach((col, index) => {
      if (col.column_id === '*' || col.aggregation === 'count')
        row.push(util.isEmpty(result.count.value) ? result.count : result.count.value);
      else {
        const colQueryName = `col${index}`;
        row.push(util.isEmpty(result[colQueryName].value) ? result[colQueryName] : result[colQueryName].value);
      }
    });
    return row;
  }

  static transformDates(row, dataset, columns, schema){
    columns.forEach((col, index) => {
      if (col.column_id !== '*' && schema.indexesToColumns[dataset][col.column_id].type === 'datetime'){
        if (isNaN(row[index]))
          row[index] = moment.utc(row[index]).utc().toISOString();
        else
          row[index] = moment.unix(row[index] / 1000).utc().toISOString();
      }
    });
  }

  static fetchNested(type, pointSeparatedColName, object){
    if ( object == null ) return null;
    if ( type === 'datetime' ) {
      if (util.isEmpty(object[pointSeparatedColName.join('.')])) return null;
      return object[pointSeparatedColName.join('.')];
    }
    while (pointSeparatedColName.length > 0) {
      if (Array.isArray(object)){
        // unwrapping arrays only works for strings, else presto will receive an array string instead of a numeric
        if (type === 'hierarchy'){
          object = object.map((obj) => {
            if (util.isEmpty(obj))
              return null;
            else
              return obj[pointSeparatedColName[0]];
          });
        }
        else
          return null;
      }
      else if (util.isEmpty(object))
        return null;
      else {
        if (type === 'numeric' && pointSeparatedColName.length === 1 && Array.isArray(object[pointSeparatedColName[0]])) // Array of floats -> not sure what to do with this, so throw away for now.
          return null;
        object = object[pointSeparatedColName[0]];
        if (!util.isEmpty(object) && object.length === 1) // Array with one value -> return the value
          object = object[0];
        if (!util.isEmpty(object) && object.length === 0) // Empty array -> return null
          object = null;
      }
      pointSeparatedColName = pointSeparatedColName.slice(1);
    }
    return object;
  }

}

class StreamTransformResultsGeneratorPushdownNoGroups extends Transform {

  constructor(dataset, columns, schema, options){
    super({ objectMode: true });
    this.first = true;
    this.columns = columns;
    this.schema = schema;
    this.dataset = dataset;
  }

  _transform(bucket, encoding, callback){
    if (!this.first)
      this.push(',');

    this.first = false;
    const row = ResultsGenerator.transformElasticPushdownNoGroupsToRow(bucket, this.dataset, this.columns, this.schema);
    ResultsGenerator.transformDates(row, this.dataset, this.columns, this.schema);
    this.push(JSON.stringify(row, null, 2));
    callback();
  }

}

class StreamTransformResultsGeneratorPushdown extends Transform {

  constructor(dataset, columns, schema, options){
    super({ objectMode: true });
    this.first = true;
    this.columns = columns;
    this.schema = schema;
    this.dataset = dataset;
  }

  _transform(bucket, encoding, callback){
    if (!this.first)
      this.push(',');

    this.first = false;
    const row = ResultsGenerator.transformElasticPushdownBucketToRow(bucket, this.dataset, this.columns, this.schema);
    ResultsGenerator.transformDates(row, this.dataset, this.columns, this.schema);
    this.push(JSON.stringify(row, null, 2));
    callback();
  }

}

class StreamTransformResultsGeneratorNonPushdown extends Transform {

  constructor(dataset, columns, schema, options){
    super({ objectMode: true });
    this.first = true;
    this.columns = columns;
    this.schema = schema;
    this.dataset = dataset;
  }

  _transform(data, encoding, callback) {
    if (!this.first)
      this.push(',');

    const row = ResultsGenerator.transformElasticEntityToRow(data, this.dataset, this.columns, this.schema);
    ResultsGenerator.transformDates(row, this.dataset, this.columns, this.schema);
    this.push(JSON.stringify(row));
    this.first = false;

    callback();
  }

}

class CopyStream extends Transform {

  constructor() {
    super({ objectMode: true });
  }

  _transform(data, encoding, callback) {
    this.push(data);
    callback();
  }

}

module.exports = ResultsGenerator;
