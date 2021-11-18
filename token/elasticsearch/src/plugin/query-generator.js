const FilterGenerator = require('./filter-generator');
const validation = require( './../util/validation' );
const errors = require( './../util/errors' );
const util = require('./../util/util.js');

class QueryGenerator {

  // schema is a mapping from column names to column info ({type, name, table})
  static generateQuery ( majorVersion, columns, filters, dataset, schema, pushdown ) {
    const compiledFilters = this.generateFilters(dataset, filters, schema);
    if (pushdown)
      return this.generatePushdownQuery( columns, compiledFilters, dataset, schema, pushdown );
    else
      return this.generateNonPushdownQuery( majorVersion, columns, compiledFilters, dataset, schema, pushdown );
  }

  static generatePushdownQueryWithoutGroups ( columns, colsGroupedAndAggr, compiledFilters, dataset, schema, pushdown ) {
    const query = {
      index: util.toOriginalTableName( dataset, schema ),
      body: {
        track_total_hits: true,
        size: 0
      }
    };
    if (Object.keys(colsGroupedAndAggr.aggregations).length > 0 )
      query.body.aggs = colsGroupedAndAggr.aggregations;

    if (compiledFilters && (compiledFilters.must.length > 0 || compiledFilters.must_not.length > 0))
      this.addFilters(query, compiledFilters);

    return query;
  }

  static generatePushdownQuery ( columns, compiledFilters, dataset, schema, pushdown ) {
    if ( !columns )
      throw errors.badRequest( 'No columns provided in query' );
    const cols = this.generateColumns( columns, dataset, schema );
    if (cols.groupBy.length === 0){
      // No groups, no buckets, else elasticsearch can't handle the aggregates.
      return this.generatePushdownQueryWithoutGroups(columns, cols, compiledFilters, dataset, schema, pushdown);
    }
    const query = {
      index: util.toOriginalTableName( dataset, schema ),
      body: {
        track_total_hits: true,
        size: 0,
        aggs: {
          all: {
            composite: {
              sources: cols.groupBy
            },
            aggs: cols.aggregations
          }
        }
      }
    };
    if (compiledFilters && (compiledFilters.must.length > 0 || compiledFilters.must_not.length > 0))
      this.addFilters(query, compiledFilters);

    return query;
  }

  static generateNonPushdownQuery( majorVersion, columns, compiledFilters, dataset, schema, pushdown ) {
    if (!columns)
      throw errors.badRequest( 'No columns provided in query' );
    const query = {
      index: util.toOriginalTableName( dataset, schema ),
      body: {
        size: 500,
        _source: columns.filter((c) => c.column_id !== '*' && util.getColumnInfo(dataset, c.column_id, schema).type !== 'datetime')
          .map((c) => util.toOriginalColumnName(dataset, c.column_id, schema)),
        docvalue_fields: columns.filter((c) => c.column_id !== '*' && util.getColumnInfo(dataset, c.column_id, schema).type === 'datetime')
          .map((c) => {
            const columnName = util.toOriginalColumnName(dataset, c.column_id, schema);
            if (majorVersion === 6)
              return columnName;
            else {
              return {
                format: 'epoch_millis',
                field: columnName
              };
            }
          })
      }
    };
    if (compiledFilters && (compiledFilters.must.length > 0 || compiledFilters.must_not.length > 0))
      this.addFilters(query, compiledFilters);
    else {
      query.body.query = {
        match_all: {}
      };
    }
    return query;
  }

  static addFilters(query, compiledFilters){
    query.body.query = {
      bool: { }
    };
    if (compiledFilters.must.length > 0)
      query.body.query.bool.must = compiledFilters.must;

    if (compiledFilters.must_not.length > 0)
      query.body.query.bool.must_not = compiledFilters.must_not;
  }

  static generateFilters(dataset, filters, schema){
    if (!util.isEmpty(filters) && filters.length > 0)
      return FilterGenerator.compileFilters(dataset, schema, filters);
    else
      return null;
  }

  static generateColumns(columns, dataset, schema ) {
    const groupByCols = [];
    const aggregations = {};
    const datasetSchema = util.getDatasetSchema(dataset, schema);
    columns.forEach((col, columnIndex) => {
      const columnName = util.toOriginalColumnName(dataset, col.column_id, schema);
      if (col.column_id === '*' || col.aggregation === 'count'){
        // do nothing, elasticsearch already provides counts.
      }
      else if (util.isEmpty(col.aggregation))
        groupByCols.push(this.generateGroupBy(col, columnName, columnIndex, datasetSchema));
      else
        aggregations[`col${columnIndex}`] = this.generateAggregate(col, columnName, datasetSchema);
    });
    return { groupBy: groupByCols, aggregations: aggregations };
  }

  static generateGroupBy(col, columnName, columnIndex, datasetSchema){
    const columnInfo = util.getColumnInfo(col.column_id, datasetSchema);
    const groupByObj = {};
    if (!util.isEmpty(col.level))
      groupByObj[`col${columnIndex}`] = this.generateDateGroupBy(col, columnName, columnInfo);
    else
      groupByObj[`col${columnIndex}`] = this.generateRegularGroupBy(col, columnName, columnInfo);

    return groupByObj;
  }

  static generateRegularGroupBy(col, columnName, columnInfo){
    const fieldName = columnInfo.hasKeyword ? `${columnName}.keyword` : columnName;
    return { terms: { field: fieldName, "missing_bucket": true } };
  }

  static generateDateGroupBy(col, columnName, dataset, datasetSchema){
    return {
      date_histogram: {
        field: columnName,
        interval: col.level
      }
    };
  }

  static generateAggregate(col, columnName, datasetSchema){
    if (validation.isValidAggregation){
      let aggregationObj = null;
      const columnInfo = datasetSchema[col.column_id];
      if (columnInfo.dbType === 'text')
        aggregationObj = this.generateTextColumnAggregate(col, columnName);
      else
        aggregationObj = this.generateNumericAggregate(col, columnName);

      return aggregationObj;
    }
    else
      throw Error('Unexpected aggregation received', col);
  }

  static generateNumericAggregate(col, columnName){
    const aggregationObj = {};
    aggregationObj[col.aggregation] = { field: columnName };
    return aggregationObj;
  }

  static generateTextColumnAggregate(col, columnName){
    return {
      terms: {
        field: columnName + '.keyword'
      }
    };
  }

}

module.exports = QueryGenerator;
