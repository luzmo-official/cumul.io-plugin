
const dbWrapper = require( './plugin/database-wrapper' )
const queryGenerator = require( './plugin/query-generator' )
const resultsGenerator = require( './plugin/results-generator' )

const schemaCache = require( './util/schema-cache' )
const validation = require( './util/validation' )
const requestParser = require( './util/request-parser' )

class Controller {
  constructor() {
    this.authorize = this.authorize.bind( this )
    this.datasets = this.datasets.bind( this )
    this.query = this.query.bind( this )
  }

  // ************************ Authorize **************************
  // https://developer.cumul.io/#post-authorize
  // Authorize is called at the moment that the an account is added to the plugin.
  // via the UI: New Dataset -> Select your plugin -> Add Account.
  // It is meant to verify whether the account that is added to the plugin is accessible
  // before adding it.
  // *************************************************************
  async authorize( request ) {
    // { database, host, port, auth, secret }
    const details = requestParser.parse( request, 'authorize' )
    validation.validateSecret( details.secret )
    await dbWrapper.authorize( details )
    return {
      statusCode: 200,
      body: 'Success'
    }
  }

  // ************************ Datasets **************************
  // https://developer.cumul.io/#get-datasets
  // Datasets is called at the moment a user tries to add a dataset via the plugin
  // In other words, in the UI that is: New Dataset -> Select your plugin (it already has an account)
  // the 'datasets' call is responsible of listing all the datasets that a user can choose from.
  // Cache:
  // in the example below we use an indirection via a cache. Consecutive calls to the 'datasets' api are never
  // cached. However, in many cases a plugin also need the 'datasets' information in the 'query' endpoint for query generation or postprocessing.
  // Since the frequency of the 'query' endpoint will be higher and performance often matters, the metadata
  // information is not fetched again when a query happens.
  // *************************************************************
  async datasets( request ) {
    const details = requestParser.parse( request, 'datasets' )
    validation.validateSecret( details.secret )
    // First fetch the metadata from your database.
    const datasetHashmap = await dbWrapper.getDatasets( details )
    schemaCache.storeDatasets( details, datasetHashmap )
    const datasets = resultsGenerator.generateDatasets( datasetHashmap )
    return {
      statusCode: 200,
      body: datasets
    }
  }

  // ************************ Query **************************
  // https://developer.cumul.io/#post-query-2
  // *************************************************************
  // Note that the plugin is not implemented in a streaming way to keep it simple.
  // In case you are handling large data, the query endpoint is best written in a streaming fashion.
  // Else you might run into memory issues.
  // Feel free to reach out for a JS example of a streaming endpoint.
  async query( request ) {
    const details = requestParser.parse( request, 'query' )
    validation.validateSecret( details.secret )
    // Note that a pushdown plugin can still receive a flag pushdown = false.
    // This is typically for queries such as for a 'data table' where one needs the data as it is
    // and not an aggregation or rollup.
    const pushdown = details.body.options ? details.body.options.pushdown : false
    // Here we avoid to fetch the schema again and fetch it instead from the cache.
    // Often, in a plugin you need the schema for query generation or postprocessing.
    // If the cache does not contain the schema ,
    let schema = await schemaCache.getDatasets( details )
    if ( !schema ) {
      schema = await dbWrapper.getDatasets( details )
      schemaCache.storeDatasets( details, schema )
    }
    const schemaForTable = schema[ details.body.id ]
    const query = await queryGenerator.generateQuery( details.body.columns, details.body.filters, details.body.id, schemaForTable, pushdown )
    console.log('## QUERY: ', query);
    const result = await dbWrapper.getData( details, query )

    // not necessary we already do it in the database, this call is kept as an example.
    // const data = resultsGenerator.generateData( result.rows, details.body.columns, schemaForTable )
    const data = result.recordset

    return {
      statusCode: 200,
      body: data
    }
  }
}

module.exports = new Controller()
