const pg = require( 'pg' )
const errors = require( './../util/errors' )
const timeouts = require( './../util/timeouts' )
const typeMapping = require( './type-mapping' )

pg.types.setTypeParser( 1114, str => str )

// ***************** Public
class DatabaseWrapper {
  // Authorize simply checks whether the database can be accessed.
  static async authorize( details ) {
    return getClient( details )
      .then(( client ) => {
        client.end()
      })
  }

  // getDatasets retrieves the datasets and transforms them to a
  // convenient format to fetch information about a specific dataset or column.
  static async getDatasets( details ) {
    // we specifically only list
    return execQuery(
      details,
      'SELECT table_schema, table_name, column_name, data_type FROM information_schema.COLUMNS WHERE table_schema NOT IN (\'pg_catalog\',\'information_schema\')',
      timeouts.datasets
    )
      .then(( schema ) => {
        // This is optional, in case you intend to use the
        // schema later to do postprocessing on a query you might want to
        // keep it around in an efficient retrieval format.
        const datasetHashmap = {}
        schema.rows.forEach(( row ) => {
          const datasetId = `${row.table_schema}.${row.table_name}`
          if ( !datasetHashmap[ datasetId ]) {
            datasetHashmap[ datasetId ] = {}
          }

          const cumulioType = typeMapping.toCumulio( row.data_type )
          // When a type was not mapped, you can choose to:
          // - Return 'hierarchy' which is probably the best choice in case a type is not mapped.
          // - Leave out the column completely if there is no map for the type. In this case we return false and will leave it out.
          if ( cumulioType ) {
            datasetHashmap[ datasetId ][ row.column_name ] = {
              name: row.column_name,
              type: cumulioType,
              dbtype: row.data_type,
              table: datasetId
            }
          } else {
            // It is good to be notified in case we did made a mapping mistake though.
            console.warn( `Type not mapped, leaving out: ${row.column_name} of type: ${row.data_type}` )
          }
        })
        return datasetHashmap
      })
  }

  static async getData( details, query ) {
    // Cumulio query endpoint requires an array of arrays so in case of the query endpoint.
    // do not return json (default for this library)
    return execQuery( details, query, timeouts.query, 'array' )
  }
}

// ***************** Private
const getClient = async( details, timeout ) => {
  const client = new pg.Client({
    connectionTimeoutMillis: timeouts.authorize,
    statement_timeout: timeout,
    user: details.key,
    host: details.host,
    database: details.database,
    password: details.token,
    port: details.port
  })
  return client.connect()
    .then(() => client )
    .catch(( error ) => {
      console.error( 'Error in authorization', error )
      throw errors.unauthorizedError( 'Could not authenticate database' )
    })
}

// execQuery executes the main query from the 'query endpoint'
const execQuery = async( details, query, timeout, rowMode = false ) => {
  let client = null
  return getClient( details, timeout )
    .then(( clientReturned ) => {
      client = clientReturned
      if ( rowMode ) {
        return client.query({ rowMode: rowMode, text: query })
      } else {
        return client.query( query )
      }
    })
    .catch(( error ) => {
      if ( error && error.code && error.code === '57014' ) {
        throw errors.timeoutError( error )
      }
      console.error( error )
      throw errors.unexpectedError( 'Query failed' )
    })
    .then(( result ) => {
      return client.end().then(() => result )
    })
}

module.exports = DatabaseWrapper
