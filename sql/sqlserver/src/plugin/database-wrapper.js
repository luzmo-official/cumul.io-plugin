const errors = require( './../util/errors' )
const timeouts = require( './../util/timeouts' )
const typeMapping = require( './type-mapping' )
const { ConnectionPool } = require('mssql');
const Memoizer = require('../util/memoizer');
const nodeCleanup = require('node-cleanup');

const connectionPools = new Memoizer(1000, 60 * 60 * 1000, 
  async( _key, promise ) => {
    promise.then(pool => {
      pool.close();
    });
  },
  async( promise ) => {
    promise.then(pool => {
      return !pool._connected;
    });
  });

const getMemoizeKey = ( details ) => {
  return JSON.stringify(details);
}

nodeCleanup(async () => {
  console.log('Closing open connection pools');
  return connectionPools.closeAll();
});

// ***************** Public
class DatabaseWrapper {
  // Authorize simply checks whether the database can be accessed.
  static async authorize( details ) {
    return getClient( details )
  }

  // getDatasets retrieves the datasets and transforms them to a
  // convenient format to fetch information about a specific dataset or column.
  static async getDatasets( details ) {
    // we specifically only list
    return execQuery(
      details,
      'SELECT table_schema, table_name, column_name, data_type FROM information_schema.COLUMNS WHERE table_schema NOT IN (\'information_schema\')',
      timeouts.datasets
    )
      .then(( schema ) => {
        // This is optional, in case you intend to use the
        // schema later to do postprocessing on a query you might want to
        // keep it around in an efficient retrieval format.
        const datasetHashmap = {}
        schema.recordset.forEach(( row ) => {
          const datasetId = `${row.table_schema}.${row.table_name}`
          if ( !datasetHashmap[ datasetId ]) {
            datasetHashmap[ datasetId ] = {}
          }

          const cumulioType = typeMapping.toCumulio( row.data_type )
          // When a type was not mapped, you can choose to:
          // - Return 'hierarchy' which is probably the best choice in case a type is not mapped.
          // - Leave out the column completely if there is no map for the type. In this case we return false and will leave it out.
          if ( cumulioType ) {
            datasetHashmap[ datasetId ][ row.column_name.toLowerCase() ] = {
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
    return execQuery( details, query, timeouts.query, true )
  }
}

// ***************** Private
const getClient = async( details, timeout ) => {
  const config = {
    user: details.key,
    password: details.token,
    database: details.database,
    server: details.host,
    port: details.port,
    pool: {
      max: 3,
      min: 1,
      idleTimeoutMillis: 120000
    },
    options: {
      encrypt: true,
      trustServerCertificate: true,
      enableArithAbort: true,
    }
  };
  const pool = new ConnectionPool(config);
  return pool.connect()
    .then(() => pool)
    .catch(error => {
      console.error('Error authorizing to database', error)
      throw errors.unauthorizedError('Could not authenticate to database');
    });
}

// execQuery executes the main query from the 'query endpoint'
const execQuery = async( details, query, timeout, arrayRowMode = false ) => {
  let client = null
  return connectionPools.memoize(getMemoizeKey(details), getClient, details, timeout )
    .then(( clientReturned ) => {
      client = clientReturned
      const request = clientReturned.request();
      request.arrayRowMode = arrayRowMode;
      return request.query( query );
    })
    .catch(( error ) => {
      console.error('Error in execQuery', error);
      if ( error && error.code && error.code === '57014' ) {
        throw errors.timeoutError( error )
      }
      console.error( error )
      throw errors.unexpectedError( 'Query failed' )
    })
    .then(( result ) => {
      return result; 
    })
}

module.exports = DatabaseWrapper
