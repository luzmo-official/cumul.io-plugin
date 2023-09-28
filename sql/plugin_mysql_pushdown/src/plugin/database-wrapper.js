const fs = require('fs');
const mysql = require('mysql');
const nodeCleanup = require('node-cleanup');

const errors = require('./../util/errors');
const timeouts = require('./../util/timeouts');
const typeMapping = require('./type-mapping');

// Declare connection pool
let pool;

const cache = { cached_at: null, cache: null, interval: 24 * 60 * 60 * 1000 };

nodeCleanup(() => {
  if (pool) pool.end();
});

// ***************** Public
class DatabaseWrapper {
  // Authorize simply checks whether the database can be accessed.
  static authorize(headers) {
    pool = getConnectionPool(headers);

    return pool.getConnection(function (err, conn) {
      if (err) {
        console.error(err);
        conn.release();
        throw error;
      }
      return;
    });
  }

  // getDatasets retrieves the datasets and transforms them to a
  // convenient format to fetch information about a specific dataset or column.
  static async getDatasets(headers) {
    //if implementing a broker plugin, broker here based on fields you provided when creating the authorization token
    pool = getConnectionPool(headers);

    return new Promise((resolve, reject) => {
      pool.getConnection(function (err, conn) {
        if (err) {
          console.error(err);
          conn.release();
          reject(
            errors.unexpectedError('MySQL database could not be reached.'),
          );
        }

        schema(conn, function (error, datasets) {
          conn.release();

          if (error)
            reject(errors.unexpectedError('Error while querying for schema'));

          resolve(datasets);
        });
      });
    });
  }

  static async getData(details, query) {
    // Cumulio query endpoint requires an array of arrays so in case of the query endpoint.
    // do not return json (default for this library)
    return execQuery(details, query);
  }
}

// ***************** Private

function getConnectionPool(eventHeaders) {
  if (pool) {
    return pool;
  } else {
    // Create a connection pool
    var connectionPool = mysql.createPool({
      connectionLimit: 100,
      host: process.env.MYSQL_HOST,
      user: eventHeaders['x-key'],
      password: eventHeaders['x-token'],
      ssl: {
        ca: fs.readFileSync(process.cwd() + '/keys/server-ca.pem'),
        // Optional client side validation certificates
        key: fs.readFileSync(process.cwd() + '/keys/client-key.pem'),
        cert: fs.readFileSync(process.cwd() + '/keys/client-cert.pem'),
      },
    });
  }
  return connectionPool;
}

// function to retrieve the metadata (which datasets and columns are available)
function schema(conn, callback) {
  // if cached, retrieve from cache
  if (cache.cached_at > new Date().getTime() - cache.interval)
    return callback(null, cache.cache);

  conn.query(
    `
      SELECT tbl.table_schema, tbl.table_name, tbl.table_type, col.column_name, col.data_type
      FROM information_schema.tables tbl
      LEFT JOIN information_schema.columns col ON tbl.table_schema = col.table_schema AND tbl.table_name = col.table_name
      WHERE tbl.table_schema NOT IN ('information_schema','mysql','sys','performance_schema') AND col.column_name IS NOT NULL
      ORDER BY tbl.table_schema, tbl.table_name, col.ordinal_position
    `,
    function (error, data) {
      if (error) return callback(error);

      var schemaResult = [];
      var previous;

      for (const row of data) {
        const tableSchema = row.TABLE_SCHEMA || row.table_schema;
        const tableName = row.TABLE_NAME || row.table_name;
        const columnName = row.COLUMN_NAME || row.column_name;
        const prevTableSchema =
          previous?.TABLE_SCHEMA || previous?.table_schema;
        const prevTableName = previous?.TABLE_NAME || previous?.table_name;

        if (
          !previous ||
          prevTableSchema + '.' + prevTableName !==
            tableSchema + '.' + tableName
        ) {
          schemaResult.push({
            id: (tableSchema + '.' + tableName),
            name: { en: tableSchema + '.' + tableName },
            description: { en: tableSchema + '.' + tableName },
            columns: [],
          });
          previous = row;
        }
        schemaResult[schemaResult.length - 1].columns.push({
          name: { en: columnName },
          id: columnName,
          type: typeMapping.toCumulioType(row.data_type),
        });
      }

      cache.cached_at = new Date().getTime();
      cache.cache = schemaResult;
      return callback(null, schemaResult);
    },
  );
}

// execQuery executes the main query from the 'query endpoint'
const execQuery = async (details, query) => {
  pool = getConnectionPool(details.headers);

  return new Promise((resolve, reject) => {
    pool.getConnection(function (err, conn) {
      if (err) {
        console.error(err);
        conn.release();
        reject('MySQL database could not be reached.');
      }
      conn.query(query, function (query_err, data) {
        conn.release();

        if (query_err) {
          console.error(query_err);
          reject('MySQL database could not be reached.');
        }

        const results = data.map((row) => {
          return Object.keys(row).map((columnName, i) => {
            if (details.body.columns[i].level && row[columnName] !== null)
              return new Date(row[columnName]).toISOString();
            return row[columnName];
          });
        });

        resolve(results);
      });
    });
  });
};

module.exports = DatabaseWrapper;
