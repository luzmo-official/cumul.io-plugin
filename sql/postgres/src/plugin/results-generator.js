const moment = require( 'moment' )

class Generator {
  replaceAll( target, search, replacement ) {
    return target.split( search ).join( replacement )
  }

  // Generates the result set that the data endpoint requires
  // The result we throw out of the clickhouse call does not adhere to this format directly
  // to avoid inefficient retrieval for a specific table/column in case we need the schema for the query generator.
  // or in case we need to pass over the result set after querying to format the result.
  // Therefore the incoming format of tableStructure here is:
  // {
  //    "<table-name>": {
  //      "<column-name>": {
  //        { name: <column-name>, type: <column-type>, table: <table> }
  //      }
  //    }
  // }

  // The final result that Cumulio requires looks like:
  // {
  //  id: <table id>,
  //  name: { en: '< name >', nl: ..., fr: ...},
  //  description: { en: ' <description> ', nl: ..., fr: ...},
  //  columns: [
  //    { id: < column id> ,
  //     name: { en: < column name >, nl: ..., fr: ... },
  //     type: '< hierarchy | numeric | datetime >'
  //    }
  //   ]
  // }

  generateDatasets( datasetHashmap ) {
    const tableNames = Object.keys( datasetHashmap )
    const result = tableNames.map(( tableName ) => {
      const columnsHash = datasetHashmap[ tableName ]
      const columNames = Object.keys( columnsHash )
      const columns = columNames.map( columnName => {
        const column = columnsHash[ columnName ]
        return {
          id: column.name,
          name: { en: column.name },
          type: column.type
        }
      })
      return {
        id: tableName,
        name: {
          en: tableName.replace( '.', ' - ' )
        },
        description: {
          en: ''
        },
        columns
      }
    })
    return result
  }

  // Sometimes data might not correspond to the Cumul.io format.
  // e.g. the most common mistake is to send a wrong date format.
  // In this case we enforce the dateformat by formatting on the database (which is often more efficient)
  // However, below is the code for in case you did not format on the database as an example
  // Data passed here is:
  //    data: array of rows
  //    columns: the columns as passed to the query, they
  //             came in the order that the data has to go out.
  //             we'll use this to get the corresponding column for an index in the row.
  //             If you have a database that provides results in a different format, you can use 'columns'
  //             to transform that format back to an array of rows and get the order of the elements in the row right.
  //    schemaForTable: the schema as a hashmap of columnId to the column information, in case you need the type to decide.

  // Note that the plugin is not implemented in a streaming way to keep it simple.
  // In case you are handling large data, the query endpoint is best written in a streaming fashion which
  // requires changes on the generation here and on the express call and return format.
  generateData( data, columns, schemaForTable ) {
    // small optimization
    const columnInformationPerIndex = []
    columns.forEach(( col, index ) => {
      if ( col.column_id !== '*' ) {
        columnInformationPerIndex[ index ] = schemaForTable[ col.column_id ]
      }
    })
    data.forEach(( row ) => {
      row.forEach(( element, index ) => {
        const colInfo = columnInformationPerIndex[ index ]
        if ( colInfo ) {
          // colInfo contains both the cumuliotype ('type') and the database type ('dbtype')
          if ( colInfo.type === 'datetime' ) {
            row[ index ] = parseDate( element )
          }
        }
      })
    })
    return data
  }
}

const parseDate = ( el ) => {
  // Cumulio requires UTC, note that moment is an awesome library but it is a bit slow.
  return moment.utc( el ).toIsoString()
}

module.exports = new Generator()
