const validation = require('./../util/validation');
const errors = require('./../util/errors');
const util = require('./../util/util');

class QueryGenerator {
  // schema is a mapping from column names to column info ({type, name, table})
  generateQuery(columns, filters, tableId, schema, pushdown) {
    const where = this.generateFilters(filters || []);
    const cols = this.generateColumns(columns, pushdown);
    const select = cols.select;
    const groupby = cols.groupby;
    const from = '`' + schema.name.en?.split('.').join('`.`') + '`';

    let query = `SELECT ${select.join(', ')} FROM ${from}`;
    if (where.length > 0) query += ' WHERE ' + where;
    if (groupby && groupby.length > 0 && pushdown)
      query += ' GROUP BY ' + groupby.join(', ');

    return query;
  }

  // generateColumns transforms a list of columns to select and groupby statements as
  // described in pseudo code here: https://developer.cumul.io/#plugin-pseudo-code
  generateColumns(columns, pushdown) {
    const selectColumns = [],
      groupByColumns = [];

    columns.forEach((col, i) => {
      var obj = col.column_id;
      if (col.column_id !== '*') obj = '`' + col.column_id + '`';
      if (col.level && pushdown)
        obj =
          "DATE_ADD('1900-01-01', interval TIMESTAMPDIFF(" +
          col.level +
          ", '1900-01-01', " +
          col.column_id +
          ') ' +
          col.level +
          ')';
      if (col.aggregation && pushdown)
        obj = col.aggregation.toUpperCase() + '(' + obj + ')';
      else groupByColumns.push(obj);
      selectColumns.push(obj + ' c' + i);
    });
    return { select: selectColumns, groupBy: groupByColumns };
  }

  // generateFilters transforms a list of cumulio filters to SQL code.
  generateFilters(Requestfilters) {
    var sql = [];
    Requestfilters.forEach((filter) => {
      switch (filter.expression) {
        case '=':
        case '>':
        case '>=':
        case '<':
        case '<=':
        case '<>':
          sql.push(
            '`' +
              filter.column_id +
              '` ' +
              filter.expression +
              ' ' +
              escape(filter.value[0]),
          );
          break;
        case 'in':
          var condition_in =
            '`' +
            filter.column_id +
            '` IN (' +
            filter.value
              .filter((val) => val !== null)
              .map(escape)
              .join(',') +
            ')';
          if (filter.value.some((val) => val === null)) {
            // Null is allowed: include an explicit 'OR x IS NULL'
            condition_in += ' OR `' + filter.column_id + '` IS NULL';
          }
          sql.push(`(${condition_in})`);
          break;
        case 'not in':
          var condition_not_in =
            '`' +
            filter.column_id +
            '` NOT IN (' +
            filter.value
              .filter((val) => val !== null)
              .map(escape)
              .join(',') +
            ')';
          if (filter.value.some((val) => val === null)) {
            // Null is NOT allowed: include an explicit 'AND x IS NOT NULL'
            condition_not_in += ' AND `' + filter.column_id + '` IS NOT NULL';
          } else {
            // Null is allowed: include an explicit 'OR x IS NULL'
            condition_not_in += ' OR `' + filter.column_id + '` IS NULL';
          }
          sql.push(`(${condition_not_in})`);
          break;
        case 'is null':
          sql.push('`' + filter.column_id + '` IS NULL');
          break;
        case 'is not null':
          sql.push('`' + filter.column_id + '` IS NOT NULL');
          break;
        default:
          break;
      }
    });
    return sql.join(' AND ');
  }

  // generateOneFilter transforms a Cumulio filter expression to SQL code
  // Note that most of the complexity is to ensure filters on null are
  // correctly calculated. SQL is a bit different in handling ‘null’ values than Cumul.io, as comparisons to NULL are tri-state!
  // https://www.sqlservercentral.com/articles/four-rules-for-nulls
  // Cumulio chose semantics are more understandable from a business perspective.
  generateOneFilter(filter, schema) {
    const columnInfo = schema[filter.column_id],
      originalCaseColName = columnInfo.name;

    if (filter.expression === 'in' || filter.expression === 'not in') {
      let values = this.parseExpressionValues(filter.value, columnInfo);
      const nullIndex = values.indexOf(null);
      let isNullCondition = '';
      if (nullIndex !== -1) {
        // No null in the filter values
        values = values.filter((val) => val != null);
        if (filter.expression === 'in') {
          isNullCondition = ` OR ${originalCaseColName} is null`;
        } else {
          isNullCondition = ` AND ${originalCaseColName} is not null`;
        }
      } else if (filter.expression === 'not in') {
        // There is a null value in the filter values
        isNullCondition = ` OR ${originalCaseColName} is null`;
      }
      return `${originalCaseColName} ${filter.expression} (${values.join(
        ',',
      )}) ${isNullCondition}`;
    }
    const values = this.parseExpressionValues(filter.value, columnInfo);
    if (!util.isEmpty(values)) {
      return `${originalCaseColName} ${filter.expression} ${values[0]}`;
    } else {
      return `${originalCaseColName} ${filter.expression}`;
    }
  }

  // getColumnName retrieves the column and wraps it if necessary.
  // wrapping could mean, transforming a specific database type to make sure it works with a specific function.
  // or simply wrap it in trunc functions for dates that are selected on a specific level.
  getColumnName(column, columnInfo, pushdown) {
    const originalCaseColName =
      column.column_id === '*' ? column.column_id : columnInfo.name;
    if (pushdown) {
      // columnInfo is null in case column == "*"
      if (column.column_id === '*') {
        return column.column_id;
      } else if (columnInfo.type === 'datetime') {
        // Some databases will not return the required iso format: 2018-01-01T00:00:00Z
        // in that case you can wrap it in a formatting function here.
        // In this example we specifically disabled the automatic conversion to javascript dates by the library
        // for three reasons:
        // - to show you what to do if it is not automatically done for you.
        // - it is probably more efficient when it is done by the database.
        // - node transforms it in the locale timezone of the client, cumulio requires UTC
        return this.wrapDateFormatting(
          this.wrapDateTrunc(column.level, originalCaseColName),
        );
      } else {
        return originalCaseColName;
      }
    } else {
      if (columnInfo.type === 'datetime') {
        return this.wrapDateFormatting(originalCaseColName);
      } else {
        return originalCaseColName;
      }
    }
  }

  // parseExpressionValues, Filters contain values.
  // For example, when you filter all values greater than a certain date. That date is a value that might need
  // to be parsed or encoded.
  parseExpressionValues(values, columnInfo) {
    if (values) {
      if (columnInfo && columnInfo.type === 'datetime') {
        return values.map((val) =>
          this.encodeDateValue(val, columnInfo.db_type),
        );
      } else if (columnInfo && columnInfo.type === 'hierarchy') {
        return values.map((val) =>
          this.encodeHierarchyValue(val, columnInfo.db_type),
        );
      } else if (columnInfo && columnInfo.type === 'numeric') {
        return values.map((val) =>
          this.encodeNumericValue(val, columnInfo.db_type),
        );
      } else {
        console.warn(
          `Unmapped value in parseExpressionValues ${
            columnInfo && columnInfo.type
          }`,
        );
      }
    }
    return values;
  }

  // encodeDateValue, Cumulio filter date value -> Database Value
  // in case your database needs another datatype than the iso date as input for filters.
  // wrap or transform it here.
  // Dummy implementation
  encodeDateValue(incValue, dbType) {
    if (!util.isEmpty(incValue)) {
      // incoming value is isoString,
      // if your database needs another foramt when constructing queries, convert it here
      return `'${incValue}'`;
    } else {
      return null;
    }
  }

  // encodeHierarchyValue, Cumulio filter hierarchy value -> Database value
  // Dummy implementation
  encodeHierarchyValue(incValue, dbType) {
    if (!util.isEmpty(incValue)) {
      return `'${incValue}'`;
    } else {
      return null;
    }
  }

  // encodeNumericValue, Cumulio filter numeric value -> Database Value
  // Dummy implementation
  encodeNumericValue(incValue, dbType) {
    if (!util.isEmpty(incValue)) {
      return incValue;
    } else {
      return null;
    }
  }

  // wrapInAggregation wraps the column name in aggregations.
  // In most SQL cases this is rather simple.
  wrapInAggregation(columnName, aggregation) {
    if (aggregation === 'distinctcount') {
      return `count(distinct ${columnName})`;
    } else {
      return `${aggregation}(${columnName})`;
    }
  }

  // wrapDateTrunc truncates dates based on the provided Cumulio level.
  wrapDateTrunc(level, column) {
    if (level && validation.isValidDateLevel(level)) {
      let mappedLevel = level;
      // most levels correspond to the date_trunc levels in postgres.
      if (level === 'millisecond') {
        mappedLevel = 'milliseconds';
      }
      return `date_trunc('${mappedLevel}', ${column})`;
    }
    return column;
  }

  // wrapDateTrunc truncates dates based on the provided Cumulio level.
  wrapDateFormatting(columnDefinition) {
    // Cumulio requires UTC
    return `to_char(${columnDefinition} at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`;
  }
}

module.exports = new QueryGenerator();
