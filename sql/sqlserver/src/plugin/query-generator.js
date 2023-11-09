const validation = require("./../util/validation");
const errors = require("./../util/errors");
const util = require("./../util/util");

class QueryGenerator {
  // schema is a mapping from column names to column info ({type, name, table})
  generateQuery(columns, filters, tableId, schema, pushdown) {
    const table = this.quoteTable(tableId);
    if (!columns) {
      throw errors.unexpectedError("No columns provided in query");
    }
    const cols = this.generateColumns(columns, schema, pushdown),
      filterStatements = this.generateFilters(filters, table, schema),
      where =
        filterStatements.length > 0
          ? `WHERE ${filterStatements.join(" AND ")}`
          : "",
      groupby =
        cols.groupBy.length > 0 ? `GROUP BY ${cols.groupBy.join(", ")}` : "",
      query = `
        SELECT ${cols.select.join(", ")}
        FROM ${table}
        ${where}
        ${groupby}
      `;
    return query;
  }

  quoteTable(table) {
    const indexFirstDot = table.indexOf(".");
    if (indexFirstDot === -1) {
      return `[${table}]`;
    } else {
      return `[${table.substring(0, table.indexOf("."))}].[${table.substring(
        table.indexOf(".") + 1
      )}]`;
    }
  }

  // generateColumns transforms a list of columns to select and groupby statements as
  // described in pseudo code here: https://developer.cumul.io/#plugin-pseudo-code
  generateColumns(columns, schema, pushdown) {
    const selectColumns = [],
      groupByColumns = [];

    columns.forEach((column) => {
      const columnName = this.getColumnName(
        column,
        schema[column.column_id],
        pushdown
      );
      if (!pushdown) {
        selectColumns.push(columnName);
      } else if (
        column.aggregation &&
        validation.isValidAggregation(column.aggregation)
      ) {
        selectColumns.push(
          `${this.wrapInAggregation(columnName, column.aggregation)}`
        );
      } else {
        selectColumns.push(columnName);
        groupByColumns.push(columnName);
      }
    });
    return { select: selectColumns, groupBy: groupByColumns };
  }

  // generateFilters transforms a list of cumulio filters to SQL code.
  generateFilters(filters, datasetId, schema) {
    const accumulator = [];
    if (filters) {
      filters.forEach((filter) => {
        if (validation.isValidFilter(filter.expression)) {
          accumulator.push(this.generateOneFilter(filter, schema));
        } else {
          console.warn("Invalid filter ignored:", filter);
        }
      });
    }
    return accumulator;
  }

  // generateOneFilter transforms a Cumulio filter expression to SQL code
  // Note that most of the complexity is to ensure filters on null are
  // correctly calculated. SQL is a bit different in handling ‘null’ values than Cumul.io, as comparisons to NULL are tri-state!
  // https://www.sqlservercentral.com/articles/four-rules-for-nulls
  // Cumulio chose semantics are more understandable from a business perspective.
  generateOneFilter(filter, schema) {
    const columnInfo = schema[filter.column_id],
      originalCaseColName = columnInfo.name;

    if (filter.expression === "in" || filter.expression === "not in") {
      let values = this.parseExpressionValues(filter.value, columnInfo);
      const nullIndex = values.indexOf(null);
      let isNullCondition = "";
      if (nullIndex !== -1) {
        // No null in the filter values
        values = values.filter((val) => val != null);
        if (filter.expression === "in") {
          isNullCondition = ` OR ${originalCaseColName} is null`;
        } else {
          isNullCondition = ` AND ${originalCaseColName} is not null`;
        }
      } else if (filter.expression === "not in") {
        // There is a null value in the filter values
        isNullCondition = ` OR ${originalCaseColName} is null`;
      }
      return `(${originalCaseColName} ${filter.expression} (${values.join(
        ","
      )}) ${isNullCondition})`;
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
      column.column_id === "*"
        ? column.column_id
        : this.wrapColumnName(columnInfo.name);
    if (pushdown) {
      // columnInfo is null in case column == "*"
      if (column.column_id === "*") {
        return column.column_id;
      } else if (columnInfo.type === "datetime") {
        return this.wrapDateTrunc(column.level, originalCaseColName);
      } else {
        return originalCaseColName;
      }
    } else {
      return originalCaseColName;
    }
  }

  // parseExpressionValues, Filters contain values.
  // For example, when you filter all values greater than a certain date. That date is a value that might need
  // to be parsed or encoded.
  parseExpressionValues(values, columnInfo) {
    if (values) {
      if (columnInfo && columnInfo.type === "datetime") {
        return values.map((val) =>
          this.encodeDateValue(val, columnInfo.db_type)
        );
      } else if (columnInfo && columnInfo.type === "hierarchy") {
        return values.map((val) =>
          this.encodeHierarchyValue(val, columnInfo.db_type)
        );
      } else if (columnInfo && columnInfo.type === "numeric") {
        return values.map((val) =>
          this.encodeNumericValue(val, columnInfo.db_type)
        );
      } else {
        console.warn(
          `Unmapped value in parseExpressionValues ${
            columnInfo && columnInfo.type
          }`
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
      return `CONVERT(datetime2, '${incValue.split("+")[0]}', 126)`;
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
    if (aggregation === "distinctcount") {
      return `count(distinct ${columnName})`;
    } else {
      return `${aggregation}(${columnName})`;
    }
  }

  // wrapDateTrunc truncates dates based on the provided Cumulio level.
  wrapDateTrunc(level, column) {
    if (level && validation.isValidDateLevel(level)) {
      switch (level) {
        case "millisecond":
          return column;
        case "week":
          return `DATEADD(${level}, DATEDIFF(${level}, 0, DATEADD(day, -1, ${column})), 0)`;
        case "second":
          let truncateDatetime = `CAST(CAST(${column} as DATE) AS DATETIME)`;
          return `DATEADD(${level}, DATEDIFF(${level}, ${truncateDatetime} , ${column}), ${truncateDatetime})`;
        default:
          return `DATEADD(${level}, DATEDIFF(${level}, 0, ${column}), 0)`;
      }
    }
    return column;
  }

  // wrapDateTrunc truncates dates based on the provided Cumulio level.
  wrapDateFormatting(columnDefinition) {
    // Cumulio requires UTC
    // return `(${columnDefinition} at time zone 'UTC' AT TIME ZONE 'UTC')`
    return columnDefinition;
  }

  wrapColumnName(column) {
    return `[${column}]`;
  }
}

module.exports = new QueryGenerator();
