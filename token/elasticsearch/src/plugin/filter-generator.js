const validation = require( './../util/validation' );
const util = require('./../util/util.js');

const cumulioFilterExpressionToEs = {
  '<': 'lt',
  '>': 'gt',
  '<=': 'lte',
  '>=': 'gte'
};

class FilterMapper{

  static compileFilters(dataset, schema, filters){
    const accumulator = {
      must: [],
      must_not: []
    };
    filters.forEach((filter) => {
      const columnInfo = util.getColumnInfo(dataset, filter.column_id, schema);
      return this.compileFilter(columnInfo.columnName, filter.expression, filter.value, columnInfo, accumulator);
    });
    return accumulator;
  }

  static compileFilter(name, cumulExpression, values, columnInfo, accumulator){
    if (!validation.isValidFilter(cumulExpression))
      throw new Error('Received invalid filter', cumulExpression);
    else {
      if (columnInfo.type === 'numeric')
        this.compileNumericOrDateFilter(name, cumulExpression, values, accumulator, columnInfo.type);
      else if (columnInfo.type === 'datetime') {
        if (!util.isEmpty(values))
          values = values.map((val) => new Date(val).getTime());

        this.compileNumericOrDateFilter(name, cumulExpression, values, accumulator, columnInfo.type);
      }
      else if ( columnInfo.type === 'hierarchy')
        this.compileHierarchyFilter(name, cumulExpression, values, accumulator, columnInfo.dbType);
    }
  }

  static compileHierarchyFilter(name, cumulExpression, values, accumulator, dbType){
    const obj = { };
    switch (cumulExpression) {
      case '=': {
        obj.terms = {};
        obj.terms[dbType === 'boolean' || dbType === 'keyword' ? name : name + '.keyword'] = values;
        accumulator.must.push(obj);
        break;
      }
      case '<':
      case '>':
      case '<=':
      case '>=': {
        console.log('TODO hierarchy comparisons.');
        break;
      }
      case 'in':
      case 'not in': {
        obj.bool = {};
        obj.bool.should = [];
        const terms = {};
        terms[dbType === 'boolean' || dbType === 'keyword' ? name : name + '.keyword'] = values.filter((el) => el !== null);
        obj.bool.should.push( { terms: terms });
        if (values.includes(null))
          obj.bool.should.push( { bool: { must_not: { exists: { field: name } } } } );

        if (cumulExpression === 'in')
          accumulator.must.push(obj);
        else
          accumulator.must_not.push(obj);

        if (values.includes(null)){
          // wrap in or!
        }
        break;
      }
      case 'is null': {
        accumulator.must_not.push({
          exists: {
            field: name
          }
        });
        break;
      }
      case 'is not null': {
        accumulator.must.push({
          exists: {
            field: name
          }
        });
        break;
      }
      default:
        throw new Error('Unknown filter expression: ' + cumulExpression);
    }
  }

  static compileNumericOrDateFilter(name, cumulExpression, values, accumulator, type){
    const obj = {
      range: {}
    };
    obj.range[name] = {};
    switch (cumulExpression) {
      case '=':
      case '<>': {
        // equal and not equal are implemented as a greater and smaller than two values that lie
        // just next to it.
        const value = values[0];
        const filters = this.compileToRangeFilter(cumulExpression, value);
        // then compile the filters to ES syntax.
        filters.forEach((f) => {
          obj.range[name][cumulioFilterExpressionToEs[f.expr]] = f.value;
          if (type === 'datetime') obj.range[name].format = 'epoch_millis';
        });
        accumulator.must.push(obj);
        break;
      }
      case '<':
      case '>':
      case '<=':
      case '>=': {
        obj.range[name][cumulioFilterExpressionToEs[cumulExpression]] = values[0];
        if (type === 'datetime') obj.range[name].format = 'epoch_millis';
        accumulator.must.push(obj);
        break;
      }
      case 'in': {
        obj.bool = {};
        obj.bool.should = [];
        values.forEach((value) => {
          const range = {};
          range[name] = {};
          range[name][cumulioFilterExpressionToEs['>=']] = value;
          range[name][cumulioFilterExpressionToEs['<=']] = value;
          if (type === 'datetime') obj.range[name].format = 'epoch_millis';
          obj.bool.should.push({ range: range });
        });
        delete obj.range;
        accumulator.must.push(obj);
        break;
      }
      case 'not in': {
        values.forEach((value) => {
          obj.range[name][cumulioFilterExpressionToEs['>=']] = value;
          obj.range[name][cumulioFilterExpressionToEs['<=']] = value;
          if (type === 'datetime') obj.range[name].format = 'epoch_millis';
        });
        accumulator.must_not.push(obj);
        break;
      }
      case 'is null': {
        accumulator.must_not.push({
          exists: {
            field: name
          }
        });
        break;
      }
      case 'is not null': {
        accumulator.must.push({
          exists: {
            field: name
          }
        });
        break;
      }
      default:
        throw new Error('Unknown filter expression: ' + cumulExpression);
    }
  }

  static compileToRangeFilter(cumulioExpression, value) {
    const range = this.splitValueInSurroundingValues(value);
    if (cumulioExpression === '=')
      return [{ expr: '>=', value: range[0] }, { expr: '<=', value: range[1] }];
    else
      return [{ expr: '<=', value: range[0] }, { expr: '>=', value: range[1] }];
  }

  // if you would present it with a value '5', you would get: [4.9999...999, 5.000...00001]
  static splitValueInSurroundingValues(value){
    const truncated = Number(parseInt(value).toFixed(16));
    const values = [];
    values[0] = truncated - 0.5 * Math.pow(10, -16);
    values[1] = truncated + 0.5 * Math.pow(10, -16);
    return values;
  }

}

module.exports = FilterMapper;
