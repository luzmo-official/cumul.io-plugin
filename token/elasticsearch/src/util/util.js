const errors = require('./errors');

class Utilities{

  isEmpty(v){
    return v === null || v === undefined;
  }

  getRecursiveOrFalse(obj, propertyList){
    if (this.isEmpty(obj))
      return false;
    else {
      if (propertyList.length > 0){
        const el = propertyList.shift();
        return this.getRecursiveOrFalse(obj[el], propertyList);
      }
      else
        return obj;
    }
  }

  checkDatetime(metadata, colName){
    for (let i = 0; i < metadata[0].columns.length; i++){
      const meta = metadata[0].columns[i];
      if (meta.id.toString().toLowerCase() === colName.toString().toLowerCase() && meta.type === 'datetime')
        return meta.id.toString();
    }
    return undefined;
  }

  toLowerCaseMap (map) {
    const lowerCaseMap = {};
    Object.keys(map).forEach((key) => {
      if (Object.prototype.hasOwnProperty.call(map, key)) lowerCaseMap[key.toLowerCase()] = map[key];
    });
    return lowerCaseMap;
  }

  reverseMap (map) {
    const reversed = {};
    Object.keys(map).forEach((cumulioType) => {
      const clickhouseTypes = map[cumulioType];
      clickhouseTypes.forEach((clickhouseType) => {
        reversed[clickhouseType] = cumulioType;
      });
    });
    return reversed;
  }

  sortByKey (array, key) {
    return array.sort(function (a, b) {
      const el1 = a[key];
      const el2 = b[key];
      if (el1 < el2)
        return -1;
      else
        return ((el1 > el2) ? 1 : 0);
    });
  }

  replaceAll (target, search, replacement) {
    return target.split(search).join(replacement);
  }

  isArray (value) {
    return Array.isArray(value);
  }

  toOriginalTableName(datasetId, schema) {
    let index = schema.indexToOriginalName[datasetId.toLowerCase()];
    if (index === undefined || index === null)
      throw errors.badRequest(`The dataset ${datasetId} could not be found. Are you sure that index (still) exists?`);
    return index;
  }

  toOriginalColumnName(datasetId, columnId, schema) {
    if (columnId === '*')
      return '*';

    return this.getColumnInfo(datasetId, columnId, schema).columnName;
  }

  getDatasetSchema(datasetId, schema) {
    if (!schema.indexesToColumns[datasetId.toLowerCase()])
      throw errors.badRequest(`The dataset ${datasetId} could not be found. Are you sure that index still exists?`);
    return schema.indexesToColumns[datasetId.toLowerCase()];
  }

  // Works for:
  // getColumnInfo(datasetId, columnId, schema)
  // getColumnInfo(columnId, datasetSchema)
  getColumnInfo(datasetId, columnId, schema) {
    let datasetSchema;
    if (!schema) {
      datasetSchema = columnId;
      columnId = datasetId;
    }
    else
      datasetSchema = this.getDatasetSchema(datasetId, schema);

    if (!datasetSchema[columnId.toLowerCase()]) {
      console.log('COLUMN NOT FOUND: ', datasetId, columnId, '. Schema: ', JSON.stringify(schema.indexesToColumns[datasetId.toLowerCase()]));
      throw errors.badRequest(`The column ${columnId} in dataset ${datasetId} could not be found. If you changed the schema, you might still need to update it in Cumul.io. It's also possible this column does not exist in the overridden data connection.`);
    }
    return datasetSchema[columnId.toLowerCase()];
  }

}

module.exports = new Utilities();
