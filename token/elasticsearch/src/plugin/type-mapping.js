
class TypeMapping {
  toCumulio (esType) {
    if (esType === 'text' || esType === 'boolean' || esType === 'keyword' || esType === 'ip' ||
          esType === 'object' || esType === 'nested' || esType === 'binary') { return 'hierarchy' } else if (esType === 'short' || esType === 'integer' || esType === 'long' || esType === 'double' ||
                  esType === 'float' || esType === 'half_float' || esType === 'scaled_float') { return 'numeric' } else if (esType === 'date') { return 'datetime' } else { return 'hierarchy' }
  }

  isFilteredType (esType) {
    if (esType === 'geo_point') { return true } else { return false }
  }
}

module.exports = new TypeMapping()
