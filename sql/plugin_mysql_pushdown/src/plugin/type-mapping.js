class TypeMapping {
  constructor() {
    const typeMapping = {
      integer: { type: 'numeric', format: ',.0f', informat: 'numeric' },
      int: { type: 'numeric', format: ',.0f', informat: 'numeric' },
      smallint: { type: 'numeric', format: ',.0f', informat: 'numeric' },
      tinyint: { type: 'numeric', format: ',.0f', informat: 'numeric' },
      mediumint: { type: 'numeric', format: ',.0f', informat: 'numeric' },
      bigint: { type: 'numeric', format: ',.0f', informat: 'numeric' },
      decimal: { type: 'numeric', format: ',.2f', informat: 'numeric' },
      numeric: { type: 'numeric', format: ',.2f', informat: 'numeric' },
      float: { type: 'numeric', format: ',.2f', informat: 'numeric' },
      double: { type: 'numeric', format: ',.2f', informat: 'numeric' },
      bit: { type: 'hierarchy', format: '', informat: 'hierarchy' },
      date: {
        type: 'datetime',
        format: '%d-%m-%Y',
        informat: 'YYYY-MM-DD HH:mm:ss.SSS',
      },
      datetime: {
        type: 'datetime',
        format: '%d-%m-%Y %H:%M:%S.%L',
        informat: 'YYYY-MM-DD HH:mm:ss.SSS',
      },
      timestamp: {
        type: 'datetime',
        format: '%d-%m-%Y %H:%M:%S.%L',
        informat: 'YYYY-MM-DD HH:mm:ss.SSS',
      },
      time: {
        type: 'datetime',
        format: '%H:%M:%S.%L',
        informat: 'YYYY-MM-DD HH:mm:ss.SSS',
        highestLevel: 6,
      },
      year: {
        type: 'datetime',
        format: '%Y',
        informat: 'YYYY-MM-DD HH:mm:ss.SSS',
      },
      char: { type: 'hierarchy', format: '', informat: 'hierarchy' },
      varchar: { type: 'hierarchy', format: '', informat: 'hierarchy' },
      binary: { type: 'hierarchy', format: '', informat: 'hierarchy' },
      varbinary: { type: 'hierarchy', format: '', informat: 'hierarchy' },
      text: { type: 'hierarchy', format: '', informat: 'hierarchy' },
      tinytext: { type: 'hierarchy', format: '', informat: 'hierarchy' },
      mediumtext: { type: 'hierarchy', format: '', informat: 'hierarchy' },
      longtext: { type: 'hierarchy', format: '', informat: 'hierarchy' },
      enum: { type: 'hierarchy', format: '', informat: 'hierarchy' },
      set: { type: 'hierarchy', format: '', informat: 'hierarchy' },
      json: { type: 'hierarchy', format: '', informat: 'hierarchy' },
      string: { type: 'hierarchy', format: '', informat: 'hierarchy' },
    };

    this.MYSQL_TO_CUMULIO = new Map(Object.entries(typeMapping));
  }

  toCumulioType(mysqlType) {
    if (mysqlType && COLUMN_TYPES[mysqlType.toLowerCase()])
      return COLUMN_TYPES[mysqlType.toLowerCase()].type;
    return 'hierarchy';
  }
}

module.exports = new TypeMapping();
