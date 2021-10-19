
const util = require( './../util/util' )

class TypeMapping {
  constructor() {
    this.cumulioToClickhouse = {
      numeric: [ 'money', '^smallint$', '^integer$', '^int$', '^tinyint$', '^numeric', '^bigint$', '^decimal$',
        '^double precision$', '^serial$', '^float(\\d)?$', '^real', '^float8', '^numeric\\((.+)(.+)\\)$' ],
      datetime: [ '^date$', '^time$', '^(small)?datetime(\\d)?(offset)?$', '^timestamp$', '^timestamptz$', '^interval$',
        '^timestamp without time zone$', '^timestamp with time zone$', '^time without time zone$', '^time with time zone$' ],
      hierarchy: [ '^uuid', '^(var)?binary$', '^xml$', '^hierarchyid$', '^character varying$', '^n?varchar(\\d)?$', '^n?char(\\d)?$', '^character$', '^n?text$', '^boolean$', '^bit$', '^bit varying$', '^uniqueidentifier$' ]
    }
    this.clickhouseToCumulio = util.reverseMap( this.cumulioToClickhouse )
  }

  toCumulio( clickhouseType ) {
    let foundType = false
    Object.keys( this.clickhouseToCumulio ).forEach(( key ) => {
      const regex = new RegExp( key )
      if ( regex.test( clickhouseType.toLowerCase())) {
        foundType = this.clickhouseToCumulio[ key ]
      }
    })
    return foundType
  }
}

module.exports = new TypeMapping()
