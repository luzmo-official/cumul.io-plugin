
const util = require( './../util/util' )

class TypeMapping {
  constructor() {
    this.cumulioToClickhouse = {
      // Below is an example of different kind of type definitions.
      // Some are never returned in this postgres plugin since the 'data_type' field
      // returns simplified types. However others are kept as an example to match more complex types in case your database returns 'raw types'.
      // Types such as Json, Xml are not matched in this example since they are
      // typically not useful in analytics applications.
      numeric: [ 'money', '^smallint$', '^integer$', '^numeric', '^bigint$', '^decimal$',
        '^double precision$', '^serial$', '^float(\\d)$', '^real', '^float8', '^numeric\\((.+)(.+)\\)$' ],
      datetime: [ '^date$', '^time$', '^timestamp$', '^timestamptz$', '^interval$',
        '^timestamp without time zone$', '^timestamp with time zone$', '^time without time zone$', '^time with time zone$' ],
      hierarchy: [ '^uuid', '^character varying$', '^varchar(\\d)$', '^char(\\d)$', '^character$', '^text$', '^boolean$', '^bit$', '^bit varying$' ]
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
