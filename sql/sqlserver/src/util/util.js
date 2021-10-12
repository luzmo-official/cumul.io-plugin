
class Util {
  toLowerCaseMap( map ) {
    const lowerCaseMap = {}
    Object.keys( map ).forEach(( key ) => {
      if ( typeof ( map[key] ) !== 'undefined' ) {
        lowerCaseMap[ key.toLowerCase() ] = map[ key ]
      }
    })
    return lowerCaseMap
  }

  reverseMap( map ) {
    const reversed = {}
    Object.keys( map ).forEach(( cumulioType ) => {
      const clickhouseTypes = map[ cumulioType ]
      clickhouseTypes.forEach(( clickhouseType ) => {
        reversed[ clickhouseType ] = cumulioType
      })
    })
    return reversed
  }

  sortByKey( array, key ) {
    return array.sort(( a, b ) => {
      const el1 = a[ key ]
      const el2 = b[ key ]
      if ( el1 < el2 ) {
        return -1
      } else {
        return (( el1 > el2 ) ? 1 : 0 )
      }
    })
  }

  replaceAll( target, search, replacement ) {
    return target.split( search ).join( replacement )
  }

  isEmpty( value ) {
    return value === null || typeof value === 'undefined'
  }

  isArray( value ) {
    return Array.isArray( value )
  }
}
module.exports = new Util()
