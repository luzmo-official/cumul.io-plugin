
// Validation is not strictly necessary since Cumul.io
// will not send you illegal queries. While testing it might however indicate very quickly
// that there is a mistake in the 'test query'.
// Besides of that it is helpful to have an idea what syntax exists.
const errors = require( './errors' )
const LEVEL = [
  'year',
  'quarter',
  'month',
  'week',
  'day',
  'hour',
  'minute',
  'second',
  'millisecond'
]
const AGGREGATION = [ 'sum', 'min', 'max', 'distinctcount', 'count' ]
const FILTER = [ '<', '>', '<>', '=', 'in', 'not in', '<=', '>=', 'is null', 'is not null' ]

class Validation {
  isValidDateLevel( level ) {
    const res = LEVEL.includes( level )
    if ( !res ) {
      console.error( 'Invalid level: ', level )
    }
    return res
  }

  isValidAggregation( aggregation ) {
    const res = AGGREGATION.includes( aggregation )
    if ( !res ) {
      console.error( 'Invalid aggregation: ', aggregation )
    }
    return res
  }

  isValidFilter( filter ) {
    const res = FILTER.includes( filter )
    if ( !res ) {
      console.error( 'Invalid filter: ', filter )
    }
    return res
  }

  validateSecret( secret ) {
    if ( !process.env.LOCAL && secret !== process.env.CUMULIO_SECRET ) {
      throw errors.unauthorizedError( 'Could not validate secret' )
    }
  }
}

module.exports = new Validation()
