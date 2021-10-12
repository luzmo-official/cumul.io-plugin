class Errors {
  unexpectedError( err ) {
    console.error( 'unexpectedError', err )
    return {
      statusCode: 500,
      body: 'Oops! something went wrong.'
    }
  }

  timeoutError( err ) {
    console.error( 'timeoutError', err )
    return {
      statusCode: 504,
      body: 'Canceling operation due to timeout'
    }
  }

  parsingError( err ) {
    console.error( 'parsingError', err )
    return {
      statusCode: 500,
      body: 'Oops! something went wrong.'
    }
  }

  unauthorizedError( err ) {
    console.error( 'unauthorizedError', err )
    return {
      statusCode: 401,
      body: 'Unauthorized'
    }
  }

  notfound( err ) {
    console.error( 'notfound', err )
    return {
      statusCode: 404,
      body: 'Table or Database not found'
    }
  }

  noHttps() {
    return {
      statusCode: 426,
      body: 'Only https plugins allowed'
    }
  }
}

module.exports = new Errors()
