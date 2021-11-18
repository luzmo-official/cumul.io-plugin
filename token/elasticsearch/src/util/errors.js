class Errors {
  toError (err) {
    console.log('original error: ', err)
    if (err.type && err.type.code && err.type.description) return err
    if (err.name && err.name === 'RequestError') {
      if (err.code === 'ECONNREFUSED') { return this.badRequest('The connection was refused by the server. Are you sure your host is up and accessible? Did you provide the correct port to connect to?') }
      if (err.code === 'EPROTO' && err.stack.includes('SSL')) { return this.badRequest('There was an error while setting up an SSL connection. Are you sure SSL is enabled? Alternatively use a http url.') }
      if (err.code === 'ENOTFOUND') { return this.badRequest('The host could not be found. Are you sure you provided the correct hostname?') }
    }
    if (err.name && err.name === 'HTTPError') {
      if (err.body && err.body.includes('unable to authenticate user')) { return this.unauthorizedError('We were unable to authenticate with the ElasticSearch cluster. Are you sure you have provided a valid username/password combination?') }
    }
    return this.unexpectedError()
  }

  unexpectedError (err) {
    const message = err || 'Oops! something went wrong.'
    return {
      type: {
        code: 500,
        description: 'Internal Server Error'
      },
      message: message
    }
  }

  parsingError (err) {
    const message = err || 'Oops! something went wrong.'
    return {
      type: {
        code: 500,
        description: 'Internal Server Error'
      },
      message: message
    }
  }

  unauthorizedError (err) {
    const message = err || 'Unauthorized'
    return {
      type: {
        code: 401,
        description: 'Unauthorized'
      },
      message: message
    }
  }

  badRequest (err) {
    const message = err || 'Bad Request'
    return {
      type: {
        code: 400,
        description: 'Bad Request'
      },
      message: message
    }
  }

  notfound (err) {
    const message = err || 'Table or Database not found'
    return {
      type: {
        code: 404,
        description: 'Not Found'
      },
      message: message
    }
  }

  noHttps () {
    return {
      type: {
        code: 426,
        description: 'No HTTPS'
      },
      message: 'Only HTTPS plugins are allowed'
    }
  }
}

module.exports = new Errors()
