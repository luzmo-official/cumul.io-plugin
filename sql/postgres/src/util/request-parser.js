const util = require( './util' )
const errors = require( './errors' )
const url = require( 'url' )

const DEFAULT_PORT = 5432
const DEFAULT_SCHEMA = 'public'

class RequestParser {
  getAuthorizeDetails( headers, body ) {
    const parsedHost = this.parseHost( body.host )
    return {
      database: parsedHost.database,
      host: parsedHost.host,
      port: parsedHost.port,
      protocol: parsedHost.protocol,
      key: body.key || '',
      token: body.token || '',
      secret: headers[ 'x-secret' ],
      body: body
    }
  }

  getCommonDetails( headers, body ) {
    const parsedHost = this.parseHost( headers[ 'x-host' ])
    const res = {
      database: parsedHost.database,
      host: parsedHost.host,
      port: parsedHost.port,
      protocol: parsedHost.protocol,
      key: headers[ 'x-key' ] || '',
      token: headers[ 'x-token' ] || '',
      secret: headers[ 'x-secret' ],
      body: body
    }
    return res
  }

  parseHost( host ) {
    let httpshost = host
    if ( host.startsWith( 'http://' )) {
      throw errors.noHttps()
    } else if ( !host.startsWith( 'https' )) {
      // default to http if no http or https was provided
      httpshost = 'https://' + host
    }
    const parsed = new url.URL( httpshost )
    const database = parsed.pathname.replace( /^\/+/g, '' )
    return { protocol: parsed.protocol || 'https', host: parsed.hostname, port: parsed.port || DEFAULT_PORT, database: database || DEFAULT_SCHEMA }
  }

  getDetails( headers, body, endpoint ) {
    if ( endpoint === 'authorize' ) {
      return this.getAuthorizeDetails( headers, body )
    } else {
      return this.getCommonDetails( headers, body )
    }
  }

  parseBody( body ) {
    if ( typeof body === 'string' ) {
      return JSON.parse( body )
    } else {
      return body
    }
  }

  parse( request, endpoint ) {
    try {
      const headers = util.toLowerCaseMap( request.headers )
      const body = this.parseBody( request.body )
      const details = this.getDetails( headers, body, endpoint )
      return details
    } catch ( err ) {
      throw errors.parsingError( err )
    }
  }
}

module.exports = new RequestParser()
