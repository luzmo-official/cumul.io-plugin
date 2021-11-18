const util = require('./util')
const errors = require('./errors')
const url = require('url')

class RequestParser {
  getDetails (headers, body, endpoint) {
    let host = endpoint === 'authorize' ? body.host : headers['x-host']
    const res = {}

    // OAuth2 auth mode
    try {
      host = JSON.parse(host)
      if (typeof host !== 'string') {
        res.oauth2_host = host.oauth2_host
        host = host.host
      }
    } catch (e) {
    }

    const parsedHost = this.parseHost(host)
    res.database = parsedHost.database
    res.host = parsedHost.host
    res.port = parsedHost.port
    res.protocol = parsedHost.protocol
    res.key = endpoint === 'authorize' ? body.key : headers['x-key'] || ''
    res.token = endpoint === 'authorize' ? body.token : headers['x-token'] || ''
    res.secret = headers['x-secret']
    res.body = body
    return res
  }

  parseHost (host) {
    if (!(process.env.LOCAL)) {
      if (host.startsWith('http://')) { throw errors.noHttps() } else if (!host.startsWith('https')) {
        // default to https if no http or https was provided
        host = 'https://' + host
      }
    } else {
      if (!host.startsWith('http')) { host = 'http://' + host }
    }
    const parsed = new url.URL(host)
    // remove leading slash
    const database = parsed.pathname.replace(/^\/+/g, '')
    return { protocol: parsed.protocol || 'https', host: parsed.hostname, port: parsed.port || 443, database: database || 'default' }
  }

  parseBody (body) {
    if (typeof body === 'string') { return JSON.parse(body) } else { return body }
  }

  parse (request, endpoint) {
    try {
      const headers = util.toLowerCaseMap(request.headers)
      const body = this.parseBody(request.body)
      const details = this.getDetails(headers, body, endpoint)
      return details
    } catch (err) {
      throw errors.parsingError(err)
    }
  }
}

module.exports = new RequestParser()
