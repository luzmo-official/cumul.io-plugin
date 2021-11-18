const Memoizer = require('./lib/memoizer')
const elasticsearch = require('./lib/elasticsearch')
const queryGenerator = require('./plugin/query-generator')
const resultsGenerator = require('./plugin/results-generator')

const validation = require('./util/validation')
const requestParser = require('./util/request-parser')
const timeouts = require('./util/timeouts')
const errors = require('./util/errors')

class Controller {
  constructor () {
    this.authorize = this.authorize.bind(this)
    this.datasets = this.datasets.bind(this)
    this.query = this.query.bind(this)
    this.schemas = new Memoizer(1000)
  }

  async status (request) {
    return {
      statusCode: 200,
      body: 'healthy'
    }
  }

  async authorize (request) {
    // { database, host, port, auth, secret }
    const details = requestParser.parse(request, 'authorize')
    validation.validateSecret(details.secret)
    try {
      const client = await elasticsearch.getClient(details)
      await elasticsearch.ping(client, timeouts.authorize)
      return {
        statusCode: 200,
        body: 'Success'
      }
    } catch (err) {
      throw errors.toError(err)
    }
  }

  getMemoizeKey (details) {
    return JSON.stringify({ key: details.key, token: details.token, host: details.host })
  }

  async datasets (request) {
    // { database, host, port, auth, secret, body}
    const details = requestParser.parse(request, 'datasets')
    validation.validateSecret(details.secret)
    try {
      const client = await elasticsearch.getClient(details)
      const schema = await this.schemas.force(this.getMemoizeKey(details), elasticsearch.getSchema, client, timeouts.datasets)
      const datasets = resultsGenerator.generateDatasets(schema.indexesToColumns)
      return {
        statusCode: 200,
        body: datasets
      }
    } catch (err) {
      throw errors.toError(err)
    }
  }

  async query (request) {
    const details = requestParser.parse(request, 'query')
    validation.validateSecret(details.secret)
    const columns = details.body.columns
    const filters = details.body.filters
    const index = details.body.id
    const pushdown = details.body.options ? details.body.options.pushdown : false
    const client = await elasticsearch.getClient(details)
    let schema = await this.schemas.memoize(this.getMemoizeKey(details), elasticsearch.getSchema, client, timeouts.datasets)
    const majorVersion = parseInt(client.transport._config.apiVersion.split('.')[0])
    let query

    try {
      query = await this._executeQuery(details, majorVersion, columns, filters, index, schema, pushdown)
    } catch (e) {
      if (e?.type?.code !== 400) { throw e }
      // In case of Bad Request, force a schema update and retry once
      console.log('Received error', e, '-- forcing schema refresh')
      schema = await this.schemas.force(this.getMemoizeKey(details), elasticsearch.getSchema, client, timeouts.datasets)
      query = await this._executeQuery(details, majorVersion, columns, filters, index, schema, pushdown)
    }

    console.log(JSON.stringify(query))
    const stream = await elasticsearch.queryStreaming(client, query, pushdown)
    const transformedStream = resultsGenerator.transformStream(query, stream, index, columns, schema, pushdown)
    return {
      statusCode: 200,
      stream: transformedStream
    }
  }

  async _executeQuery (details, majorVersion, columns, filters, index, schema, pushdown) {
    return queryGenerator.generateQuery(majorVersion, columns, filters, index, schema, pushdown)
  }
}

module.exports = new Controller()
