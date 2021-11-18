const elasticsearch = require('elasticsearch')
const HttpConnector = require('elasticsearch/src/lib/connectors/http')
const Stream = require('stream')
const got = require('got')
const util = require('util')

const typeMapping = require('./../plugin/type-mapping')
const utils = require('./../util/util.js')
const timeouts = require('./../util/timeouts')

const MAX_PER_BATCH = 10000

class ElasticSearch {
  static async getClient (details) {
    return getClient(details)
  }

  static async ping (client, timeout) {
    return client.ping({
      requestTimeout: timeout
    })
  }

  static async queryStreaming (client, query, pushdown) {
    const stream = new Stream.PassThrough({ objectMode: true })
    if (pushdown) {
      // specific case where pushdown but no group by's
      if (utils.isEmpty(query.body.aggs) || utils.isEmpty(query.body.aggs.all) || utils.isEmpty(query.body.aggs.all.composite)) { queryNoGroupBys(client, query, stream) } else { queryBatchesPushdown(client, query, stream) }
    } else { queryBatchesNonPushdown(client, query, stream) }

    return stream
  }

  static async getSchema (client, timeout) {
    const response = await client.indices.getMapping()
    let aliases
    try {
      aliases = await client.indices.getAlias()
    } catch (e) {
      // Server does not have Aliases support yet
    }
    const indexesToColumns = {}
    const indexToOriginalName = {}
    Object.keys(response)
      .forEach((indexName) => {
        if (!isExposedIndex(indexName)) { return }

        const mappings = response[indexName].mappings
        // Take into account older versions.
        let properties = mappings._doc ? mappings._doc.properties : mappings.properties
        if (utils.isEmpty(properties) && Object.keys(mappings).length > 0) { properties = mappings[Object.keys(mappings)[0]].properties }

        if (!utils.isEmpty(properties)) {
          indexToOriginalName[indexName.toLowerCase()] = indexName
          if (!indexesToColumns[indexName.toLowerCase()]) { indexesToColumns[indexName.toLowerCase()] = {} }

          recursiveAddProperties(indexName, properties, indexesToColumns, '')
        }
        // Also enter entries for aliases
        if (!utils.isEmpty(aliases) && !utils.isEmpty(aliases[indexName]) && !utils.isEmpty(aliases[indexName].aliases)) {
          Object.keys(aliases[indexName].aliases).forEach(alias => {
            indexToOriginalName[alias.toLowerCase()] = alias
            if (utils.isEmpty(indexesToColumns[alias.toLowerCase()])) { indexesToColumns[alias.toLowerCase()] = {} }
            // Make sure all columns appear in alias even though schema might differ between indices
            Object.keys(indexesToColumns[indexName.toLowerCase()]).forEach(columnId => {
              indexesToColumns[alias.toLowerCase()][columnId] = indexesToColumns[indexName.toLowerCase()][columnId]
            })
          })
        }
      })

    return {
      indexesToColumns: indexesToColumns,
      indexToOriginalName: indexToOriginalName
    }
  }
}

const isExposedIndex = (indexName) => {
  return !(indexName.startsWith('apm-') || indexName === 'index' || indexName.indexOf('.') === 0)
}

const recursiveAddProperties = (indexName, properties, indexesToColumns, prefix) => {
  return Object.keys(properties).forEach((n) => {
    const columnName = prefix.length === 0 ? n : prefix + '.' + n
    const props = properties[n]
    if (utils.isEmpty(props.properties)) {
      if (!typeMapping.isFilteredType(props.type)) {
        indexesToColumns[indexName.toLowerCase()][columnName.toLowerCase()] = {
          columnName: columnName,
          dbType: props.type,
          hasKeyword: utils.getRecursiveOrFalse(props, ['fields', 'keyword', 'type']) === 'keyword',
          type: typeMapping.toCumulio(props.type)
        }
      }
    } else { recursiveAddProperties(indexName, properties[n].properties, indexesToColumns, columnName) }
  })
}

const getClient = async (details) => {
  let client
  if (details.oauth2_host) {
    // Get an OAuth2 token
    const token = await getOAuth2Token(details)
    const esInfo = await got(getHost(details), { headers: { authorization: `Bearer ${token}`, 'user-agent': 'Cumul.io Plugin Agent' }, method: 'GET', timeout: 10000, retry: { retries: 0 } })
    const versionSeparated = JSON.parse(esInfo.body).version.number.split('.')
    // Elasticsearch JS library only supports latest versions for ES 6.x and 5.x.
    // There are no massive breaking changes from previous versions though - so hack the version number so the connection succeeds
    switch (versionSeparated[0]) {
      case '6':
        versionSeparated[1] = '8'
        break
      case '5':
        versionSeparated[1] = '6'
        break
    }

    // Create a client, but override the connectionClass to set Bearer authorization
    // See https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/16.x/extending_core_components.html
    function MyHttpConnector (host, config) {
      HttpConnector.call(this, host, config)
    }
    util.inherits(MyHttpConnector, HttpConnector)
    MyHttpConnector.prototype.makeReqParams = function (params) {
      params = params || {}
      params.headers.authorization = `Bearer ${token}`
      return HttpConnector.prototype.makeReqParams.call(this, params)
    }
    client = new elasticsearch.Client({
      host: getHost(details),
      apiVersion: `${versionSeparated[0]}.${versionSeparated[0] === '7' ? 'x' : versionSeparated[1]}`,
      connectionClass: MyHttpConnector,
      requestTimeout: 120000
    })
  } else {
    const esInfo = await got(getHost(details), { headers: { 'user-agent': 'Cumul.io Plugin Agent' }, method: 'GET', timeout: 10000, retry: { retries: 0 } })
    const versionSeparated = JSON.parse(esInfo.body).version.number.split('.')
    // Elasticsearch JS library only supports latest versions for ES 6.x and 5.x.
    // There are no massive breaking changes from previous versions though - so hack the version number so the connection succeeds
    switch (versionSeparated[0]) {
      case '6':
        versionSeparated[1] = '8'
        break
      case '5':
        versionSeparated[1] = '6'
        break
    }
    client = new elasticsearch.Client({
      host: getHost(details),
      apiVersion: `${versionSeparated[0]}.${versionSeparated[0] === '7' ? 'x' : versionSeparated[1]}`,
      requestTimeout: 120000
    })
  }
  return client
}

const getOAuth2Token = async (details) => {
  const result = await got(details.oauth2_host, {
    headers: {
      authorization: `Basic ${getBasicAuthCreds(details)}`,
      'user-agent': 'Cumul.io Plugin Agent'
    },
    json: { grant_type: 'client_credentials' },
    method: 'POST',
    timeout: 90000,
    retry: { retries: 0 }
  })
  return result.body.access_token
}

const getBasicAuthCreds = (details) => {
  return Buffer.from(details.key + ':' + details.token).toString('base64')
}

const getHost = (details) => {
  if (details.oauth2_host) { return `${details.protocol}//${details.host.toString()}:${details.port}` } else {
    const key = utils.isEmpty(details.key) ? '' : encodeURIComponent(details.key.toString())
    const token = utils.isEmpty(details.token) ? '' : encodeURIComponent(details.token.toString())
    const authHeader = key !== '' && key !== 'null' && token !== '' && token !== 'null' ? `${key}:${token}@` : ''
    return `${details.protocol}//${authHeader}${details.host.toString()}:${details.port}`
  }
}

const queryNoGroupBys = async (client, query, stream) => {
  const queryRes = await client.search(query)
  if (utils.isEmpty(queryRes.aggregations)) { queryRes.aggregations = { count: queryRes.hits.total } } else { queryRes.aggregations.count = queryRes.hits.total }

  stream.push(queryRes.aggregations)
  // there is only one row with this query since it
  // is the function that is chosen when there are no group bys.
  stream.push(null)
}

const queryBatchesPushdown = async (client, query, stream) => {
  query.body.aggs.all.composite.size = MAX_PER_BATCH
  let queryRes = await client.search(query)
  let after = queryRes.aggregations.all.after_key // after is ES's continuation for composite aggregations

  if (!after && queryRes.aggregations.all.buckets.length > 0) { after = queryRes.aggregations.all.buckets[queryRes.aggregations.all.buckets.length - 1].key }

  while (!utils.isEmpty(after) && queryRes.aggregations.all.buckets.length > 0) {
    queryRes.aggregations.all.buckets.forEach((bucket) => {
      stream.push(bucket)
    })
    after = queryRes.aggregations.all.after_key // after is ES's continuation for composite aggregations
    query.body.aggs.all.composite.after = after
    queryRes = await client.search(query)
  }
  stream.push(null)
}

const queryBatchesNonPushdown = async (client, query, stream) => {
  const scrollSecondsStr = (timeouts.queryPage / 1000) + 's'
  query.scroll = scrollSecondsStr
  query.body.size = MAX_PER_BATCH
  let results = await client.search(query)
  while (results.hits.hits.length > 0) {
    results.hits.hits.forEach((entity) => {
      stream.push(entity)
    })
    results = await client.scroll({
      scrollId: results._scroll_id, // scrollId is ES's continuation id.
      scroll: scrollSecondsStr
    })
  }
  stream.push(null)
}

module.exports = ElasticSearch
