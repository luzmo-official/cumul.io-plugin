const hash = require('object-hash');
// 5 minutes (min * secs * ms)
const TTL = 5 * 60 * 1000;

class SchemaCache {
  constructor() {
    this.cache = {};
    this.storeDatasets = this.storeDatasets.bind(this);
    this.getDatasets = this.getDatasets.bind(this);
  }

  // Store in cache
  async storeDatasets(details, datasetHashMap) {
    const schemaHash = hash({
      database: details.database,
      host: details.host,
      key: details.key,
      token: details.token,
      port: details.port,
    });
    this.cache[schemaHash] = {
      timestamp: new Date().getTime(),
      data: datasetHashMap,
    };
  }

  // Fetch the datasets from the cache
  async getDatasets(details) {
    const schemaHash = hash({
      database: details.database,
      host: details.host,
      key: details.key,
      token: details.token,
      port: details.port,
    });
    if (
      !this.cache[schemaHash] ||
      new Date().getTime() - this.cache[schemaHash].timestamp > TTL
    ) {
      delete this.cache[schemaHash];
      return false;
    }
    return this.cache[schemaHash].data;
  }
}

module.exports = new SchemaCache();
