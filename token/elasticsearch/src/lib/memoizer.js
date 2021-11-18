const LRU = require('lru-cache')

class Memoizer {
  constructor (max, maxAge, disposeFun, evictionFun) {
    this.cache = new LRU({
      max: max,
      dispose: disposeFun,
      maxAge: maxAge,
      updateAgeOnGet: maxAge > 0 ? true : undefined
    })
    this.evictionFun = evictionFun
  }

  async force (key, promiseFun, ...args) {
    // Run the promise with the given arguments
    const promise = promiseFun.apply(null, args)
    // If the cached promise rejects, remove it from the cache so we don't cache errors
    // Rethrow the error so the subsequent error handlers can run
      .catch(error => {
        this.cache.del(key)
        throw error
      })
    // Put the promise itself in cache, so subsequent requests can attach to the same promise
    this.cache.set(key, promise)
    return promise
  }

  async memoize (key, promiseFun, ...args) {
    if (this.cache.has(key) && (!this.evictionFun || !(await this.evictionFun(this.cache.get(key))))) { return this.cache.get(key) }
    return this.force(key, promiseFun, ...args)
  }
}

module.exports = Memoizer
