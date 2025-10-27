import { createEncoding } from './encoding.js'
import AsyncLock from 'async-lock'
import { EntryStream } from 'level-read-stream'

function prefixKey (db, key) {
  return db._ttl.encoding.encode(db._ttl._prefixNs.concat(key))
}

function expiryKey (db, expiryDate, key) {
  return db._ttl.encoding.encode(db._ttl._expiryNs.concat(expiryDate, key))
}

function buildQuery (db) {
  const encode = db._ttl.encoding.encode
  const expiryNs = db._ttl._expiryNs
  return {
    keyEncoding: 'binary',
    valueEncoding: 'binary',
    gte: encode(expiryNs),
    lte: encode(expiryNs.concat(new Date()))
  }
}

function startTtl (db, checkFrequency) {
  db._ttl.intervalId = setInterval(function () {
    const batch = []
    const subBatch = []
    const sub = db._ttl.sub
    const query = buildQuery(db)
    const decode = db._ttl.encoding.decode

    db._ttl._checkInProgress = true
    const emitError = db.emit.bind(db, 'error')

    new EntryStream(sub || db, query)
      .on('data', function (data) {
        // the value is the key!
        const key = decode(data.value)
        // expiryKey that matches this query
        subBatch.push({ type: 'del', key: data.key })
        subBatch.push({ type: 'del', key: prefixKey(db, key) })
        // the actual data that should expire now!
        batch.push({ type: 'del', key: key })
      })
      .on('error', emitError)
      .on('end', function () {
        if (!batch.length) return

        if (sub) {
          sub.batch(subBatch, { keyEncoding: 'binary' }).catch(emitError)
          db._ttl.batch(batch, { keyEncoding: 'binary' }).catch(emitError)
        } else {
          db._ttl.batch(subBatch.concat(batch), { keyEncoding: 'binary' }).catch(emitError)
        }
      })
      .on('close', function () {
        db._ttl._checkInProgress = false
        if (db._ttl._stopAfterCheck) {
          stopTtl(db)
          db._ttl._stopAfterCheck = false
        }
      })
  }, checkFrequency)

  if (db._ttl.intervalId.unref) {
    db._ttl.intervalId.unref()
  }
}

function stopTtl (db) {
  // can't close a db while an interator is in progress
  // so if one is, defer
  if (db._ttl._checkInProgress) {
    db._ttl._stopAfterCheck = true
  } else {
    clearInterval(db._ttl.intervalId)
  }
}

async function ttlon (db, keys, ttl) {
  const expiryTime = new Date(Date.now() + ttl)
  const batch = []
  const sub = db._ttl.sub
  const batchFn = (sub ? sub.batch.bind(sub) : db._ttl.batch)
  const encode = db._ttl.encoding.encode

  await db._ttl._lock.acquire(keys, async function (release) {
    try {
      await ttloff(db, keys)
      keys.forEach(function (key) {
        batch.push({ type: 'put', key: expiryKey(db, expiryTime, key), value: encode(key) })
        batch.push({ type: 'put', key: prefixKey(db, key), value: encode(expiryTime) })
      })
      if (!batch.length) return release()

      await batchFn(batch, { keyEncoding: 'binary', valueEncoding: 'binary' })
    } catch (err) {
      db.emit('error', err)
    }
    release()
  })
}

async function ttloff (db, keys) {
  const batch = []
  const sub = db._ttl.sub
  const getFn = (sub ? sub.get.bind(sub) : db.get.bind(db))
  const batchFn = (sub ? sub.batch.bind(sub) : db._ttl.batch)
  const decode = db._ttl.encoding.decode
  try {
    await Promise.all(keys.map(async key => {
      const prefixedKey = prefixKey(db, key)
      try {
        // TODO: refactor with getMany
        const exp = await getFn(prefixedKey, { keyEncoding: 'binary', valueEncoding: 'binary' })
        if (exp) {
          batch.push({ type: 'del', key: expiryKey(db, decode(exp), key) })
          batch.push({ type: 'del', key: prefixedKey })
        }
      } catch (err) {
        if (err.code !== 'LEVEL_NOT_FOUND') throw err
      }
    }))
    if (!batch.length) return
    await batchFn(batch, { keyEncoding: 'binary', valueEncoding: 'binary' })
  } catch (err) {
    db.emit('error', err)
  }
}

function put (db, key, value, options = {}) {
  if (db._ttl.options.defaultTTL > 0 && !options.ttl && options.ttl !== 0) {
    options.ttl = db._ttl.options.defaultTTL
  }

  if (options.ttl > 0 && key != null && value != null) {
    return Promise.all([
      db._ttl.put.call(db, key, value, options),
      ttlon(db, [key], options.ttl)
    ])
  } else {
    return db._ttl.put.call(db, key, value, options)
  }
}

function setTtl (db, key, ttl) {
  if (ttl > 0 && key != null) {
    ttlon(db, [key], ttl)
  }
}

async function del (db, key, options) {
  if (key != null) {
    await ttloff(db, [key])
  }
  await db._ttl.del.call(db, key, options)
}

async function batch (db, arr, options = {}) {
  if (db._ttl.options.defaultTTL > 0 && !options.ttl && options.ttl !== 0) {
    options.ttl = db._ttl.options.defaultTTL
  }

  if (options.ttl > 0 && Array.isArray(arr)) {
    const on = []
    const off = []
    arr.forEach(function (entry) {
      if (!entry || entry.key == null) { return }
      if (entry.type === 'put' && entry.value != null) on.push(entry.key)
      if (entry.type === 'del') off.push(entry.key)
    })
    await Promise.all([
      on.length ? ttlon(db, on, options.ttl) : null,
      off.length ? ttloff(db, off) : null
    ])
  }

  return db._ttl.batch.call(db, arr, options)
}

async function close (db) {
  stopTtl(db)
  if (db._ttl && typeof db._ttl.close === 'function') {
    await db._ttl.close.call(db)
  }
}

function setup (db, options = {}) {
  if (db._ttl) return

  options = Object.assign({
    methodPrefix: '',
    namespace: options.sub ? '' : 'ttl',
    expiryNamespace: 'x',
    separator: '!',
    checkFrequency: 10000,
    defaultTTL: 0
  }, options)

  const _prefixNs = options.namespace ? [options.namespace] : []

  db._ttl = {
    put: db.put.bind(db),
    del: db.del.bind(db),
    batch: db.batch.bind(db),
    close: db.close.bind(db),
    sub: options.sub,
    options: options,
    encoding: createEncoding(options),
    _prefixNs: _prefixNs,
    _expiryNs: _prefixNs.concat(options.expiryNamespace),
    _lock: new AsyncLock()
  }

  db[options.methodPrefix + 'put'] = put.bind(null, db)
  db[options.methodPrefix + 'del'] = del.bind(null, db)
  db[options.methodPrefix + 'batch'] = batch.bind(null, db)
  db[options.methodPrefix + 'ttl'] = setTtl.bind(null, db)
  db[options.methodPrefix + 'stop'] = stopTtl.bind(null, db)
  // we must intercept close()
  db.close = close.bind(null, db)

  startTtl(db, options.checkFrequency)

  return db
}

export default setup
