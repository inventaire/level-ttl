/* eslint-env mocha */
import should from 'should'
import { MemoryLevel } from 'memory-level'
import ttl from './level-ttl.js'
import bytewise from 'bytewise'
import { bwRange, contains, getDbEntries, getDbEntriesAfterDelay, numberRange, randomPutBatch, shouldNotBeCalled, wait } from './tests_helpers.js'

const bwEncode = bytewise.encode
const level = opts => new MemoryLevel(opts)
const levelTtl = opts => ttl(level(opts), opts)

describe('level-ttl', () => {
  it('should work without options', () => {
    levelTtl()
  })

  it('should separate data and sublevel ttl meta data', async () => {
    const db = new MemoryLevel()
    const sub = db.sublevel('meta')
    const ttldb = ttl(db, { sub })
    const batch = randomPutBatch(5)
    await ttldb.batch(batch, { ttl: 10000 })
    const entries = await getDbEntries(db)
    batch.forEach(item => {
      contains(entries, '!meta!' + item.key, /\d{13}/)
      contains(entries, new RegExp('!meta!x!\\d{13}!' + item.key), item.key)
    })
  })

  it('should separate data and sublevel ttl meta data (custom ttlEncoding)', async () => {
    const db = new MemoryLevel({ keyEncoding: 'binary', valueEncoding: 'binary' })
    const sub = db.sublevel('meta')
    const ttldb = ttl(db, { sub, ttlEncoding: bytewise })
    const batch = randomPutBatch(5)
    function prefix (buf) {
      return Buffer.concat([Buffer.from('!meta!'), buf])
    }
    await ttldb.batch(batch, { ttl: 10000 })
    const entries = await getDbEntries(db)
    batch.forEach(item => {
      contains(entries, prefix(bwEncode([item.key])), bwRange())
      contains(entries, {
        gt: prefix(bwEncode(['x', new Date(0), item.key])),
        lt: prefix(bwEncode(['x', new Date(9999999999999), item.key]))
      }, bwEncode(item.key))
    })
  })

  it('should expire sublevel data properly', async () => {
    const db = new MemoryLevel()
    const sub = db.sublevel('meta')
    const ttldb = ttl(db, { checkFrequency: 25, sub })
    const batch = randomPutBatch(50)
    await ttldb.batch(batch, { ttl: 100 })
    const entries = await getDbEntriesAfterDelay(db, 200)
    entries.length.should.equal(0)
  })

  it('should expire sublevel data properly (custom ttlEncoding)', async () => {
    const db = new MemoryLevel()
    const sub = db.sublevel('meta')
    const ttldb = ttl(db, { checkFrequency: 25, sub, ttlEncoding: bytewise })
    const batch = randomPutBatch(50)
    await ttldb.batch(batch, { ttl: 100 })
    const entries = await getDbEntriesAfterDelay(db, 200)
    entries.length.should.equal(0)
  })
})

describe('put', () => {
  it('should throw on missing key', async () => {
    const db = levelTtl({ checkFrequency: 50 })
    try {
      // @ts-expect-error
      await db.put()
      shouldNotBeCalled()
    } catch (err) {
      err.message.should.equal('Key cannot be null or undefined')
      err.code.should.equal('LEVEL_INVALID_KEY')
    }
  })

  it('should put a single ttl entry', async () => {
    const db = levelTtl({ checkFrequency: 50 })
    await db.put('foo', 'foovalue')
    await db.put('bar', 'barvalue', { ttl: 100 })
    const entries = await getDbEntries(db)
    contains(entries, /!ttl!x!\d{13}!bar/, 'bar')
    contains(entries, '!ttl!bar', /\d{13}/)
    contains(entries, 'bar', 'barvalue')
    contains(entries, 'foo', 'foovalue')
  })

  it('should put a single ttl entry (custom ttlEncoding)', async () => {
    const db = levelTtl({ checkFrequency: 50, ttlEncoding: bytewise })
    await db.put('foo', 'foovalue')
    await db.put('bar', 'barvalue', { ttl: 100 })
    const entries = await getDbEntries(db, { keyEncoding: 'binary', valueEncoding: 'binary' })
    contains(entries, bwRange(['ttl', 'x']), bwEncode('bar'))
    contains(entries, bwEncode(['ttl', 'bar']), bwRange())
    contains(entries, Buffer.from('bar'), Buffer.from('barvalue'))
    contains(entries, Buffer.from('foo'), Buffer.from('foovalue'))
    const updatedEntries = await getDbEntriesAfterDelay(db, 150)
    updatedEntries.should.deepEqual([{ key: 'foo', value: 'foovalue' }])
  })

  it('should put multiple ttl entries', async () => {
    const db = levelTtl({ checkFrequency: 50 })
    async function expect (delay, keysCount) {
      const entries = await getDbEntriesAfterDelay(db, delay)
      entries.length.should.equal(1 + keysCount * 3)
      contains(entries, 'afoo', 'foovalue')
      if (keysCount >= 1) {
        contains(entries, 'bar1', 'barvalue1')
        contains(entries, /^!ttl!x!\d{13}!bar1$/, 'bar1')
        contains(entries, '!ttl!bar1', /^\d{13}$/)
      }
      if (keysCount >= 2) {
        contains(entries, 'bar2', 'barvalue2')
        contains(entries, /^!ttl!x!\d{13}!bar2$/, 'bar2')
        contains(entries, '!ttl!bar2', /^\d{13}$/)
      }
      if (keysCount >= 3) {
        contains(entries, 'bar3', 'barvalue3')
        contains(entries, /^!ttl!x!\d{13}!bar3$/, 'bar3')
        contains(entries, '!ttl!bar3', /^\d{13}$/)
      }
    }

    db.put('afoo', 'foovalue')
    db.put('bar1', 'barvalue1', { ttl: 400 })
    db.put('bar2', 'barvalue2', { ttl: 250 })
    db.put('bar3', 'barvalue3', { ttl: 100 })

    await Promise.all([
      expect(25, 3),
      expect(200, 2),
      expect(350, 1),
      expect(500, 0)
    ])
  })

  it('should put multiple ttl entries (custom ttlEncoding)', async () => {
    const db = levelTtl({ checkFrequency: 50, ttlEncoding: bytewise })
    async function expect (delay, keysCount) {
      const entries = await getDbEntriesAfterDelay(db, delay, { keyEncoding: 'binary', valueEncoding: 'binary' })
      entries.length.should.equal(1 + keysCount * 3)
      contains(entries, Buffer.from('afoo'), Buffer.from('foovalue'))
      if (keysCount >= 1) {
        contains(entries, Buffer.from('bar1'), Buffer.from('barvalue1'))
        contains(entries, bwRange(['ttl', 'x']), bwEncode('bar1'))
        contains(entries, bwEncode(['ttl', 'bar1']), bwRange())
      }
      if (keysCount >= 2) {
        contains(entries, Buffer.from('bar2'), Buffer.from('barvalue2'))
        contains(entries, bwRange(['ttl', 'x']), bwEncode('bar2'))
        contains(entries, bwEncode(['ttl', 'bar2']), bwRange())
      }
      if (keysCount >= 3) {
        contains(entries, Buffer.from('bar3'), Buffer.from('barvalue3'))
        contains(entries, bwRange(['ttl', 'x']), bwEncode('bar3'))
        contains(entries, bwEncode(['ttl', 'bar3']), bwRange())
      }
    }

    db.put('afoo', 'foovalue')
    db.put('bar1', 'barvalue1', { ttl: 400 })
    db.put('bar2', 'barvalue2', { ttl: 250 })
    db.put('bar3', 'barvalue3', { ttl: 100 })

    await Promise.all([
      expect(25, 3),
      expect(200, 2),
      expect(350, 1),
      expect(500, 0)
    ])
  })

  it('should prolong entry life with additional put', async () => {
    const db = levelTtl({ checkFrequency: 50 })
    await db.put('foo', 'foovalue')
    for (let i = 0; i <= 180; i += 20) {
      await db.put('bar', 'barvalue', { ttl: 250 })
      const entries = await getDbEntriesAfterDelay(db, 50)
      contains(entries, 'foo', 'foovalue')
      contains(entries, 'bar', 'barvalue')
      contains(entries, /!ttl!x!\d{13}!bar/, 'bar')
      contains(entries, '!ttl!bar', /\d{13}/)
    }
  })

  it('should prolong entry life with additional put (custom ttlEncoding)', async () => {
    const db = levelTtl({ checkFrequency: 50, ttlEncoding: bytewise })
    await db.put('foo', 'foovalue')
    for (let i = 0; i <= 180; i += 20) {
      await db.put('bar', 'barvalue', { ttl: 250 })
      const entries = await getDbEntriesAfterDelay(db, 50, { keyEncoding: 'binary', valueEncoding: 'binary' })
      contains(entries, Buffer.from('foo'), Buffer.from('foovalue'))
      contains(entries, Buffer.from('bar'), Buffer.from('barvalue'))
      contains(entries, bwRange(['ttl', 'x']), bwEncode('bar'))
      contains(entries, bwEncode(['ttl', 'bar']), bwRange())
    }
  })

  it('should not duplicate the TTL key when prolonging entry', async () => {
    const db = levelTtl({ checkFrequency: 50 })
    async function retest (delay) {
      await wait(delay)
      db.put('bar', 'barvalue', { ttl: 20 })
      const entries = await getDbEntriesAfterDelay(db, 50)
      const count = entries.filter(entry => {
        return /!ttl!x!\d{13}!bar/.exec(entry.key)
      }).length
      count.should.be.belowOrEqual(1)
    }
    db.put('foo', 'foovalue')
    await Promise.all(numberRange(0, 50).map(retest))
  })

  it('should put a single entry with default ttl set', async () => {
    const db = levelTtl({ checkFrequency: 50, defaultTTL: 75 })
    await basicPutTest(db, 175)
  })

  it('should put a single entry with default ttl set (custom ttlEncoding)', async () => {
    const db = levelTtl({ checkFrequency: 50, defaultTTL: 75, ttlEncoding: bytewise })
    await basicPutTest(db, 175)
  })

  it('should put a single entry with overridden ttl set', async () => {
    const db = levelTtl({ checkFrequency: 50, defaultTTL: 75 })
    await basicPutTest(db, 200, { ttl: 99 })
  })

  it('should put a single entry with overridden ttl set (custom ttlEncoding)', async () => {
    const db = levelTtl({ checkFrequency: 50, defaultTTL: 75, ttlEncoding: bytewise })
    await basicPutTest(db, 200, { ttl: 99 })
  })
})

async function basicPutTest (db, timeout, opts) {
  await db.put('foo', 'foovalue', opts)
  await wait(50)
  const res = await db.get('foo')
  res.should.equal('foovalue')
  await wait(timeout - 50)
  const res2 = await db.get('foo')
  should(res2).not.be.ok()
}

describe('del', () => {
  it('should throw on missing key', async () => {
    const db = levelTtl({ checkFrequency: 50 })
    try {
      // @ts-expect-error
      await db.del()
      shouldNotBeCalled()
    } catch (err) {
      err.message.should.equal('Key cannot be null or undefined')
      err.code.should.equal('LEVEL_INVALID_KEY')
    }
  })

  it('should remove both key and its ttl meta data', async () => {
    const db = levelTtl({ checkFrequency: 50 })
    db.put('foo', 'foovalue')
    db.put('bar', 'barvalue', { ttl: 10000 })

    const entries = await getDbEntriesAfterDelay(db, 150)
    contains(entries, 'foo', 'foovalue')
    contains(entries, 'bar', 'barvalue')
    contains(entries, /!ttl!x!\d{13}!bar/, 'bar')
    contains(entries, '!ttl!bar', /\d{13}/)

    setTimeout(() => db.del('bar'), 250)

    const updatedEntries = await getDbEntriesAfterDelay(db, 350)
    updatedEntries.should.deepEqual([
      { key: 'foo', value: 'foovalue' }
    ])
  })

  it('should remove both key and its ttl meta data (custom ttlEncoding)', async () => {
    const db = levelTtl({ checkFrequency: 50, keyEncoding: 'utf8', valueEncoding: 'json', ttlEncoding: bytewise })
    // @ts-expect-error
    db.put('foo', { v: 'foovalue' })
    db.put('bar', { v: 'barvalue' }, { ttl: 250 })

    const entries = await getDbEntriesAfterDelay(db, 50, { keyEncoding: 'binary', valueEncoding: 'binary' })
    contains(entries, Buffer.from('foo'), Buffer.from('{"v":"foovalue"}'))
    contains(entries, Buffer.from('bar'), Buffer.from('{"v":"barvalue"}'))
    contains(entries, bwRange(['ttl', 'x']), bwEncode('bar'))
    contains(entries, bwEncode(['ttl', 'bar']), bwRange())

    setTimeout(() => db.del('bar'), 175)

    const updatedEntries = await getDbEntriesAfterDelay(db, 350, { valueEncoding: 'utf8' })
    updatedEntries.should.deepEqual([
      { key: 'foo', value: '{"v":"foovalue"}' }
    ])
  })
})

describe('batch', () => {
  it('should batch-put multiple ttl entries', async () => {
    const db = levelTtl({ checkFrequency: 50 })
    async function expect (delay, keysCount) {
      const entries = await getDbEntriesAfterDelay(db, delay)
      entries.length.should.equal(1 + keysCount * 3)
      contains(entries, 'afoo', 'foovalue')
      if (keysCount >= 1) {
        contains(entries, 'bar1', 'barvalue1')
        contains(entries, /^!ttl!x!\d{13}!bar1$/, 'bar1')
        contains(entries, '!ttl!bar1', /^\d{13}$/)
      }
      if (keysCount >= 2) {
        contains(entries, 'bar2', 'barvalue2')
        contains(entries, /^!ttl!x!\d{13}!bar2$/, 'bar2')
        contains(entries, '!ttl!bar2', /^\d{13}$/)
      }
      if (keysCount >= 3) {
        contains(entries, 'bar3', 'barvalue3')
        contains(entries, /^!ttl!x!\d{13}!bar3$/, 'bar3')
        contains(entries, '!ttl!bar3', /^\d{13}$/)
      }
      if (keysCount >= 3) {
        contains(entries, 'bar4', 'barvalue4')
        contains(entries, /^!ttl!x!\d{13}!bar4$/, 'bar4')
        contains(entries, '!ttl!bar4', /^\d{13}$/)
      }
    }

    db.put('afoo', 'foovalue')
    db.batch([
      { type: 'put', key: 'bar1', value: 'barvalue1' },
      { type: 'put', key: 'bar2', value: 'barvalue2' }
    ], { ttl: 60 })
    db.batch([
      { type: 'put', key: 'bar3', value: 'barvalue3' },
      { type: 'put', key: 'bar4', value: 'barvalue4' }
    ], { ttl: 120 })

    await expect(20, 4)
  })

  it('should batch-put multiple ttl entries (custom ttlEncoding)', async () => {
    const db = levelTtl({ checkFrequency: 50, ttlEncoding: bytewise })
    async function expect (delay, keysCount) {
      const entries = await getDbEntriesAfterDelay(db, delay, { keyEncoding: 'binary', valueEncoding: 'binary' })
      entries.length.should.equal(1 + keysCount * 3)
      contains(entries, Buffer.from('afoo'), Buffer.from('foovalue'))
      if (keysCount >= 1) {
        contains(entries, Buffer.from('bar1'), Buffer.from('barvalue1'))
        contains(entries, bwRange(['ttl', 'x']), bwEncode('bar1'))
        contains(entries, bwEncode(['ttl', 'bar1']), bwRange())
      }
      if (keysCount >= 2) {
        contains(entries, Buffer.from('bar2'), Buffer.from('barvalue2'))
        contains(entries, bwRange(['ttl', 'x']), bwEncode('bar2'))
        contains(entries, bwEncode(['ttl', 'bar2']), bwRange())
      }
      if (keysCount >= 3) {
        contains(entries, Buffer.from('bar3'), Buffer.from('barvalue3'))
        contains(entries, bwRange(['ttl', 'x']), bwEncode('bar3'))
        contains(entries, bwEncode(['ttl', 'bar3']), bwRange())
      }
      if (keysCount >= 3) {
        contains(entries, Buffer.from('bar4'), Buffer.from('barvalue4'))
        contains(entries, bwRange(['ttl', 'x']), bwEncode('bar4'))
        contains(entries, bwEncode(['ttl', 'bar4']), bwRange())
      }
    }

    db.put('afoo', 'foovalue')
    db.batch([
      { type: 'put', key: 'bar1', value: 'barvalue1' },
      { type: 'put', key: 'bar2', value: 'barvalue2' }
    ], { ttl: 60 })
    db.batch([
      { type: 'put', key: 'bar3', value: 'barvalue3' },
      { type: 'put', key: 'bar4', value: 'barvalue4' }
    ], { ttl: 120 })

    await expect(20, 4)
  })

  it('should batch put with default ttl set', async () => {
    const db = levelTtl({ checkFrequency: 50, defaultTTL: 75 })
    await basicBatchPutTest(db, 175)
  })

  it('should batch put with default ttl set (custom ttlEncoding)', async () => {
    const db = levelTtl({ checkFrequency: 50, defaultTTL: 75, ttlEncoding: bytewise })
    await basicBatchPutTest(db, 175)
  })

  it('should batch put with overriden ttl set', async () => {
    const db = levelTtl({ checkFrequency: 50, defaultTTL: 75 })
    await basicBatchPutTest(db, 200, { ttl: 99 })
  })

  it('should batch put with overriden ttl set (custom ttlEncoding)', async () => {
    const db = levelTtl({ checkFrequency: 50, defaultTTL: 75, ttlEncoding: bytewise })
    await basicBatchPutTest(db, 200, { ttl: 99 })
  })
})

async function basicBatchPutTest (db, timeout, opts) {
  await db.batch([
    { type: 'put', key: 'foo', value: 'foovalue' },
    { type: 'put', key: 'bar', value: 'barvalue' }
  ], opts)
  await wait(50)
  const res = await db.getMany(['foo', 'bar'])
  res.should.deepEqual(['foovalue', 'barvalue'])
  await wait(timeout - 50)
  const res2 = await db.getMany(['foo', 'bar'])
  res2.should.deepEqual([undefined, undefined])
}

describe('ttl', () => {
  it('should prolong entry life', async () => {
    const db = levelTtl({ checkFrequency: 50 })
    db.put('foo', 'foovalue')
    db.put('bar', 'barvalue')
    for (let i = 0; i <= 180; i += 20) {
      await db.ttl('bar', 250)
      const entries = await getDbEntriesAfterDelay(db, 25)
      contains(entries, 'foo', 'foovalue')
      contains(entries, 'bar', 'barvalue')
      contains(entries, /!ttl!x!\d{13}!bar/, 'bar')
      contains(entries, '!ttl!bar', /\d{13}/)
    }
  })

  it('should prolong entry life (custom ttlEncoding)', async () => {
    const db = levelTtl({ checkFrequency: 50, ttlEncoding: bytewise })
    db.put('foo', 'foovalue')
    db.put('bar', 'barvalue')
    for (let i = 0; i <= 180; i += 20) {
      await db.ttl('bar', 250)
      const entries = await getDbEntriesAfterDelay(db, 25, { keyEncoding: 'binary', valueEncoding: 'binary' })
      contains(entries, Buffer.from('bar'), Buffer.from('barvalue'))
      contains(entries, Buffer.from('foo'), Buffer.from('foovalue'))
      contains(entries, bwRange(['ttl', 'x']), bwEncode('bar'))
      contains(entries, bwEncode(['ttl', 'bar']), bwRange())
    }
  })
})

describe('stop', () => {
  it('should stop interval and not hold process up', async () => {
    let intervals = 0
    const _setInterval = global.setInterval
    const _clearInterval = global.clearInterval

    global.setInterval = function () {
      intervals++
      return _setInterval.apply(global, arguments)
    }

    global.clearInterval = function () {
      intervals--
      return _clearInterval.apply(global, arguments)
    }

    const db = levelTtl({ checkFrequency: 50 })
    intervals.should.equal(1)
    await db.put('foo', 'bar1', { ttl: 25 })
    await wait(40)
    const res = await db.get('foo')
    should(res).equal('bar1')
    await wait(40)
    const res2 = await db.get('foo')
    // Getting a missing key doesn't throw an error anymore,
    // see https://github.com/Level/abstract-level/blob/main/UPGRADING.md#12-not-found
    should(res2).not.be.ok()
    await wait(40)
    await db.stop()
    await db._ttl.close()
    global.setInterval = _setInterval
    global.clearInterval = _clearInterval
    intervals.should.equal(0)
  })
})
