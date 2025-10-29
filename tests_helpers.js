import random from 'slump'
import { EntryStream } from 'level-read-stream'
import bytewise from 'bytewise'

const bwEncode = bytewise.encode

// Reimplemented with EntryStream as the `level-concat-iterator` implementation
// with `concat(db.iterator)` was not returning anything
export async function getDbEntries (db, opts) {
  const entries = []
  return new Promise((resolve, reject) => {
    new EntryStream(db, opts)
      .on('data', function (data) {
        entries.push(data)
      })
      .on('close', function () {
        resolve(entries)
      })
      .on('error', reject)
  })
}

export async function getDbEntriesAfterDelay (db, delay, opts) {
  await wait(delay)
  return getDbEntries(db, opts)
}

function bufferEq (a, b) {
  if (a instanceof Buffer && b instanceof Buffer) {
    return a.toString('hex') === b.toString('hex')
  }
}

function isRange (range) {
  return range && (range.gt || range.lt || range.gte || range.lte)
}

function matchRange (range, buffer) {
  const target = buffer.toString('hex')
  let match = true

  if (range.gt) {
    match = match && target > range.gt.toString('hex')
  } else if (range.gte) {
    match = match && target >= range.gte.toString('hex')
  }

  if (range.lt) {
    match = match && target < range.lt.toString('hex')
  } else if (range.lte) {
    match = match && target <= range.lte.toString('hex')
  }

  return match
}

export function bwRange (prefix, resolution) {
  const now = Date.now()
  const min = new Date(resolution ? now - resolution : 0)
  const max = new Date(resolution ? now + resolution : 9999999999999)
  return {
    gte: bwEncode(prefix ? prefix.concat(min) : min),
    lte: bwEncode(prefix ? prefix.concat(max) : max)
  }
}

function formatRecord (key, value) {
  if (isRange(key)) {
    key.source = '[object KeyRange]'
  }
  if (isRange(value)) {
    value.source = '[object ValueRange]'
  }
  return '{' + (key.source || key) + ', ' + (value.source || value) + '}'
}

export function contains (entries, key, value) {
  for (let i = 0; i < entries.length; i++) {
    if (typeof key === 'string' && entries[i].key !== key) continue
    if (typeof value === 'string' && entries[i].value !== value) continue
    if (key instanceof RegExp && !key.test(entries[i].key)) continue
    if (value instanceof RegExp && !value.test(entries[i].value)) continue
    if (key instanceof Buffer && !bufferEq(key, entries[i].key)) continue
    if (value instanceof Buffer && !bufferEq(value, entries[i].value)) continue
    if (isRange(key) && !matchRange(key, entries[i].key)) continue
    if (isRange(value) && !matchRange(value, entries[i].value)) continue
    return true
  }
  throw new Error('does not contain ' + formatRecord(key, value))
}

export function randomPutBatch (length) {
  const batch = []
  const randomize = () => random.string({ enc: 'base58', length: 10 })
  for (let i = 0; i < length; ++i) {
    batch.push({ type: 'put', key: randomize(), value: randomize() })
  }
  return batch
}

export const wait = ms => new Promise(resolve => setTimeout(resolve, ms))

export function shouldNotBeCalled (res) {
  const err = new Error('function was expected not to be called')
  err.name = 'shouldNotBeCalled'
  err.message += ` (got: ${JSON.stringify(res)})`
  throw err
}

export function numberRange (min, max) {
  return Object.keys(new Array(max + 1).fill('')).slice(min).map(num => parseInt(num))
}
