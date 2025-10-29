import type { AbstractLevel, AbstractPutOptions, AbstractBatchOptions } from 'abstract-level'
import type { Encoding } from 'level-transcoder'

export interface LevelTtlOptions {
  defaultTTL: number
  checkFrequency: number
  ttlEncoding?: Encoding
  sub?: AbstractLevel
  namespace: string
  methodPrefix: string
  expiryNamespace: string
  separator: string
}

export interface LevelTtlOpsExtraOptions {
  ttl?: number
}

export interface LevelTtlPutOptions <K, V> extends AbstractPutOptions <K, V>, LevelTtlOpsExtraOptions {}

export interface LevelTtlBatchOptions <K, V> extends AbstractBatchOptions <K, V>, LevelTtlOpsExtraOptions {}

export interface _TTL extends Pick<AbstractLevel, 'put' | 'del' | 'batch' | 'close'> {
  sub?: AbstractLevel
  options: LevelTtlOptions
  encoding: Encoding
  _prefixNs: string[]
  _expiryNs: string[]
  _lock: AsyncLock
}

declare function LevelTTL <DB extends AbstractLevel> (db: DB, options: Partial<LevelTtlOptions>): DB & {
  put: <K = string, V = string> (key: K, value: V, options: LevelTtlPutOptions) => Promise<void>
  batch: <K = string, V = string> (operations: Array<AbstractBatchOperation<typeof this, K, V>>, options: LevelTtlBatchOptions) => Promise<void>
  ttl: <K = string> (key: K, delay: number) => Promise<void>
  stop: () => void
  _ttl: _TTL
}

export default LevelTTL
