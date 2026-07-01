/**
 * HistoricalCOTStore — COT historical prices and positioning metadata only.
 * Never used as live price.
 */

import { resolveMarketBlock } from '../../charts/marketBlockResolve.js'

const COT_3Y_PATH = '/data/cot_3y_series_latest.json'

const _listeners = new Set()
let _doc = null
let _loading = false
let _error = null
let _fetchUrl = COT_3Y_PATH
let _promise = null
let _subscriberCount = 0
let _snapshotCache = null
let _snapshotCacheKey = ''

function emit() {
  _snapshotCache = null
  _snapshotCacheKey = ''
  for (const fn of _listeners) fn()
}

function prefetch() {
  if (_doc) return Promise.resolve(_doc)
  if (_promise) return _promise

  _loading = true
  _error = null
  const url = `${COT_3Y_PATH}?v=${encodeURIComponent(String(Date.now()))}`
  _fetchUrl = url
  emit()

  _promise = fetch(url, { cache: 'no-store' })
    .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
    .then((d) => {
      _doc = d && typeof d === 'object' ? d : null
      _loading = false
      _error = _doc ? null : new Error('Empty cot_3y payload')
      emit()
      return _doc
    })
    .catch((e) => {
      _doc = null
      _loading = false
      _error = e instanceof Error ? e : new Error(String(e))
      emit()
      return null
    })

  return _promise
}

export const HistoricalCOTStore = {
  STORE_NAME: 'HistoricalCOTStore',

  subscribe(listener) {
    _listeners.add(listener)
    _subscriberCount += 1
    if (_subscriberCount === 1) prefetch()
    return () => {
      _listeners.delete(listener)
      _subscriberCount = Math.max(0, _subscriberCount - 1)
    }
  },

  getSnapshot() {
    const key = `${_doc?.generated_at ?? ''}|${_loading}|${_error?.message ?? ''}|${_subscriberCount}|${_fetchUrl}`
    if (_snapshotCache && _snapshotCacheKey === key) return _snapshotCache
    _snapshotCacheKey = key
    _snapshotCache = {
      doc: _doc,
      loading: _loading,
      loaded: _doc != null,
      errored: Boolean(_error),
      error: _error,
      fetchUrl: _fetchUrl,
      generatedAt: _doc?.generated_at ?? null,
      subscriberCount: _subscriberCount,
    }
    return _snapshotCache
  },

  getBlock(marketId) {
    if (!marketId || !_doc) return null
    return resolveMarketBlock(_doc, marketId)
  },

  /** COT series rows with historical price field. */
  getSeries(marketId) {
    const block = this.getBlock(marketId)
    return block?.series ?? block?.rows ?? []
  },

  /** Historical COT close at a given COT week date label. */
  getHistoricalCloseAtDate(marketId, cotDate) {
    if (!cotDate) return null
    const series = this.getSeries(marketId)
    const hit = series.find((r) => r.date === cotDate || r.label === cotDate)
    const price = hit?.price
    return typeof price === 'number' && Number.isFinite(price) ? price : null
  },

  async refresh() {
    _doc = null
    _promise = null
    await prefetch()
  },
}

/** Back-compat for cot3ySeriesStore consumers */
export function getCot3ySnapshot() {
  return HistoricalCOTStore.getSnapshot()
}

export function subscribeCot3ySeries(listener) {
  return HistoricalCOTStore.subscribe(listener)
}

export function prefetchCot3ySeries() {
  return prefetch()
}

export function invalidateCot3ySeriesCache() {
  _doc = null
  _loading = false
  _error = null
  _promise = null
  _fetchUrl = COT_3Y_PATH
  emit()
}

export const COT_3Y_PATH_EXPORT = COT_3Y_PATH
