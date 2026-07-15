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

// Auto-refresh: detect a newly published cot_3y file while a workstation is open,
// so the user never needs Ctrl+F5. We poll a cheap HEAD (etag/last-modified) and
// only re-download the full JSON when the published version actually changed.
const POLL_INTERVAL_MS = 45000
let _polledTag = null
let _pollTimer = null
let _pollInFlight = false

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

async function checkForNewVersion() {
  if (_pollInFlight || _subscriberCount === 0) return
  _pollInFlight = true
  try {
    let tag = null
    try {
      const head = await fetch(COT_3Y_PATH, { method: 'HEAD', cache: 'no-store' })
      if (head.ok) tag = head.headers.get('etag') || head.headers.get('last-modified')
    } catch {
      tag = null
    }

    if (tag) {
      if (_polledTag == null) {
        _polledTag = tag // establish baseline, do not refetch on first observation
      } else if (tag !== _polledTag) {
        _polledTag = tag
        if (_doc) await HistoricalCOTStore.refresh()
      }
      return
    }

    // Fallback for servers without HEAD/etag support: compare generated_at via GET.
    try {
      const res = await fetch(`${COT_3Y_PATH}?probe=${Date.now()}`, { cache: 'no-store' })
      if (!res.ok) return
      const next = await res.json()
      const nextGen = next?.generated_at ?? null
      const curGen = _doc?.generated_at ?? null
      if (nextGen && nextGen !== curGen && next && typeof next === 'object') {
        _doc = next
        _loading = false
        _error = null
        emit()
      }
    } catch {
      /* ignore transient poll errors */
    }
  } finally {
    _pollInFlight = false
  }
}

function startPolling() {
  if (_pollTimer != null || typeof window === 'undefined') return
  _pollTimer = window.setInterval(checkForNewVersion, POLL_INTERVAL_MS)
}

function stopPolling() {
  if (_pollTimer != null && typeof window !== 'undefined') {
    window.clearInterval(_pollTimer)
  }
  _pollTimer = null
  _polledTag = null
}

export const HistoricalCOTStore = {
  STORE_NAME: 'HistoricalCOTStore',

  subscribe(listener) {
    _listeners.add(listener)
    _subscriberCount += 1
    if (_subscriberCount === 1) {
      prefetch()
      startPolling()
    }
    return () => {
      _listeners.delete(listener)
      _subscriberCount = Math.max(0, _subscriberCount - 1)
      if (_subscriberCount === 0) stopPolling()
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

/**
 * Manual "Reload latest data": drop the in-memory document, refetch with cache
 * busting, and notify subscribers. Resolves with the fresh snapshot so callers
 * can re-anchor the camera once the newest document is loaded.
 */
export async function reloadCot3ySeries() {
  invalidateCot3ySeriesCache()
  _polledTag = null
  await prefetch()
  return HistoricalCOTStore.getSnapshot()
}

export const COT_3Y_PATH_EXPORT = COT_3Y_PATH
