/**
 * LivePriceStore — authoritative OANDA live quotes only.
 * No weekly OHLC, no historical COT, no valuation fallbacks.
 */

import { getLiveQuoteFreshness, LIVE_QUOTE_POLL_MS } from '../../hooks/liveQuoteFreshness.js'

const LIVE_QUOTES_URL = '/data/live_quotes_latest.json'

const _listeners = new Set()
let _doc = null
let _loadPromise = null
let _lastFetchUrl = null
let _lastFetchedAtMs = null
let _refreshing = false
let _refreshError = null
let _pollId = null
let _subscriberCount = 0
let _snapshotCache = null
let _snapshotCacheKey = ''

function emit() {
  _snapshotCache = null
  _snapshotCacheKey = ''
  for (const fn of _listeners) fn()
}

function normalizeLiveQuote(marketId, raw) {
  if (!raw || raw.live_fetch_ok === false) {
    const mid = raw?.live_price
    if (mid == null || !Number.isFinite(Number(mid))) return null
  }
  const mid = raw?.live_price
  if (mid == null || !Number.isFinite(Number(mid))) return null
  return {
    instrumentId: marketId,
    symbol: raw.canonical_symbol ?? null,
    bid: Number.isFinite(Number(raw.live_bid)) ? Number(raw.live_bid) : null,
    ask: Number.isFinite(Number(raw.live_ask)) ? Number(raw.live_ask) : null,
    mid: Number(mid),
    source: raw.live_price_source ?? 'OANDA',
    asOf: raw.live_price_as_of ?? null,
    fetchOk: raw.live_fetch_ok !== false,
    fetchError: raw.live_fetch_error ?? null,
  }
}

export async function triggerLiveQuotesExport() {
  const resp = await fetch('/api/live-quotes/refresh', {
    method: 'POST',
    cache: 'no-store',
  })
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({}))
    throw new Error(body?.error || `HTTP ${resp.status}`)
  }
  return resp.json().catch(() => ({ ok: true }))
}

async function fetchDoc({ bustCache = false } = {}) {
  if (_doc && !bustCache) return _doc
  if (_loadPromise && !bustCache) return _loadPromise

  if (bustCache) {
    _doc = null
    _loadPromise = null
  }

  const fetchUrl = `${LIVE_QUOTES_URL}?v=${Date.now()}`
  _lastFetchUrl = fetchUrl
  _loadPromise = fetch(fetchUrl, { cache: 'no-store' })
    .then((r) => {
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      return r.json()
    })
    .then((doc) => {
      _doc = doc && typeof doc === 'object' ? doc : { instruments: {} }
      _lastFetchedAtMs = Date.now()
      _loadPromise = null
      _refreshError = null
      emit()
      return _doc
    })
    .catch((err) => {
      _loadPromise = null
      _refreshError = String(err?.message || err)
      emit()
      throw err
    })
  return _loadPromise
}

function startPolling() {
  if (_pollId != null) return
  _pollId = window.setInterval(() => {
    fetchDoc({ bustCache: true }).catch(() => {})
  }, LIVE_QUOTE_POLL_MS)
}

function stopPolling() {
  if (_pollId != null) {
    window.clearInterval(_pollId)
    _pollId = null
  }
}

export const LivePriceStore = {
  STORE_NAME: 'LivePriceStore',

  subscribe(listener) {
    _listeners.add(listener)
    _subscriberCount += 1
    if (_subscriberCount === 1) {
      fetchDoc({ bustCache: true }).catch(() => {})
      startPolling()
    }
    return () => {
      _listeners.delete(listener)
      _subscriberCount = Math.max(0, _subscriberCount - 1)
      if (_subscriberCount === 0) stopPolling()
    }
  },

  getSnapshot() {
    const key = `${_doc?.generated_at ?? ''}|${_refreshing}|${_refreshError ?? ''}|${_subscriberCount}|${_lastFetchedAtMs ?? ''}`
    if (_snapshotCache && _snapshotCacheKey === key) return _snapshotCache
    _snapshotCacheKey = key
    _snapshotCache = {
      doc: _doc,
      loaded: _doc != null,
      fetchUrl: _lastFetchUrl,
      fetchedAtMs: _lastFetchedAtMs,
      generatedAt: _doc?.generated_at ?? null,
      refreshing: _refreshing,
      refreshError: _refreshError,
      subscriberCount: _subscriberCount,
    }
    return _snapshotCache
  },

  /** Live OANDA quote for one instrument — never includes weekly/historical fields. */
  getQuote(marketId) {
    if (!marketId || !_doc?.instruments) return null
    return normalizeLiveQuote(marketId, _doc.instruments[marketId])
  },

  getFreshness(marketId) {
    const raw = _doc?.instruments?.[marketId]
    return getLiveQuoteFreshness(raw, _doc)
  },

  /** 'LIVE' | 'STALE' | 'UNAVAILABLE' */
  getStatus(marketId) {
    const quote = this.getQuote(marketId)
    if (!quote) return 'UNAVAILABLE'
    const { isStale } = this.getFreshness(marketId)
    return isStale ? 'STALE' : 'LIVE'
  },

  async refresh({ runExport = true } = {}) {
    _refreshing = true
    _refreshError = null
    emit()
    try {
      if (runExport) {
        try {
          await triggerLiveQuotesExport()
        } catch (exportErr) {
          console.warn('[LivePriceStore] export refresh unavailable', exportErr)
        }
      }
      await fetchDoc({ bustCache: true })
    } catch (err) {
      _refreshError = String(err?.message || err)
      emit()
      throw err
    } finally {
      _refreshing = false
      emit()
    }
  },

  /** @deprecated use LivePriceStore.refresh */
  clearCache() {
    _doc = null
    _loadPromise = null
    emit()
  },
}
