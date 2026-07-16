/**
 * LivePriceStore — live quotes from the Phase 2 Current Price Service.
 *
 * Backed by CurrentPriceStreamStore (single shared WebSocket to
 * ws://localhost:8787/ws/prices via the Vite /ws/prices proxy).
 *
 * No longer polls /data/live_quotes_latest.json for the current price.
 * Historical weekly OHLC remains in WeeklyOHLCStore.
 */

import { CurrentPriceStreamStore } from './CurrentPriceStreamStore.js'

const _listeners = new Set()
let _unsubStream = null
let _snapshotCache = null
let _snapshotCacheKey = ''

function emit() {
  _snapshotCache = null
  _snapshotCacheKey = ''
  for (const fn of _listeners) fn()
}

function ensureStreamSubscription() {
  if (_unsubStream) return
  _unsubStream = CurrentPriceStreamStore.subscribe(() => emit())
}

function releaseStreamSubscription() {
  if (_listeners.size > 0) return
  if (_unsubStream) {
    _unsubStream()
    _unsubStream = null
  }
}

function toLegacyQuote(price) {
  if (!price) return null
  const mid = price.mid ?? price.currentPrice
  if (mid == null || !Number.isFinite(Number(mid))) return null

  return {
    instrumentId: price.internalKey,
    symbol: price.providerSymbol ?? null,
    bid: price.bid,
    ask: price.ask,
    mid: Number(mid),
    source: price.provider
      ? `${price.provider}:${price.providerSymbol || ''}`.replace(/:$/, '')
      : 'oanda',
    asOf: price.timestamp ?? null,
    fetchOk: true,
    fetchError: null,
    pricePrecision: price.pricePrecision,
    status: price.status,
    ageSeconds: price.ageSeconds,
    provider: price.provider,
    providerSymbol: price.providerSymbol,
    currentPrice: price.currentPrice,
    fallbackClose: price.fallbackClose,
    fallbackSource: price.fallbackSource,
  }
}

export async function triggerLiveQuotesExport() {
  // Legacy path kept for callers; live prices now come from the stream service.
  return { ok: true, skipped: true, reason: 'current_price_stream' }
}

export const LivePriceStore = {
  STORE_NAME: 'LivePriceStore',

  subscribe(listener) {
    _listeners.add(listener)
    ensureStreamSubscription()
    return () => {
      _listeners.delete(listener)
      releaseStreamSubscription()
    }
  },

  getSnapshot() {
    const stream = CurrentPriceStreamStore.getSnapshot()
    const key = [
      stream.connectionState,
      stream.generatedAt ?? '',
      Object.keys(stream.prices).length,
      stream.lastError ?? '',
    ].join('|')

    if (_snapshotCache && _snapshotCacheKey === key) return _snapshotCache

    _snapshotCacheKey = key
    _snapshotCache = {
      doc: {
        generated_at: stream.generatedAt,
        instruments: stream.prices,
        source: 'current_price_service',
      },
      loaded: Object.keys(stream.prices).length > 0 || stream.connected,
      fetchUrl: 'ws:/ws/prices',
      fetchedAtMs: stream.generatedAt ? Date.parse(stream.generatedAt) || Date.now() : null,
      generatedAt: stream.generatedAt,
      refreshing: stream.reconnecting,
      refreshError: stream.lastError,
      subscriberCount: _listeners.size,
      connectionState: stream.connectionState,
      streamMeta: stream.streamMeta,
    }
    return _snapshotCache
  },

  getQuote(marketId) {
    return toLegacyQuote(CurrentPriceStreamStore.getPrice(marketId))
  },

  getFreshness(marketId) {
    const price = CurrentPriceStreamStore.getPrice(marketId)
    if (!price) {
      return {
        isStale: true,
        ageMs: null,
        quoteAsOf: null,
        docGeneratedAt: CurrentPriceStreamStore.getSnapshot().generatedAt,
      }
    }

    const ageSeconds = price.ageSeconds
    const ageMs = ageSeconds != null ? ageSeconds * 1000 : null
    const status = CurrentPriceStreamStore.getDisplayStatus(marketId)
    const isStale = status !== 'LIVE'

    return {
      isStale,
      ageMs,
      quoteAsOf: price.timestamp ?? null,
      docGeneratedAt: CurrentPriceStreamStore.getSnapshot().generatedAt,
      status,
    }
  },

  getStatus(marketId) {
    return CurrentPriceStreamStore.getDisplayStatus(marketId)
  },

  getActiveWeeklyCandle(marketId) {
    return CurrentPriceStreamStore.getWeeklyCandle(marketId)
  },

  async refresh() {
    await CurrentPriceStreamStore.reconnect()
    return this.getSnapshot()
  },

  clearCache() {
    CurrentPriceStreamStore.clearCache()
    emit()
  },
}

// Dev/proof hook: backend quote ↔ store ↔ DOM parity checks.
if (typeof window !== 'undefined') {
  window.__HPTL_LIVE_PRICE_STORE__ = LivePriceStore
  window.__HPTL_CURRENT_PRICE_STREAM__ = CurrentPriceStreamStore
}

export default LivePriceStore
