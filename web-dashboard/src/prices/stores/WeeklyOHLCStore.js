/**
 * WeeklyOHLCStore — completed weekly OHLC only.
 * Never used as live price.
 */

import { normalizeWeeklyOhlc } from '../../workstation/data/normalizeWeeklyTimeline.js'

const OHLC_URL = '/data/workstation_ohlc_latest.json'

const _listeners = new Set()
let _doc = null
let _loadPromise = null
let _lastFetchUrl = null
let _subscriberCount = 0
let _snapshotCache = null
let _snapshotCacheKey = ''

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function emit() {
  _snapshotCache = null
  _snapshotCacheKey = ''
  for (const fn of _listeners) fn()
}

function isPlottableBar(bar) {
  if (!bar) return false
  const { open, high, low, close } = bar
  return [open, high, low, close].every(isNum) && high > low
}

function normalizeExportWeekly(bars) {
  if (!Array.isArray(bars)) return []
  return normalizeWeeklyOhlc(
    bars
      .map((b) => ({
        date: String(b.date || '').slice(0, 10),
        open: Number(b.open),
        high: Number(b.high),
        low: Number(b.low),
        close: Number(b.close),
      }))
      .filter((b) => b.date && isPlottableBar(b)),
  )
}

async function fetchDoc({ bustCache = false } = {}) {
  if (_doc && !bustCache) return _doc
  if (_loadPromise && !bustCache) return _loadPromise

  if (bustCache) {
    _doc = null
    _loadPromise = null
  }

  const fetchUrl = `${OHLC_URL}?v=${Date.now()}`
  _lastFetchUrl = fetchUrl
  _loadPromise = fetch(fetchUrl, { cache: 'no-store' })
    .then((r) => {
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      return r.json()
    })
    .then((doc) => {
      _doc = doc && typeof doc === 'object' ? doc : { instruments: {} }
      _loadPromise = null
      emit()
      return _doc
    })
    .catch(() => {
      _doc = { instruments: {} }
      _loadPromise = null
      emit()
      return _doc
    })
  return _loadPromise
}

export const WeeklyOHLCStore = {
  STORE_NAME: 'WeeklyOHLCStore',

  subscribe(listener) {
    _listeners.add(listener)
    _subscriberCount += 1
    if (_subscriberCount === 1 && !_doc && !_loadPromise) {
      fetchDoc({ bustCache: false })
    }
    return () => {
      _listeners.delete(listener)
      _subscriberCount = Math.max(0, _subscriberCount - 1)
    }
  },

  getSnapshot() {
    const key = `${_doc?.generated_at ?? ''}|${_lastFetchUrl ?? ''}`
    if (_snapshotCache && _snapshotCacheKey === key) return _snapshotCache
    _snapshotCacheKey = key
    _snapshotCache = {
      doc: _doc,
      loaded: _doc != null,
      fetchUrl: _lastFetchUrl,
      generatedAt: _doc?.generated_at ?? null,
      subscriberCount: _subscriberCount,
    }
    return _snapshotCache
  },

  getExportBlock(marketId) {
    if (!marketId || !_doc?.instruments) return null
    return _doc.instruments[marketId] ?? null
  },

  getWeeklyBars(marketId) {
    const block = this.getExportBlock(marketId)
    if (!block?.weekly_ohlc?.length) return []
    return normalizeExportWeekly(block.weekly_ohlc)
  },

  /** Latest completed weekly candle — authoritative weekly close. */
  getCompletedWeekly(marketId) {
    const bars = this.getWeeklyBars(marketId)
    const latest = bars[bars.length - 1]
    if (!latest) return null
    return {
      instrumentId: marketId,
      date: latest.date,
      open: latest.open,
      high: latest.high,
      low: latest.low,
      close: latest.close,
      priceSource: this.getPriceSource(marketId),
    }
  },

  getPriceSource(marketId) {
    const block = this.getExportBlock(marketId)
    return block?.price_source || block?.canonical_source || 'workstation_ohlc_latest.json'
  },

  getPriceQuality(marketId) {
    return this.getExportBlock(marketId)?.price_quality ?? null
  },

  async refresh({ bustCache = true } = {}) {
    await fetchDoc({ bustCache })
  },
}

/** @deprecated use WeeklyOHLCStore */
export function loadWorkstationOhlcStore(opts) {
  return fetchDoc(opts)
}

export function clearWorkstationOhlcCache() {
  _doc = null
  _loadPromise = null
  emit()
}

export function getWorkstationOhlcLastFetchUrl() {
  return _lastFetchUrl
}

export function resolveWorkstationWeeklyOhlc(marketId, _priceRec, ohlcExportBlock = null) {
  const block = ohlcExportBlock ?? WeeklyOHLCStore.getExportBlock(marketId)
  const weeklyBars = block?.weekly_ohlc?.length
    ? normalizeExportWeekly(block.weekly_ohlc)
    : WeeklyOHLCStore.getWeeklyBars(marketId)
  return {
    weeklyBars,
    priceSource: block?.price_source || WeeklyOHLCStore.getPriceSource(marketId),
    resolvedFrom: weeklyBars.length ? 'workstation_ohlc_latest.json' : 'none',
    exportMeta: block,
  }
}
