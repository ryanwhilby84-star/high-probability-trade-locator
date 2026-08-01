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
  if (![open, high, low, close].every(isNum) || !(high > low)) return false
  // Reject mixed-unit weeks (e.g. Copper $/lb OHLC mixed with tonne/HG×1000 scale).
  // Those paint as full-height candle "forests" that obscure price.
  if (high / Math.max(low, 1e-12) > 2.5) return false
  const mid = (high + low) / 2
  if (mid > 0) {
    if (Math.max(open, high, low, close) / mid > 2.5) return false
    if (mid / Math.min(open, high, low, close) > 2.5) return false
  }
  return true
}

function median(vals) {
  if (!vals.length) return null
  const s = [...vals].sort((a, b) => a - b)
  const m = Math.floor(s.length / 2)
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2
}

/**
 * Keep one coherent price scale for the series.
 * Copper history mixes OANDA $/lb (~2–10) with legacy HG chart scale
 * (USD/lb × 1000, ~2k–10k). Prefer the recent regime; convert when safe.
 */
function coerceConsistentPriceScale(bars) {
  if (!bars.length) return bars
  const recent = bars.slice(-Math.min(104, bars.length))
  const med = median(recent.map((b) => b.close).filter(isNum))
  if (!isNum(med)) return bars

  const scaleBar = (b, factor) => ({
    ...b,
    open: b.open * factor,
    high: b.high * factor,
    low: b.low * factor,
    close: b.close * factor,
  })

  if (med < 50) {
    // Spot / $/lb regime — convert HG×1000 bars down to $/lb when whole bar is high-scale.
    return bars
      .map((b) => {
        const allHigh = b.open > 100 && b.high > 100 && b.low > 100 && b.close > 100
        const allLow = b.open < 100 && b.high < 100 && b.low < 100 && b.close < 100
        if (allLow) return b
        if (allHigh) return scaleBar(b, 1 / 1000)
        return null // mixed intra-bar already rejected by isPlottableBar
      })
      .filter(Boolean)
      .filter((b) => isPlottableBar(b))
  }
  if (med > 200) {
    // HG chart regime — convert raw $/lb up when whole bar is low-scale.
    return bars
      .map((b) => {
        const allHigh = b.open > 100 && b.high > 100 && b.low > 100 && b.close > 100
        const allLow = b.open < 100 && b.high < 100 && b.low < 100 && b.close < 100
        if (allHigh) return b
        if (allLow) return scaleBar(b, 1000)
        return null
      })
      .filter(Boolean)
      .filter((b) => isPlottableBar(b))
  }
  return bars
}

function normalizeExportWeekly(bars) {
  if (!Array.isArray(bars)) return []
  const mapped = bars
    .map((b) => ({
      date: String(b.date || '').slice(0, 10),
      open: Number(b.open),
      high: Number(b.high),
      low: Number(b.low),
      close: Number(b.close),
    }))
    .filter((b) => b.date && isPlottableBar(b))
  return normalizeWeeklyOhlc(coerceConsistentPriceScale(mapped))
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
