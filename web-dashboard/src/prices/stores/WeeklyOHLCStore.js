/**
 * WeeklyOHLCStore — completed weekly OHLC only.
 * Never used as live price.
 *
 * Canonical HPTL price history is the primary source for overlapping dates.
 * The legacy workstation OHLC export is retained as a historical backfill so a
 * shorter canonical refresh cannot silently collapse a 5Y/10Y chart to ~1Y.
 */

import { normalizeWeeklyOhlc } from '../../workstation/data/normalizeWeeklyTimeline.js'

const CANONICAL_URL = '/data/prices_latest.json'
const LEGACY_OHLC_URL = '/data/workstation_ohlc_latest.json'

const _listeners = new Set()
let _doc = null
let _legacyDoc = null
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
    return bars
      .map((b) => {
        const allHigh = b.open > 100 && b.high > 100 && b.low > 100 && b.close > 100
        const allLow = b.open < 100 && b.high < 100 && b.low < 100 && b.close < 100
        if (allLow) return b
        if (allHigh) return scaleBar(b, 1 / 1000)
        return null
      })
      .filter(Boolean)
      .filter((b) => isPlottableBar(b))
  }
  if (med > 200) {
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

/**
 * Merge historical weekly candles by date.
 *
 * The legacy export is inserted first so it can supply older history. Canonical
 * bars are inserted second and therefore win on every overlapping date. This
 * gives the workstation the deepest available completed history without ever
 * preferring stale legacy values for a week that exists in prices_latest.json.
 */
export function mergeWeeklyHistory(legacyBars, canonicalBars) {
  const legacy = normalizeExportWeekly(legacyBars)
  const canonical = normalizeExportWeekly(canonicalBars)
  const byDate = new Map()

  for (const bar of legacy) byDate.set(bar.date, bar)
  for (const bar of canonical) byDate.set(bar.date, bar)

  return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date))
}

async function fetchJson(url) {
  const r = await fetch(url, { cache: 'no-store' })
  if (!r.ok) throw new Error(`HTTP ${r.status}`)
  return r.json()
}

async function fetchDoc({ bustCache = false } = {}) {
  if (_doc && !bustCache) return _doc
  if (_loadPromise && !bustCache) return _loadPromise

  if (bustCache) {
    _doc = null
    _legacyDoc = null
    _loadPromise = null
  }

  const stamp = Date.now()
  const canonicalUrl = `${CANONICAL_URL}?v=${stamp}`
  const legacyUrl = `${LEGACY_OHLC_URL}?v=${stamp}`
  _lastFetchUrl = canonicalUrl

  _loadPromise = Promise.allSettled([
    fetchJson(canonicalUrl),
    fetchJson(legacyUrl),
  ])
    .then(([canonical, legacy]) => {
      _doc = canonical.status === 'fulfilled' && canonical.value && typeof canonical.value === 'object'
        ? canonical.value
        : { instruments: {} }
      _legacyDoc = legacy.status === 'fulfilled' && legacy.value && typeof legacy.value === 'object'
        ? legacy.value
        : { instruments: {} }
      _loadPromise = null
      emit()
      return _doc
    })
    .catch(() => {
      _doc = { instruments: {} }
      _legacyDoc = { instruments: {} }
      _loadPromise = null
      emit()
      return _doc
    })
  return _loadPromise
}

function canonicalRecord(marketId) {
  return marketId && _doc?.instruments ? _doc.instruments[marketId] ?? null : null
}

function legacyBlock(marketId) {
  return marketId && _legacyDoc?.instruments ? _legacyDoc.instruments[marketId] ?? null : null
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
    const key = `${_doc?.generated_at ?? ''}|${_legacyDoc?.generated_at ?? ''}|${_lastFetchUrl ?? ''}`
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
    if (!marketId) return null
    const canonical = canonicalRecord(marketId)
    const legacy = legacyBlock(marketId)

    if (canonical?.weekly?.length) {
      const legacyWeekly = legacy?.weekly_ohlc || []
      const mergedWeekly = mergeWeeklyHistory(legacyWeekly, canonical.weekly)
      const canonicalWeekly = normalizeExportWeekly(canonical.weekly)
      const legacyContributed = mergedWeekly.length > canonicalWeekly.length

      return {
        ...(legacy || {}),
        weekly_ohlc: mergedWeekly,
        forming_weekly: canonical.forming_weekly ?? null,
        price_source: legacyContributed
          ? 'canonical prices_latest.json + legacy historical backfill'
          : 'canonical prices_latest.json',
        canonical_source: 'prices_latest.json',
        price_quality: canonical.error ? 'provider_error' : 'canonical_fresh',
        canonical_history: canonical.history ?? null,
        history_merge: {
          canonical_weeks: canonicalWeekly.length,
          legacy_weeks: normalizeExportWeekly(legacyWeekly).length,
          merged_weeks: mergedWeekly.length,
          legacy_backfill_used: legacyContributed,
          canonical_wins_overlap: true,
        },
      }
    }

    return legacy
  },

  getWeeklyBars(marketId) {
    const block = this.getExportBlock(marketId)
    if (!block?.weekly_ohlc?.length) return []
    return normalizeExportWeekly(block.weekly_ohlc)
  },

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
    const canonical = canonicalRecord(marketId)
    if (canonical?.weekly?.length) {
      const legacy = legacyBlock(marketId)
      const merged = mergeWeeklyHistory(legacy?.weekly_ohlc || [], canonical.weekly)
      const canonicalWeeks = normalizeExportWeekly(canonical.weekly).length
      return merged.length > canonicalWeeks
        ? 'canonical prices_latest.json + legacy historical backfill'
        : 'canonical prices_latest.json'
    }
    const block = legacyBlock(marketId)
    return block?.price_source || block?.canonical_source || 'workstation_ohlc_latest.json'
  },

  getPriceQuality(marketId) {
    const canonical = canonicalRecord(marketId)
    if (canonical?.weekly?.length) return canonical.error ? 'provider_error' : 'canonical_fresh'
    return legacyBlock(marketId)?.price_quality ?? null
  },

  async refresh({ bustCache = true } = {}) {
    await fetchDoc({ bustCache })
  },
}

export function loadWorkstationOhlcStore(opts) {
  return fetchDoc(opts)
}

export function clearWorkstationOhlcCache() {
  _doc = null
  _legacyDoc = null
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
  const canonical = canonicalRecord(marketId)
  const mergedSource = block?.history_merge?.legacy_backfill_used === true
  return {
    weeklyBars,
    priceSource: mergedSource
      ? 'canonical prices_latest.json + legacy historical backfill'
      : canonical?.weekly?.length
        ? 'canonical prices_latest.json'
        : block?.price_source || WeeklyOHLCStore.getPriceSource(marketId),
    resolvedFrom: mergedSource
      ? 'prices_latest.json+workstation_ohlc_latest.json'
      : canonical?.weekly?.length
        ? 'prices_latest.json'
        : (weeklyBars.length ? 'workstation_ohlc_latest.json' : 'none'),
    exportMeta: block,
  }
}
