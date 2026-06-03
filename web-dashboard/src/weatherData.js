/** Instrument weather context — ``weather_context_latest.json`` (Wheat + Nat Gas). */
import { interpretWeatherRegion, weeklyBiasLine } from './weatherInterpretation.js'

export const WEATHER_CONTEXT_MARKETS = new Set(['Wheat', 'Natural Gas / NG'])

function recordsFromContext(doc, market) {
  const block = doc?.markets?.[market]
  if (!block || typeof block !== 'object') return []
  return Array.isArray(block.records) ? block.records : []
}

function feedWeatherRows(row) {
  const feed = row?.market_environment_feed
  if (!feed || typeof feed !== 'object') return []
  const snap = feed.weather_snapshot
  if (Array.isArray(snap) && snap.length) return snap
  const recs = Array.isArray(feed.records) ? feed.records : []
  return recs.filter((r) => r && r.category === 'weather' && (r.summary || r.forecast_summary))
}

function normalizeRecord(raw, market) {
  if (!raw || typeof raw !== 'object') return null
  const base = {
    region: String(raw.region || '—').trim(),
    temperature_display: raw.temperature_display ?? raw.temperature ?? null,
    precipitation_display: raw.precipitation_display ?? null,
    precipitation_mm_24h: raw.precipitation_mm_24h ?? null,
    forecast_summary: raw.forecast_summary ?? raw.summary ?? null,
    timestamp: raw.timestamp ?? raw.fetched_at ?? null,
    fetched_at: raw.fetched_at ?? null,
    importance: raw.importance ?? null,
    signals: raw.signals || null,
    ok: raw.ok === true,
    error: raw.error ? String(raw.error) : null,
    provider: raw.provider || 'openweather',
  }
  if (base.ok) {
    base.interpretation = interpretWeatherRegion({ ...raw, ...base }, market)
  }
  return base
}

/**
 * @param {object} row
 * @param {object|null} weatherContext
 * @param {{ loadError?: string|null }} [meta]
 */
export function resolveWeatherForMarket(row, weatherContext, meta = {}) {
  const market = String(row?.market || '').trim()
  if (!WEATHER_CONTEXT_MARKETS.has(market)) {
    return {
      market,
      enabled: false,
      records: [],
      bundleError: null,
      loadError: null,
      provider: null,
      hasOk: false,
    }
  }

  const fromContext = recordsFromContext(weatherContext, market)
    .map((r) => normalizeRecord(r, market))
    .filter(Boolean)
  const fromFeed = feedWeatherRows(row)
    .map((r) =>
      normalizeRecord(
        {
          region: r.region || r.title,
          temperature_display: r.temperature_display,
          precipitation_display: r.precipitation_display,
          precipitation_mm_24h: r.precipitation_mm_24h,
          forecast_summary: r.summary || r.forecast_summary,
          timestamp: r.fetched_at,
          fetched_at: r.fetched_at,
          importance: r.importance,
          signals: r.signals,
          interpretation: r.interpretation,
          ok: true,
          error: null,
          provider: r.provider,
        },
        market,
      ),
    )
    .filter(Boolean)

  const records = fromContext.length ? fromContext : fromFeed
  const hasOk = records.some((r) => r.ok)
  const bundleError =
    weatherContext?.error ||
    (records.length && !hasOk ? records.map((r) => r.error).filter(Boolean).join('; ') : null) ||
    null

  return {
    market,
    enabled: true,
    records,
    weeklyBiasLine: weeklyBiasLine(weatherContext, market),
    bundleError: bundleError ? String(bundleError) : null,
    loadError: meta.loadError ? String(meta.loadError) : null,
    provider: weatherContext?.provider || records[0]?.provider || null,
    hasOk,
  }
}

/** @param {ReturnType<typeof resolveWeatherForMarket>} wx */
export function hasRealWeather(wx) {
  return !!(wx?.hasOk && wx.records?.some((r) => r.ok))
}
