/**
 * Trust-first wire labels for live environment feeds.
 * LIVE | STALE | NOT WIRED | LOW CONFIDENCE
 */

export const LIVE_STALE_MS = 30 * 60 * 1000

const NOT_WIRED = 'NOT WIRED'
const STALE = 'STALE'
const LIVE = 'LIVE'
const LOW_CONFIDENCE = 'LOW CONFIDENCE'

export function normalizeWireLabel(raw) {
  const u = String(raw || '')
    .trim()
    .toUpperCase()
  if (!u || u === '—' || u.includes('NOT WIRED') || u === 'NOT CONFIGURED') return NOT_WIRED
  if (u.includes('STALE')) return STALE
  if (u.includes('LOW CONFIDENCE') || u === 'UNKNOWN') return LOW_CONFIDENCE
  if (u === 'LIVE') return LIVE
  return u
}

export function isoMs(iso) {
  const t = Date.parse(String(iso || ''))
  return Number.isFinite(t) ? t : NaN
}

export function isFeedTimestampStale(iso, nowMs = Date.now()) {
  const t = isoMs(iso)
  if (!Number.isFinite(t)) return true
  return nowMs - t > LIVE_STALE_MS
}

function feedBundle(row) {
  const feed = row?.market_environment_feed
  return feed && typeof feed === 'object' ? feed : null
}

function finnhubNewsConfigured(feed) {
  const st = feed?.sources_status?.finnhub_news
  return st === 'finnhub' || feed?.sources_status?.finnhub_news_active === true
}

function newsItemsLowConfidence(items) {
  if (!items?.length) return false
  const lows = items.filter((n) => String(n.confidence || '').toLowerCase() === 'low')
  return lows.length === items.length
}

/** @returns {{ status: string, detail: string }} */
export function resolveNewsWireStatus(row, nowMs = Date.now()) {
  const feed = feedBundle(row)
  if (!feed || !Object.keys(feed).length) {
    return { status: NOT_WIRED, detail: 'No market_environment_feed on this row — run environment feed update.' }
  }
  if (!finnhubNewsConfigured(feed)) {
    return { status: NOT_WIRED, detail: 'Set FINNHUB_API_KEY in .env and run environment feed update.' }
  }
  const items = Array.isArray(feed.news_items) ? feed.news_items : []
  const checked = feed.live_bundle_last_checked_at
  if (!items.length) {
    return {
      status: LOW_CONFIDENCE,
      detail: 'Finnhub connected; no headlines matched this instrument’s catalyst keywords in the latest pull.',
    }
  }
  if (isFeedTimestampStale(checked, nowMs)) {
    return { status: STALE, detail: 'News bundle older than 30 minutes — re-run environment feed update.' }
  }
  if (newsItemsLowConfidence(items)) {
    return { status: LOW_CONFIDENCE, detail: 'Headlines present but confidence is low on all items.' }
  }
  return { status: LIVE, detail: `${items.length} validated headline(s) from Finnhub.` }
}

/** @returns {{ status: string, detail: string }} */
export function resolveCalendarWireStatus(row, globalCalendar, nowMs = Date.now()) {
  const feed = feedBundle(row)
  const cal = feed?.calendar_catalysts
  if (cal && typeof cal === 'object' && cal.wired) {
    const up = cal.upcoming_high_impact?.length || 0
    const rel = cal.latest_released?.length || 0
    if (isFeedTimestampStale(feed.live_bundle_last_checked_at, nowMs)) {
      return { status: STALE, detail: 'Calendar slice stale — re-run environment feed update.' }
    }
    if (!up && !rel) {
      return {
        status: LOW_CONFIDENCE,
        detail: 'Calendar wired; no high-impact events for this instrument in the current window.',
      }
    }
    return { status: LIVE, detail: `Calendar via ${cal.provider || 'provider'} (${up} upcoming, ${rel} released).` }
  }
  if (globalCalendar?.wired) {
    const up = globalCalendar.upcoming_high_impact?.length || 0
    const rel = globalCalendar.latest_released?.length || 0
    if (isFeedTimestampStale(globalCalendar.fetched_at, nowMs)) {
      return { status: STALE, detail: 'Global calendar export is stale.' }
    }
    if (!up && !rel) {
      return { status: LOW_CONFIDENCE, detail: 'Global calendar loaded; nothing mapped to this market.' }
    }
    return {
      status: LIVE,
      detail: `Global calendar (${globalCalendar.provider || 'provider'}).`,
    }
  }
  const msg = cal?.message || globalCalendar?.message || 'Add FINNHUB_API_KEY (or Trading Economics) in .env.'
  return { status: NOT_WIRED, detail: msg }
}

/** @returns {{ status: string, detail: string }} */
export function resolveWeatherWireStatus(row, weatherContext, weatherLoadError, nowMs = Date.now()) {
  const feed = feedBundle(row)
  if (feed?.weather_feed_connected) {
    if (isFeedTimestampStale(feed.live_bundle_last_checked_at, nowMs)) {
      return { status: STALE, detail: 'Weather snapshot stale — re-run environment feed update.' }
    }
    return { status: LIVE, detail: 'OpenWeather snapshot on row feed.' }
  }
  if (weatherContext?.wired && weatherContext?.openweather_api_key_detected) {
    const market = String(row?.market || '').trim()
    const block = weatherContext.markets?.[market]
    const recs = block?.records || []
    if (weatherLoadError) {
      return { status: NOT_WIRED, detail: weatherLoadError }
    }
    if (isFeedTimestampStale(weatherContext.fetched_at, nowMs)) {
      return { status: STALE, detail: 'weather_context_latest.json is stale.' }
    }
    const ok = recs.filter((r) => r.ok)
    if (!ok.length) {
      return { status: LOW_CONFIDENCE, detail: weatherContext.error || 'Weather export has no OK regions.' }
    }
    return { status: LIVE, detail: `${ok.length} region(s) in weather_context_latest.json.` }
  }
  if (weatherContext?.openweather_api_key_detected === false) {
    return { status: NOT_WIRED, detail: 'Set OPENWEATHER_API_KEY for weather-enabled markets.' }
  }
  return { status: NOT_WIRED, detail: 'Weather not enabled for this market or feed not built.' }
}

/** Scanner catalyst dots: news / weather / calendar */
export function catalystSummaryFromRow(row, globalCalendar, weatherContext, weatherLoadError, nowMs = Date.now()) {
  return {
    news: resolveNewsWireStatus(row, nowMs).status,
    weather: resolveWeatherWireStatus(row, weatherContext, weatherLoadError, nowMs).status,
    calendar: resolveCalendarWireStatus(row, globalCalendar, nowMs).status,
  }
}
