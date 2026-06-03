/**
 * Compact “live market context” for one instrument — trust-first.
 * Row snapshot fields are labeled as COT-week data, not intraday “live”.
 */

import {
  validateNewsItem,
  validateEventItem,
} from './marketEnvironment.js'
import { buildMacroReadableDigest } from './macroReadableDigest.js'
import {
  normalizeWireLabel,
  resolveNewsWireStatus,
  resolveCalendarWireStatus,
  resolveWeatherWireStatus,
} from './liveFeedStatus.js'

function real(v) {
  const s = String(v ?? '').trim()
  return s.length > 0 && s.toUpperCase() !== 'N/A'
}

function num(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : NaN
}

function clip(s, n) {
  const t = String(s || '').trim()
  if (t.length <= n) return t
  return `${t.slice(0, n - 1)}…`
}

function isoMs(iso) {
  const t = Date.parse(String(iso || ''))
  return Number.isFinite(t) ? t : NaN
}

function rowAsOf(row) {
  return String(row?.date || row?.latest_report_date || row?.as_of_date || '').trim() || null
}

/** What matters for events/weather-style risk by contract (static lens, not a feed). */
export const EVENT_WEATHER_FOCUS = {
  'Natural Gas / NG': 'Weather, storage, flow balance.',
  'Wheat': 'Weather, USDA reports, crop conditions, related grains.',
  Corn: 'Weather, USDA reports, crop conditions, related grains.',
  Soybeans: 'Weather, USDA reports, China demand, related grains.',
  'Crude Oil / CL': 'Inventories, OPEC posture, geopolitics, shipping.',
  'Copper / HG': 'China demand proxies, USD, yields, related metals.',
  Gold: 'Real yields, USD, CPI/Fed calendar risk, geopolitical shocks.',
  Silver: 'Real yields, USD, industrial cycle, Gold.',
  'NASDAQ / NQ': 'Yields, USD, large-cap earnings, semis when mapped.',
  'S&P 500 / ES': 'Yields, USD, credit impulse, large-cap earnings.',
  'Dow / YM': 'Yields, USD, cyclicals vs defensives.',
  Coffee: 'Weather, Brazil supply, freight.',
  Cocoa: 'West Africa weather, supply chain.',
}

/** Cross-checks named in plain language (not a data feed). */
export const RELATED_LENS = {
  Gold: 'DXY, US yields, Silver',
  Silver: 'Gold, yields, DXY',
  'Copper / HG': 'AUD, China growth proxies, DXY, yields',
  'Crude Oil / CL': 'DXY, risk assets, related energies',
  'Natural Gas / NG': 'Crude, power spreads, weather',
  Wheat: 'Corn, Soybeans, USD',
  Corn: 'Wheat, Soybeans, ethanol policy',
  Soybeans: 'Corn, Wheat, China trade flow',
  'NASDAQ / NQ': 'Yields, DXY, semis (when in row), liquidity',
  'S&P 500 / ES': 'Yields, DXY, credit',
  'Dow / YM': 'Yields, DXY, cyclicals',
  Coffee: 'Cocoa, BRL, weather',
  Cocoa: 'Coffee, GBP, weather',
}

function ratesMacroWired(row) {
  const rm = row?.rates_macro
  if (rm && typeof rm === 'object') {
    if (
      real(rm.macro_signal) ||
      real(rm.rates_bias) ||
      real(rm.curve_state) ||
      Number.isFinite(num(rm.macro_score))
    ) {
      return true
    }
  }
  return real(row?.macro_regime) || Number.isFinite(num(row?.macro_score))
}

function ratesMacroSentence(market, row) {
  const rm = row?.rates_macro && typeof row.rates_macro === 'object' ? row.rates_macro : null
  const parts = []
  if (rm) {
    if (real(rm.rates_bias)) parts.push(String(rm.rates_bias).trim())
    if (real(rm.curve_state)) parts.push(`curve ${String(rm.curve_state).trim()}`)
    if (real(rm.macro_signal)) parts.push(String(rm.macro_signal).trim())
  }
  if (real(row?.macro_regime)) parts.push(`regime ${String(row.macro_regime).trim()}`)
  if (Number.isFinite(num(row?.macro_score))) parts.push(`macro score ${row.macro_score}`)
  if (!parts.length) return null
  const core = clip(parts.slice(0, 3).join(' — '), 130)
  return `Same-week snapshot for ${market}: ${core}.`
}

function ratesMacroSourceLine(row) {
  const rm = row?.rates_macro && typeof row.rates_macro === 'object' ? row.rates_macro : null
  const bits = ['Confluence export']
  if (rm && real(rm.rates_snapshot_date)) bits.push(`rates snapshot ${String(rm.rates_snapshot_date).trim()}`)
  if (rowAsOf(row)) bits.push(`COT week ${rowAsOf(row)}`)
  return bits.join(' · ')
}

function relatedMarketsState(row) {
  const inter = row?.intermarket_impulse_context && typeof row.intermarket_impulse_context === 'object' ? row.intermarket_impulse_context : {}
  const conf = String(inter.intermarket_confirmation || '').trim().toUpperCase()
  const hasDrivers =
    (Array.isArray(inter.supporting_drivers) && inter.supporting_drivers.length > 0) ||
    (Array.isArray(inter.conflicting_drivers) && inter.conflicting_drivers.length > 0)
  if (!conf && !hasDrivers) return 'NOT WIRED'
  if (conf === 'CONFIRMING') return 'Confirming'
  if (conf === 'MIXED') return 'Mixed'
  if (conf === 'DIVERGING' || conf === 'WARNING') return 'Contradicting'
  if (!conf && hasDrivers) return 'Mixed'
  return 'NOT WIRED'
}

function formatFetchedHm(iso) {
  const ms = isoMs(iso)
  if (!Number.isFinite(ms)) return null
  try {
    return new Date(ms).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', hour12: false })
  } catch {
    return null
  }
}

function feedRecords(feed) {
  return Array.isArray(feed?.records) ? feed.records : []
}

function weatherLinesFromFeed(feed) {
  return feedRecords(feed)
    .filter((r) => r && r.category === 'weather' && String(r.summary || '').trim())
    .slice(0, 2)
    .map((r) => ({
      text: clip(String(r.summary), 110),
      source: String(r.source || 'OpenWeather').trim(),
      fetched: formatFetchedHm(r.fetched_at),
    }))
}

function relatedLinesFromFeed(feed) {
  return feedRecords(feed)
    .filter((r) => r && r.category === 'related_market' && String(r.title || '').trim())
    .slice(0, 2)
    .map((r) => ({
      text: clip(`${r.title}: ${r.summary || ''}`, 120),
      source: String(r.source || '').trim(),
      fetched: formatFetchedHm(r.fetched_at),
    }))
}

/**
 * @param {object} row
 * @param {object} pack
 * @param {object|null} globalMarketRegime
 * @param {number} [nowMs]
 */
/**
 * @param {object} row
 * @param {object} pack
 * @param {object|null} globalMarketRegime
 * @param {object} [options]
 * @param {object|null} [options.globalCalendar]
 * @param {object|null} [options.weatherContext]
 * @param {string|null} [options.weatherLoadError]
 * @param {number} [options.nowMs]
 */
export function computeLiveMarketContext(row, pack, globalMarketRegime, options = {}) {
  const nowMs = options.nowMs ?? Date.now()
  const globalCalendar = options.globalCalendar ?? null
  const weatherContext = options.weatherContext ?? null
  const weatherLoadError = options.weatherLoadError ?? null
  const market = String(row?.market || '').trim() || '—'
  const feed = row?.market_environment_feed && typeof row.market_environment_feed === 'object' ? row.market_environment_feed : {}

  const rmWired = ratesMacroWired(row)
  const rmSentence = rmWired ? ratesMacroSentence(market, row) : null

  const rawNews = Array.isArray(feed.news_items) ? feed.news_items : []
  const validatedNews = []
  for (let i = 0; i < rawNews.length; i++) {
    const v = validateNewsItem(rawNews[i], market)
    if (v.ok && v.item) validatedNews.push(v.item)
  }

  const rawEvents = Array.isArray(feed.event_items) ? feed.event_items : []
  const validatedEvents = []
  for (let i = 0; i < rawEvents.length; i++) {
    const v = validateEventItem(rawEvents[i], market)
    if (v.ok && v.item) validatedEvents.push(v.item)
  }

  const instEvents = validatedEvents.filter(
    (e) => !e.related_instruments?.length || e.related_instruments.some((x) => String(x).trim() === market),
  )

  const newsWire = resolveNewsWireStatus(row, nowMs)
  const calWire = resolveCalendarWireStatus(row, globalCalendar, nowMs)
  const wxWire = resolveWeatherWireStatus(row, weatherContext, weatherLoadError, nowMs)

  const focusLine = EVENT_WEATHER_FOCUS[market] || 'Macro prints, USD, and cross-asset liquidity.'
  const weatherLines = weatherLinesFromFeed(feed)

  const relatedLens = RELATED_LENS[market] || 'See intermarket drivers on the row export.'
  const relatedFeedLines = relatedLinesFromFeed(feed)
  const relatedState = relatedFeedLines.length ? 'LIVE' : relatedMarketsState(row)

  const macroDigest = buildMacroReadableDigest(row, globalMarketRegime)
  const bundleChecked = feed.live_bundle_last_checked_at ? String(feed.live_bundle_last_checked_at).trim() : ''

  const showLiveBadge =
    (newsWire.status === 'LIVE' || calWire.status === 'LIVE') &&
    newsWire.status !== 'STALE' &&
    calWire.status !== 'STALE'

  return {
    macroDigest,
    ratesMacro: {
      state: rmWired ? 'LIVE' : 'NOT WIRED',
      sentence: rmSentence || 'Rates/macro slice not present on this row — rebuild confluence if expected.',
      source: ratesMacroSourceLine(row),
      timestamp: rowAsOf(row),
      wired: rmWired,
    },
    newsFlow: {
      state: newsWire.status,
      detail: newsWire.detail,
      wired: newsWire.status === 'LIVE' || newsWire.status === 'LOW CONFIDENCE' || newsWire.status === 'STALE',
      items: validatedNews,
      stale: newsWire.status === 'STALE',
      bundleChecked: bundleChecked || null,
    },
    eventWeather: {
      focusLine,
      state: calWire.status,
      detail: calWire.detail,
      weatherDetail: wxWire.detail,
      wired: calWire.status === 'LIVE' || calWire.status === 'LOW CONFIDENCE' || wxWire.status === 'LIVE',
      stale: calWire.status === 'STALE' || wxWire.status === 'STALE',
      weatherLines,
      weatherStatus: wxWire.status,
    },
    relatedMarkets: {
      lens: relatedLens,
      state: normalizeWireLabel(relatedState),
      wired: relatedState !== 'NOT WIRED' || relatedFeedLines.length > 0,
      feedLines: relatedFeedLines,
    },
    meta: {
      showLiveBadge,
      bundle_checked_at: bundleChecked || null,
    },
  }
}
