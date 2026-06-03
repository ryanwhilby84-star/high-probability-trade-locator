/**
 * Red-folder economic calendar — row feed or global export only. No invented events.
 */

import {
  formatValues,
  interpretCalendarEvent,
  surpriseLabel,
  surpriseTone,
} from './calendarInterpretation.js'

const NOT_WIRED_MSG = 'NOT WIRED — set FINNHUB_API_KEY (or Trading Economics) and run environment feed update'

const fmt = (v) => (v === null || v === undefined || v === '' ? '—' : v)

function calendarFromRow(row) {
  const feed = row?.market_environment_feed
  if (!feed || typeof feed !== 'object') return null
  const cal = feed.calendar_catalysts
  if (cal && typeof cal === 'object') return cal
  return null
}

function calendarFromGlobal(globalCal, market) {
  if (!globalCal || typeof globalCal !== 'object') {
    return { wired: false, message: NOT_WIRED_MSG, upcoming_high_impact: [], latest_released: [] }
  }
  if (!globalCal.wired) {
    return {
      wired: false,
      message: globalCal.message || NOT_WIRED_MSG,
      upcoming_high_impact: [],
      latest_released: [],
    }
  }
  const events = Array.isArray(globalCal.events) ? globalCal.events : []
  const forMarket = events.filter((e) => (e.affected_markets || []).includes(market))
  const today = new Date().toISOString().slice(0, 10)
  const upcoming = forMarket
    .filter((e) => e.actual == null && String(e.date || '').slice(0, 10) >= today)
    .filter((e) => Number(e.importance_rank) >= 3)
    .slice(0, 10)
  const released = forMarket
    .filter((e) => e.released || e.actual != null)
    .sort((a, b) => String(b.event_timestamp || '').localeCompare(String(a.event_timestamp || '')))
    .slice(0, 10)
  const fallbackUp = (globalCal.upcoming_high_impact || []).filter((e) =>
    (e.affected_markets || []).includes(market),
  )
  const fallbackRel = (globalCal.latest_released || []).filter((e) =>
    (e.affected_markets || []).includes(market),
  )
  return {
    wired: true,
    message: '',
    provider: globalCal.provider,
    upcoming_high_impact: upcoming.length ? upcoming : fallbackUp.slice(0, 10),
    latest_released: released.length ? released : fallbackRel.slice(0, 10),
    event_risk: globalCal.event_risk_by_market?.[market],
  }
}

export function resolveCalendarCatalysts(row, globalCalendar) {
  const market = String(row?.market || '').trim()
  const fromRow = calendarFromRow(row)
  if (fromRow) {
    if (!fromRow.wired && !fromRow.message) fromRow.message = NOT_WIRED_MSG
    return fromRow
  }
  return calendarFromGlobal(globalCalendar, market)
}

function hasReleaseSurprise(ev) {
  if (!ev?.released && ev?.actual == null) return false
  const d = ev.direction_vs_forecast
  if (d !== 'beat' && d !== 'miss') return false
  const mag = ev.magnitude_vs_forecast
  const rank = Number(ev.importance_rank) || 0
  return rank >= 3 || mag === 'medium' || mag === 'large'
}

export function eventRiskBadge(row, globalCalendar) {
  const market = String(row?.market || '').trim()
  if (globalCalendar && globalCalendar.wired === false) return 'not_wired'
  const fromFeed = row?.market_environment_feed?.calendar_catalysts?.event_risk
  if (fromFeed) return fromFeed
  if (globalCalendar?.event_risk_by_market?.[market]) {
    return globalCalendar.event_risk_by_market[market]
  }
  const cal = resolveCalendarCatalysts(row, globalCalendar)
  if (!cal.wired) return 'not_wired'

  const events = Array.isArray(globalCalendar?.events)
    ? globalCalendar.events.filter((e) => (e.affected_markets || []).includes(market))
    : [...(cal.upcoming_high_impact || []), ...(cal.latest_released || [])]

  const today = new Date().toISOString().slice(0, 10)
  const weekEnd = new Date()
  weekEnd.setDate(weekEnd.getDate() + 7)
  const weekEndStr = weekEnd.toISOString().slice(0, 10)
  const lookback = new Date()
  lookback.setDate(lookback.getDate() - 2)
  const lookbackStr = lookback.toISOString().slice(0, 10)

  let highToday = false
  let highWeek = false
  let releasedSurprise = false

  for (const ev of events) {
    const d = String(ev.date || ev.event_timestamp || '').slice(0, 10)
    const rank = Number(ev.importance_rank) || 0
    if (ev.released && d >= lookbackStr && d <= today && hasReleaseSurprise(ev)) {
      releasedSurprise = true
    }
    if (rank < 3 || d < today) continue
    if (d === today) highToday = true
    if (d >= today && d <= weekEndStr) highWeek = true
  }

  if (highToday) return 'high_today'
  if (releasedSurprise) return 'released_surprise'
  if (highWeek) return 'high_this_week'
  return 'clean'
}

export function eventRiskLabel(code) {
  const c = String(code || '').toLowerCase()
  if (c === 'not_wired') return 'NOT WIRED'
  if (c === 'high_today') return 'High today'
  if (c === 'high_this_week') return 'High this week'
  if (c === 'released_surprise') return 'Released surprise'
  if (c === 'clean') return 'Clean'
  return 'NOT WIRED'
}

export function formatEventRow(ev) {
  const vals = formatValues(ev)
  const unit = vals.unit
  const interpretation = interpretCalendarEvent(ev) || fmt(ev.interpretation) || '—'
  const tone = surpriseTone(ev)
  return {
    name: fmt(ev.event_name),
    when: fmt(ev.date || ev.event_timestamp?.slice?.(0, 16)?.replace('T', ' ')),
    country: fmt(ev.country),
    currency: fmt(ev.currency),
    impact: fmt(ev.impact_label || ev.importance),
    impactRank: Number(ev.importance_rank) || 0,
    actual: vals.actual != null ? `${vals.actual}${unit}` : '—',
    forecast: vals.forecast != null ? `${vals.forecast}${unit}` : '—',
    previous: vals.previous != null ? `${vals.previous}${unit}` : '—',
    actualTone: vals.actualTone,
    surprise: surpriseLabel(ev),
    surpriseTone: tone,
    markets: (ev.affected_markets || []).join(', ') || '—',
    interpretation,
    source: fmt(ev.source),
    raw: ev,
  }
}
