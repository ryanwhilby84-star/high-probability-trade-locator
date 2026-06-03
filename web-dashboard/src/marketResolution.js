/**
 * Canonical market IDs and COT row resolution against confluence export records.
 * Resolves staggered CFTC report dates (e.g. indices through 2026-05-05 vs commodities 2026-05-12).
 */

export const TRACKED_MARKET_IDS = [
  'NASDAQ / NQ',
  'S&P 500 / ES',
  'Dow / YM',
  'Euro FX / 6E',
  'British Pound / 6B',
  'Japanese Yen / 6J',
  'Swiss Franc / 6S',
  'Australian Dollar / 6A',
  'Canadian Dollar / 6C',
  'NZ Dollar / 6N',
  'Gold',
  'Silver',
  'Copper / HG',
  'Crude Oil / CL',
  'Natural Gas / NG',
  'Coffee',
  'Cocoa',
  'Corn',
  'Wheat',
  'Soybeans',
]

const norm = (m = '') => String(m || '').toLowerCase().trim()

/**
 * FX cross pattern like "NZD/USD", "GBP/NZD" (3-letter / 3-letter).
 * These are distinct instruments and must NOT be collapsed onto a single currency-leg
 * COT market — doing so is what made many pairs reuse the same leg's COT table.
 */
const FX_PAIR_RE = /^([A-Za-z]{3})\/([A-Za-z]{3})$/

/** @param {string} market */
export function canonicalMarketId(market = '') {
  const trimmed = String(market || '').trim()
  const pair = trimmed.match(FX_PAIR_RE)
  if (pair) return `${pair[1].toUpperCase()}/${pair[2].toUpperCase()}`

  const m = norm(market)
  if (m.includes('nasdaq') || m.includes('/ nq')) return 'NASDAQ / NQ'
  if (m.includes('s&p') || m.includes('sp 500') || m.includes('/ es')) return 'S&P 500 / ES'
  if (m.includes('dow') || m.includes('djia') || m.includes('s30') || m.includes('/ ym')) return 'Dow / YM'
  if (m.includes('euro fx') || m.includes('/ 6e') || (m.includes('eur') && m.includes('fx'))) return 'Euro FX / 6E'
  if (m.includes('british pound') || m.includes('/ 6b') || m.includes('gbp')) return 'British Pound / 6B'
  if (m.includes('japanese yen') || m.includes('/ 6j') || m.includes('jpy')) return 'Japanese Yen / 6J'
  if (m.includes('swiss franc') || m.includes('/ 6s') || m.includes('chf')) return 'Swiss Franc / 6S'
  if (m.includes('australian dollar') || m.includes('/ 6a') || m.includes('aud')) return 'Australian Dollar / 6A'
  if (m.includes('canadian dollar') || m.includes('/ 6c') || m.includes('cad')) return 'Canadian Dollar / 6C'
  if (m.includes('nz dollar') || m.includes('new zealand') || m.includes('/ 6n') || m.includes('nzd')) return 'NZ Dollar / 6N'
  if (m.includes('gold') || m.includes('/ gc')) return 'Gold'
  if (m.includes('silver') || m.includes('/ si')) return 'Silver'
  if (m.includes('copper') || m.includes('/ hg')) return 'Copper / HG'
  if (m.includes('crude oil') || m.includes('/ cl')) return 'Crude Oil / CL'
  if (m.includes('natural gas') || m.includes('/ ng')) return 'Natural Gas / NG'
  if (m.includes('coffee') || m.includes('/ kc')) return 'Coffee'
  if (m.includes('cocoa') || m.includes('/ cc')) return 'Cocoa'
  if (m.includes('corn') || m.includes('/ zc')) return 'Corn'
  if (m.includes('wheat') || m.includes('/ zw')) return 'Wheat'
  if (m.includes('soybeans') || m.includes('/ zs')) return 'Soybeans'
  if (trimmed.includes('/')) return trimmed
  return trimmed
}

/** @param {unknown} v */
export function normalizeReportDate(v = '') {
  const s = String(v ?? '').trim()
  if (!s) return ''
  const head = s.slice(0, 10)
  if (/^\d{4}-\d{2}-\d{2}$/.test(head)) return head
  const d = new Date(s)
  if (!Number.isNaN(d.getTime())) {
    return d.toISOString().slice(0, 10)
  }
  return head
}

/** Calendar week anchor on a record (dropdown / week filter). */
export function recordCalendarDate(r = {}) {
  return normalizeReportDate(r.date || r.week || '')
}

/** Actual CFTC report date when present. */
export function recordCotReportDate(r = {}) {
  return normalizeReportDate(r.cot_report_date || r.latest_report_date || r.date || '')
}

const NA_COT = 'N/A: no COT row for this market and date'

/** @param {object} row */
export function isCotRowResolved(row) {
  if (!row || typeof row !== 'object') return false
  const bias = String(row.cot_bias ?? '').trim()
  if (!bias || bias.toUpperCase() === 'N/A') return false
  const flow = String(row.institutional_flow_summary ?? '')
  if (flow.includes(NA_COT)) return false
  const reason = String(row.missing_reason ?? '')
  if (reason.includes('no mapped raw COT row')) return false
  return true
}

/**
 * @param {object[]} records
 * @param {string} market
 * @param {string} weekDate calendar week (YYYY-MM-DD)
 */
export function resolveRowForMarketWeek(records, market, weekDate) {
  const marketId = canonicalMarketId(market)
  const week = normalizeReportDate(weekDate)
  if (!marketId || !week || !Array.isArray(records)) {
    return { row: null, matchMode: 'none', matchedCount: 0, matchedMarkets: [], matchedDates: [] }
  }

  const forMarket = records.filter(
    (r) => canonicalMarketId(r.market || r.raw_cftc_market_name || '') === marketId,
  )

  const exact = forMarket.filter((r) => recordCalendarDate(r) === week)
  const exactResolved = exact.filter(isCotRowResolved)
  if (exactResolved.length) {
    const row = pickBestRow(exactResolved)
    return packResult(row, 'exact_calendar_week', exactResolved)
  }

  const backward = forMarket
    .filter((r) => recordCalendarDate(r) <= week && isCotRowResolved(r))
    .sort((a, b) => recordCalendarDate(b).localeCompare(recordCalendarDate(a)))
  if (backward.length) {
    return packResult(backward[0], 'backward_calendar_week', backward)
  }

  const cotBackward = forMarket
    .filter((r) => recordCotReportDate(r) <= week && isCotRowResolved(r))
    .sort((a, b) => recordCotReportDate(b).localeCompare(recordCotReportDate(a)))
  if (cotBackward.length) {
    return packResult(cotBackward[0], 'backward_cot_report_date', cotBackward)
  }

  if (exact.length) {
    return packResult(pickBestRow(exact), 'exact_unresolved', exact)
  }

  return { row: null, matchMode: 'none', matchedCount: 0, matchedMarkets: [], matchedDates: [] }
}

function pickBestRow(rows) {
  return [...rows].sort((a, b) => completenessScore(b) - completenessScore(a))[0]
}

function completenessScore(row = {}) {
  const fields = [
    row.cot_bias,
    row.cot_score,
    row.long_value,
    row.short_value,
    row.net_value,
    row.institutional_flow_summary,
    row.positioning_state,
  ]
  return fields.reduce((n, v) => n + (hasReal(v) ? 1 : 0), 0)
}

function hasReal(v) {
  if (v === null || v === undefined) return false
  const s = String(v).trim().toLowerCase()
  return Boolean(s) && s !== 'n/a' && s !== 'nan'
}

function packResult(row, matchMode, pool) {
  return {
    row,
    matchMode,
    matchedCount: pool.length,
    matchedMarkets: [...new Set(pool.map((r) => r.market).filter(Boolean))],
    matchedDates: [...new Set(pool.map((r) => recordCalendarDate(r)).filter(Boolean))],
    matchedCotDates: [...new Set(pool.map((r) => recordCotReportDate(r)).filter(Boolean))],
  }
}

/** Default week: latest calendar date where all three equity indices resolve. */
export function defaultDashboardWeek(records, fallback = '') {
  const dates = [...new Set(records.map((r) => recordCalendarDate(r)).filter(Boolean))].sort()
  const indices = ['NASDAQ / NQ', 'S&P 500 / ES', 'Dow / YM']
  for (let i = dates.length - 1; i >= 0; i -= 1) {
    const d = dates[i]
    if (indices.every((m) => isCotRowResolved(resolveRowForMarketWeek(records, m, d).row))) {
      return d
    }
  }
  return dates.at(-1) || normalizeReportDate(fallback) || ''
}

export function cotDebugEnabled() {
  try {
    if (typeof window === 'undefined') return false
    if (window.localStorage?.getItem('hptl_cot_debug') === '1') return true
    return new URLSearchParams(window.location.search).get('cot_debug') === '1'
  } catch {
    return false
  }
}

/** @param {string} weekDate */
export function logCotResolutionForWeek(records, weekDate, markets = TRACKED_MARKET_IDS) {
  if (!cotDebugEnabled()) return
  const week = normalizeReportDate(weekDate)
  console.group(`[HPTL COT resolve] calendar week ${week}`)
  markets.forEach((market) => {
    const res = resolveRowForMarketWeek(records, market, week)
    const row = res.row
    console.log({
      requestedMarket: market,
      requestedDate: week,
      matchMode: res.matchMode,
      matchedCount: res.matchedCount,
      matchedMarkets: res.matchedMarkets,
      matchedCalendarDates: res.matchedDates,
      matchedCotReportDates: res.matchedCotDates,
      resolved: isCotRowResolved(row),
      cot_bias: row?.cot_bias,
      cot_score: row?.cot_score,
      cot_report_date: row ? recordCotReportDate(row) : null,
    })
  })
  console.groupEnd()
}
