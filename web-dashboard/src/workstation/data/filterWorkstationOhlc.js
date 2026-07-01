/**
 * Workstation OHLC filters — visualization only.
 * Drops partial ISO weeks and bars beyond the latest closed COT date.
 */

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function isoWeekKey(dateStr) {
  if (!dateStr) return ''
  const d = new Date(`${String(dateStr).slice(0, 10)}T12:00:00Z`)
  if (Number.isNaN(d.getTime())) return ''
  const thursday = new Date(d)
  thursday.setUTCDate(d.getUTCDate() + 3 - ((d.getUTCDay() + 6) % 7))
  const yearStart = new Date(Date.UTC(thursday.getUTCFullYear(), 0, 1))
  const week = Math.ceil(((thursday - yearStart) / 86400000 + 1) / 7)
  return `${thursday.getUTCFullYear()}-W${String(week).padStart(2, '0')}`
}

function currentIsoWeekKey(asOf = new Date()) {
  return isoWeekKey(asOf.toISOString().slice(0, 10))
}

function daysBetween(a, b) {
  const da = Date.parse(`${String(a).slice(0, 10)}T00:00:00Z`)
  const db = Date.parse(`${String(b).slice(0, 10)}T00:00:00Z`)
  if (!Number.isFinite(da) || !Number.isFinite(db)) return Infinity
  return Math.abs(db - da) / 86400000
}

/** Last calendar day (Sunday) of the ISO week containing `dateStr`. */
function endOfIsoWeekDate(dateStr) {
  const d = new Date(`${String(dateStr).slice(0, 10)}T12:00:00Z`)
  if (Number.isNaN(d.getTime())) return null
  const day = d.getUTCDay()
  const daysToSunday = day === 0 ? 0 : 7 - day
  const sun = new Date(d)
  sun.setUTCDate(d.getUTCDate() + daysToSunday)
  return sun.toISOString().slice(0, 10)
}

function isPlottableBar(bar) {
  if (!bar) return false
  const { open, high, low, close } = bar
  return [open, high, low, close].every(isNum) && high > low
}

/**
 * @param {Array} bars - normalized weekly bars { date, open, high, low, close, time? }
 * @param {object} opts
 * @param {string|null} opts.cotLastDate
 * @param {Date|null} opts.asOf
 */
export function filterCompletedWorkstationOhlc(bars, { cotLastDate = null, asOf = null } = {}) {
  const rejected = []
  const kept = []
  const curWeek = currentIsoWeekKey(asOf || new Date())
  const cotLast = cotLastDate ? String(cotLastDate).slice(0, 10) : null
  const cotCap = cotLast ? endOfIsoWeekDate(cotLast) : null

  for (const bar of bars || []) {
    if (!isPlottableBar(bar)) {
      rejected.push({ bar, reason: 'invalid_ohlc' })
      continue
    }
    const date = String(bar.date).slice(0, 10)
    const wk = isoWeekKey(date)
    if (wk >= curWeek) {
      rejected.push({ bar, reason: 'incomplete_iso_week', iso_week: wk })
      continue
    }
    if (cotCap && date > cotCap) {
      rejected.push({ bar, reason: 'after_cot_last', cot_last: cotLast, cot_cap: cotCap })
      continue
    }
    kept.push(bar)
  }

  return { bars: kept, rejected }
}

/**
 * Match one OHLC bar to a COT report week — no stale reuse across forward-filled COT rows.
 */
export function matchOhlcBarForCotWeek(cotDate, priceBars, prevMatchedBarDate = null) {
  if (!cotDate || !priceBars?.length) return null
  const d = String(cotDate).slice(0, 10)
  const cotWeek = isoWeekKey(d)

  let sameWeekBar = null
  for (const bar of priceBars) {
    if (isoWeekKey(bar.date) === cotWeek && isPlottableBar(bar)) {
      sameWeekBar = bar
    }
  }

  let best = sameWeekBar
  if (!best) {
    for (const bar of priceBars) {
      if (bar.date <= d) best = bar
      else break
    }
  }
  if (!best || !isPlottableBar(best)) return null

  if (prevMatchedBarDate && best.date === prevMatchedBarDate) {
    return null
  }

  const gap = daysBetween(best.date, d)
  if (gap > 14) return null

  return best
}

/**
 * OHLC slice end for chart visibility: extend through matched OHLC week for the final COT row.
 * @param {string} cotEndDate - last COT week in the visible range
 * @param {Array} priceBars - filtered weekly OHLC bars (OHLC week dates)
 * @param {string[]} cotDatesInRange - COT weeks in chart order (for prev-match chain)
 */
export function resolveWorkstationVisibleOhlcEnd(cotEndDate, priceBars, cotDatesInRange) {
  const end = String(cotEndDate || '').slice(0, 10)
  if (!end || !priceBars?.length) return end || null

  const dates = Array.isArray(cotDatesInRange) && cotDatesInRange.length
    ? cotDatesInRange.map((d) => String(d).slice(0, 10)).filter((d) => d && d <= end)
    : [end]

  let prevMatched = null
  let lastMatch = null
  for (const cot of dates) {
    const m = matchOhlcBarForCotWeek(cot, priceBars, prevMatched)
    if (m?.date) {
      prevMatched = m.date
      lastMatch = m
    }
  }

  return lastMatch?.date || end
}

export { isoWeekKey, currentIsoWeekKey }
