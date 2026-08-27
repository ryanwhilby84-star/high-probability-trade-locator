/**
 * Workstation OHLC filters — visualization only.
 *
 * Drops the incomplete current ISO week only.
 * Price history must NEVER be truncated to the latest COT report.
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

function isPlottableBar(bar) {
  if (!bar) return false
  const { open, high, low, close } = bar
  return [open, high, low, close].every(isNum) && high > low
}

/**
 * Keep completed weekly OHLC only (drop the in-progress ISO week).
 * `cotLastDate` is accepted for API compatibility but must not truncate price.
 *
 * @param {Array} bars
 * @param {{ cotLastDate?: string|null, asOf?: Date|null }} [opts]
 */
export function filterCompletedWorkstationOhlc(bars, { cotLastDate = null, asOf = null } = {}) {
  void cotLastDate // intentionally unused — price must continue past COT
  const rejected = []
  const kept = []
  const curWeek = currentIsoWeekKey(asOf || new Date())

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

  // Prefer the latest completed price bar when it is after the COT-matched tip.
  const latestPrice = priceBars[priceBars.length - 1]
  if (latestPrice?.date && (!lastMatch?.date || latestPrice.date > lastMatch.date)) {
    return latestPrice.date
  }

  return lastMatch?.date || end
}

export { isoWeekKey, currentIsoWeekKey }
