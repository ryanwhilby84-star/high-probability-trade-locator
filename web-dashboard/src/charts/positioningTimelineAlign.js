/**
 * Align COT report weeks (cot_3y_series dates) with weekly OHLC bar timestamps.
 * Visualization-only — no COT or price calculations changed.
 */

export function barTimeToDate(barTime) {
  if (barTime == null) return null
  return new Date(barTime * 1000).toISOString().slice(0, 10)
}

/** Latest bar with bar.date <= cotDate. */
export function cotDateToBarTime(bars, cotDate) {
  if (!cotDate || !Array.isArray(bars) || !bars.length) return null
  const d = String(cotDate).slice(0, 10)
  let best = null
  for (const bar of bars) {
    if (bar.date <= d) best = bar
    else break
  }
  return best?.time ?? null
}

/** Latest COT row with row.date <= barDate. */
export function findCotRowAsOf(series, barDate) {
  if (!barDate || !Array.isArray(series) || !series.length) return null
  const d = String(barDate).slice(0, 10)
  let best = null
  for (const row of series) {
    const rd = String(row?.date || row?.label || '').slice(0, 10)
    if (!rd) continue
    if (rd <= d) best = row
    else break
  }
  return best
}

export function sliceBarsByDateRange(bars, fromDate, toDate) {
  if (!Array.isArray(bars) || !bars.length) return []
  const from = fromDate ? String(fromDate).slice(0, 10) : null
  const to = toDate ? String(toDate).slice(0, 10) : null
  return bars.filter((b) => {
    if (from && b.date < from) return false
    if (to && b.date > to) return false
    return true
  })
}

export function sliceSeriesByDateRange(series, fromDate, toDate) {
  if (!Array.isArray(series) || !series.length) return []
  const from = fromDate ? String(fromDate).slice(0, 10) : null
  const to = toDate ? String(toDate).slice(0, 10) : null
  return series.filter((row) => {
    const d = String(row?.date || row?.label || '').slice(0, 10)
    if (!d) return false
    if (from && d < from) return false
    if (to && d > to) return false
    return true
  })
}

export function unixRangeFromSeries(series) {
  if (!Array.isArray(series) || !series.length) return null
  const first = String(series[0]?.date || series[0]?.label || '').slice(0, 10)
  const last = String(series[series.length - 1]?.date || series[series.length - 1]?.label || '').slice(0, 10)
  if (!first || !last) return null
  return {
    from: Math.floor(Date.parse(`${first}T00:00:00Z`) / 1000),
    to: Math.floor(Date.parse(`${last}T23:59:59Z`) / 1000),
  }
}

export function unixRangeFromBars(bars) {
  if (!Array.isArray(bars) || !bars.length) return null
  return { from: bars[0].time, to: bars[bars.length - 1].time }
}
