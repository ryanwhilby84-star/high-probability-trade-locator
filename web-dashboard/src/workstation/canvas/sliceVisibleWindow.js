import { barTimeToDate, sliceBarsByDateRange, sliceSeriesByDateRange } from '../../charts/positioningTimelineAlign.js'

/**
 * Apply interactive candle zoom window on top of preset-sliced series.
 */
export function sliceByVisibleTimeRange(series, bars, visibleTimeRange) {
  if (!visibleTimeRange?.from || !visibleTimeRange?.to) {
    return { series: series || [], bars: bars || [] }
  }
  const fromDate = barTimeToDate(visibleTimeRange.from)
  const toDate = barTimeToDate(visibleTimeRange.to)
  if (!fromDate || !toDate) {
    return { series: series || [], bars: bars || [] }
  }
  return {
    series: sliceSeriesByDateRange(series, fromDate, toDate),
    bars: sliceBarsByDateRange(bars, fromDate, toDate),
  }
}

export function labelFromCrosshairTime(series, crosshairTime) {
  if (!crosshairTime || !Array.isArray(series) || !series.length) return null
  const d = barTimeToDate(crosshairTime)
  if (!d) return null
  let best = null
  for (const row of series) {
    const rd = String(row?.date || row?.label || '').slice(0, 10)
    if (!rd) continue
    if (rd <= d) best = row
    else break
  }
  return best?.label || best?.date || d
}
