/**
 * Normalize weekly OHLC bars from prices_latest.json for workstation charts.
 */

import { resolveWeeklyOhlc } from './deriveWeeklyOhlc.js'

export { deriveWeeklyOhlcFromDaily, resolveWeeklyOhlc } from './deriveWeeklyOhlc.js'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

/**
 * @param {Array<{date, open, high, low, close, volume}>} weekly
 * @returns {Array<{time: number, open, high, low, close, date: string}>}
 */
export function normalizeWeeklyOhlc(weekly) {
  if (!Array.isArray(weekly)) return []
  const out = []
  for (const bar of weekly) {
    const date = String(bar?.date || '').slice(0, 10)
    if (!date) continue
    const open = Number(bar.open)
    const high = Number(bar.high)
    const low = Number(bar.low)
    const close = Number(bar.close)
    if (![open, high, low, close].every(isNum)) continue
    const time = Math.floor(Date.parse(`${date}T12:00:00Z`) / 1000)
    if (!Number.isFinite(time)) continue
    out.push({ time, date, open, high, low, close })
  }
  out.sort((a, b) => a.time - b.time)
  return out
}

/**
 * Align valuation history rows to weekly bar dates.
 * Uses as-of match (latest valuation date on or before bar date) because
 * export weeks (e.g. COT Friday) may differ from derived ISO week-end dates.
 */
export function alignValuationToWeekly(valuationSeries, weeklyBars) {
  const sorted = [...(valuationSeries || [])]
    .map((row) => ({
      ...row,
      date: String(row?.date || '').slice(0, 10),
    }))
    .filter((row) => row.date)
    .sort((a, b) => a.date.localeCompare(b.date))

  return weeklyBars.map((bar) => {
    let val = null
    for (const row of sorted) {
      if (row.date <= bar.date) val = row
      else break
    }
    return {
      time: bar.time,
      date: bar.date,
      valuation_as_of: val?.date ?? null,
      fair_value: isNum(val?.fair_value) ? val.fair_value : null,
      deviation_pct: isNum(val?.deviation_pct) ? val.deviation_pct : null,
      publish: val?.publish ?? null,
      model_id: val?.model_id ?? null,
    }
  })
}

/**
 * Merge OHLC + valuation into unified timeline rows for cross-panel sync.
 */
export function buildWeeklyTimelineRows(weeklyBars, valuationAligned) {
  const valByTime = new Map((valuationAligned || []).map((r) => [r.time, r]))
  return weeklyBars.map((bar) => {
    const val = valByTime.get(bar.time) || {}
    return {
      ...bar,
      fair_value: val.fair_value ?? null,
      deviation_pct: val.deviation_pct ?? null,
      valuation_publish: val.publish ?? null,
      model_id: val.model_id ?? null,
    }
  })
}

export function findTimelineRowByTime(rows, time) {
  if (!time || !rows?.length) return null
  let best = null
  for (const row of rows) {
    if (row.time <= time) best = row
    else break
  }
  return best
}
