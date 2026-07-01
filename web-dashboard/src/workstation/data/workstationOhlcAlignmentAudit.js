import { filterCompletedWorkstationOhlc, matchOhlcBarForCotWeek } from './filterWorkstationOhlc.js'

const TAIL = 12

function tail(arr, n = TAIL) {
  return Array.isArray(arr) ? arr.slice(-n) : []
}

/**
 * Visualization diagnostics — final OHLC/COT alignment tail per instrument.
 */
export function buildWorkstationOhlcAlignmentAudit(marketId, model, priceBars, exportBlock = null) {
  const cotSeries = Array.isArray(model?.series) ? model.series : []
  const cotLast = cotSeries[cotSeries.length - 1]?.date || exportBlock?.cot_last_date || null

  const { bars: filteredBars, rejected } = filterCompletedWorkstationOhlc(priceBars, {
    cotLastDate: cotLast,
  })

  const cotTail = tail(cotSeries).map((r) => ({
    date: String(r.date || '').slice(0, 10),
    price: r.price ?? null,
  }))

  const ohlcTail = tail(priceBars).map((b) => ({
    date: b.date,
    open: b.open,
    high: b.high,
    low: b.low,
    close: b.close,
  }))

  const filteredTail = tail(filteredBars).map((b) => ({
    date: b.date,
    close: b.close,
  }))

  const matched = []
  let prevBarDate = null
  for (const row of tail(cotSeries)) {
    const cotDate = String(row.date || '').slice(0, 10)
    const bar = matchOhlcBarForCotWeek(cotDate, filteredBars, prevBarDate)
    matched.push({
      cot_date: cotDate,
      ohlc_date: bar?.date ?? null,
      close: bar?.close ?? null,
      cot_price: row.price ?? null,
      matched: Boolean(bar),
    })
    if (bar?.date) prevBarDate = bar.date
  }

  const rejectedTail = tail(rejected).map((r) => ({
    date: r.bar?.date ?? null,
    reason: r.reason,
    iso_week: r.iso_week ?? null,
  }))

  return {
    instrument: marketId,
    cot_last_date: cotLast,
    ohlc_last_raw: priceBars[priceBars.length - 1]?.date ?? null,
    ohlc_last_filtered: filteredBars[filteredBars.length - 1]?.date ?? null,
    final_12_ohlc_raw: ohlcTail,
    final_12_ohlc_filtered: filteredTail,
    final_12_cot_weeks: cotTail,
    final_12_matched: matched,
    rejected_partial_week_rows: rejectedTail,
    rejected_count: rejected.length,
  }
}

export function logWorkstationOhlcAlignmentAudit(marketId, audit) {
  if (!audit || typeof console === 'undefined') return
  console.info(`[workstation] OHLC/COT alignment tail — ${marketId}`, audit)
}
