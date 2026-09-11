/**
 * Bind COT weekly series + price OHLC into one shared workstation timeline.
 * Visualization-only — does not alter COT calculations or exports.
 *
 * Price candles use completed weekly OHLC dates (provider/store weeks).
 * COT values live ONLY on genuine COT report dates. They are never carried
 * forward onto price-only rows. This matters because carrying Tuesday's report
 * onto a Friday price bar made one COT observation appear as two separate weeks
 * in the chart/inspector and produced duplicated percentile/lookback reads.
 */

import {
  computeWorkstationCommonRange,
  sliceBarsToDateRange,
  sliceRowsToDateRange,
} from './computeWorkstationCommonRange.js'
import { filterCompletedWorkstationOhlc, matchOhlcBarForCotWeek } from './filterWorkstationOhlc.js'
import { resolveWorkstationWeeklyOhlc } from './resolveWorkstationOhlc.js'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function isPlottableWeeklyOhlc(ohlc) {
  if (!ohlc) return false
  const { open, high, low, close } = ohlc
  if (![open, high, low, close].every(isNum)) return false
  if (!(high > low)) return false
  if (high / Math.max(low, 1e-12) > 2.5) return false
  return true
}

function barTime(date) {
  return Math.floor(Date.parse(`${date}T12:00:00Z`) / 1000)
}

function toPriceBar(bar) {
  if (!bar || !isPlottableWeeklyOhlc(bar)) return null
  const date = String(bar.date || '').slice(0, 10)
  const time = Number.isFinite(bar.time) ? bar.time : barTime(date)
  if (!date || !Number.isFinite(time)) return null
  return {
    time,
    date,
    open: bar.open,
    high: bar.high,
    low: bar.low,
    close: bar.close,
  }
}

/**
 * @param {object} model - buildCotWorkstation() output
 * @param {object|null} priceRec - getInstrumentPrices() record
 * @param {object|null} ohlcExportBlock - workstation_ohlc_latest.json instrument block
 * @param {{ preserveFullCotHistory?: boolean, clipToCommonRange?: boolean }} [options]
 */
export function buildPositioningWorkstationSeries(
  model,
  priceRec,
  ohlcExportBlock = null,
  options = {},
) {
  const preserveFullCotHistory = options.preserveFullCotHistory === true
  const clipToCommonRange = options.clipToCommonRange === true
  const cotSeries = Array.isArray(model?.series) ? model.series : []
  if (!cotSeries.length) {
    return { rows: [], weeklyBars: [], priceSource: 'none', meta: {} }
  }

  const resolved = resolveWorkstationWeeklyOhlc(model.market || null, priceRec, ohlcExportBlock)
  const cotLastDate =
    cotSeries[cotSeries.length - 1]?.date || ohlcExportBlock?.cot_last_date || null

  // Completed price weeks only — never capped by COT last.
  const { bars: completedPriceBars } = filterCompletedWorkstationOhlc(resolved.weeklyBars || [], {
    cotLastDate: null,
  })

  const priceByDate = new Map()
  for (const bar of completedPriceBars) {
    const pb = toPriceBar(bar)
    if (pb) priceByDate.set(pb.date, pb)
  }

  // Exact COT report map. A report is one observation and must appear once.
  const cotByDate = new Map()
  for (const cot of cotSeries) {
    const date = String(cot.date || '').slice(0, 10)
    if (!date) continue
    cotByDate.set(date, cot)
  }

  // Unified timeline = all completed price weeks ∪ all genuine COT report weeks.
  // Price and COT can therefore share a camera without inventing extra COT prints.
  const allDates = new Set([
    ...priceByDate.keys(),
    ...cotByDate.keys(),
  ])
  const sortedDates = [...allDates].sort()

  const fullRows = []
  const fullWeeklyBars = []
  let alignedOhlcWeeks = 0

  for (const date of sortedDates) {
    const time = barTime(date)
    if (!Number.isFinite(time)) continue

    const priceBar = priceByDate.get(date) || null

    // CRITICAL: exact report date only. Do not as-of carry a COT print onto a
    // price-only date (for example Tuesday report -> Friday price bar).
    const cotRow = cotByDate.get(date) || null

    const ohlc = priceBar
      ? {
          open: priceBar.open,
          high: priceBar.high,
          low: priceBar.low,
          close: priceBar.close,
        }
      : null

    // A genuine COT report date can fall on a different weekday from the
    // provider's weekly candle. Match price only for that report row so the
    // inspector has a price reference; this does not create another COT row.
    let rowOhlc = ohlc
    if (!rowOhlc && cotRow) {
      const matched = matchOhlcBarForCotWeek(date, completedPriceBars, null)
      if (matched && isPlottableWeeklyOhlc(matched)) {
        rowOhlc = {
          open: matched.open,
          high: matched.high,
          low: matched.low,
          close: matched.close,
        }
      }
    }

    const row = {
      label: date,
      date,
      time,
      open: rowOhlc?.open ?? null,
      high: rowOhlc?.high ?? null,
      low: rowOhlc?.low ?? null,
      close: rowOhlc?.close ?? null,
      price: isNum(rowOhlc?.close) ? rowOhlc.close : cotRow?.price ?? null,
      institutional_net: cotRow?.institutional_net ?? null,
      institutional_wow: cotRow?.institutional_wow ?? null,
      retail_net: cotRow?.retail_net ?? null,
      retail_wow: cotRow?.retail_wow ?? null,
      commercial_net: cotRow?.commercial_net ?? null,
      commercial_wow: cotRow?.commercial_wow ?? null,
      isCotReport: Boolean(cotRow),
    }

    fullRows.push(row)
    if (priceBar) {
      alignedOhlcWeeks += 1
      fullWeeklyBars.push(priceBar)
    } else if (rowOhlc) {
      alignedOhlcWeeks += 1
      fullWeeklyBars.push({
        time,
        date,
        open: rowOhlc.open,
        high: rowOhlc.high,
        low: rowOhlc.low,
        close: rowOhlc.close,
      })
    }
  }

  // Prefer pure completed provider price bars as the candle series.
  const priceOnlyBars = completedPriceBars.map(toPriceBar).filter(Boolean)

  const range = computeWorkstationCommonRange(fullRows, priceOnlyBars)

  // The workstation price pane retains the provider's full completed weekly
  // history. Common-range clipping is diagnostics-only and explicit.
  const useCommon = clipToCommonRange && !preserveFullCotHistory && Boolean(range.commonFirst && range.commonLast)
  const rows = useCommon
    ? sliceRowsToDateRange(fullRows, range.commonFirst, range.commonLast)
    : fullRows
  const weeklyBars = useCommon
    ? sliceBarsToDateRange(priceOnlyBars, range.commonFirst, range.commonLast)
    : priceOnlyBars

  const incomplete =
    ohlcExportBlock?.incomplete_history ??
    range.incompleteHistory ??
    Boolean(range.cotFirst && range.ohlcFirst && range.commonFirst && range.commonFirst > range.cotFirst)

  const note =
    ohlcExportBlock?.note ??
    (incomplete ? 'Price OHLC history incomplete — full available price history retained.' : null)

  return {
    rows,
    weeklyBars,
    priceSource: resolved.priceSource,
    meta: {
      cotWeeks: cotSeries.filter((row) => isNum(row?.commercial_net) || isNum(row?.institutional_net) || isNum(row?.retail_net)).length,
      storeWeeklyBars: resolved.weeklyBars?.length ?? 0,
      filteredWeeklyBars: completedPriceBars.length,
      alignedOhlcWeeks,
      resolvedFrom: resolved.resolvedFrom,
      canonicalSymbol: ohlcExportBlock?.canonical_symbol ?? resolved.exportMeta?.canonical_symbol ?? null,
      range,
      commonFirst: range.commonFirst,
      commonLast: range.commonLast,
      commonRows: rows.length,
      missingOhlcWeeks: range.missingOhlcWeeks,
      incompleteHistory: incomplete,
      rangeNote: note,
      cotLastDate,
      priceLastDate: weeklyBars[weeklyBars.length - 1]?.date ?? null,
      priceNotTruncatedToCot: !useCommon,
      clippedToCommonRange: useCommon,
      cotCarriedOntoPriceRows: false,
    },
  }
}

export function sliceWorkstationRows(rows, weeks) {
  if (!Array.isArray(rows) || !rows.length) return rows || []
  if (!weeks || weeks >= rows.length) return rows
  return rows.slice(rows.length - weeks)
}

export function sliceWorkstationBars(bars, weeks) {
  if (!Array.isArray(bars) || !bars.length) return bars || []
  if (!weeks || weeks >= bars.length) return bars
  return bars.slice(bars.length - weeks)
}

export function rowsToWeeklyBars(rows) {
  const isNumLocal = (v) => typeof v === 'number' && Number.isFinite(v)
  return (rows || [])
    .map((r) => ({
      time: r.time,
      date: r.date,
      open: r.open,
      high: r.high,
      low: r.low,
      close: r.close,
    }))
    .filter(
      (b) =>
        isNumLocal(b.time) &&
        isNumLocal(b.open) &&
        isNumLocal(b.high) &&
        isNumLocal(b.low) &&
        isNumLocal(b.close) &&
        b.high > b.low,
    )
}

/** @deprecated visualization-only — retained for diagnostics scripts */
export function shouldUseStoreOhlc() {
  return false
}

export function detectCotPriceScaleBreakIndex(cotSeries) {
  if (!Array.isArray(cotSeries) || cotSeries.length < 2) return -1
  for (let i = 1; i < cotSeries.length; i++) {
    const prev = cotSeries[i - 1]?.price
    const cur = cotSeries[i]?.price
    if (!isNum(prev) || !isNum(cur) || prev <= 0) continue
    const ratio = cur / prev
    if (ratio < 0.35 || ratio > 2.75) return i
  }
  return -1
}
