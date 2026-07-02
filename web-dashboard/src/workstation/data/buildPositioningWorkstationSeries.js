/**
 * Bind COT weekly series + price OHLC into one shared workstation timeline.
 * Visualization-only — does not alter COT calculations or exports.
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
  return high > low
}

function pickStoreOhlc(bar) {
  if (!bar || !isNum(bar.close)) return null
  const ohlc = {
    open: bar.open,
    high: bar.high,
    low: bar.low,
    close: bar.close,
  }
  return isPlottableWeeklyOhlc(ohlc) ? ohlc : null
}

/**
 * @param {object} model - buildCotWorkstation() output
 * @param {object|null} priceRec - getInstrumentPrices() record
 * @param {object|null} ohlcExportBlock - workstation_ohlc_latest.json instrument block
 * @param {{ preserveFullCotHistory?: boolean }} [options]
 */
export function buildPositioningWorkstationSeries(
  model,
  priceRec,
  ohlcExportBlock = null,
  options = {},
) {
  const preserveFullCotHistory = options.preserveFullCotHistory === true
  const cotSeries = Array.isArray(model?.series) ? model.series : []
  if (!cotSeries.length) {
    return { rows: [], weeklyBars: [], priceSource: 'none', meta: {} }
  }

  const resolved = resolveWorkstationWeeklyOhlc(model.market || null, priceRec, ohlcExportBlock)
  const cotLastDate =
    cotSeries[cotSeries.length - 1]?.date || ohlcExportBlock?.cot_last_date || null
  const { bars: priceBars } = filterCompletedWorkstationOhlc(resolved.weeklyBars || [], {
    cotLastDate,
  })

  const fullRows = []
  const fullWeeklyBars = []
  let alignedOhlcWeeks = 0
  let prevMatchedBarDate = null

  for (const cot of cotSeries) {
    const date = String(cot.date || '').slice(0, 10)
    if (!date) continue
    const time = Math.floor(Date.parse(`${date}T12:00:00Z`) / 1000)
    if (!Number.isFinite(time)) continue

    const storeBar = matchOhlcBarForCotWeek(date, priceBars, prevMatchedBarDate)
    const ohlc = pickStoreOhlc(storeBar)
    if (storeBar?.date) prevMatchedBarDate = storeBar.date

    const row = {
      label: date,
      date,
      time,
      open: ohlc?.open ?? null,
      high: ohlc?.high ?? null,
      low: ohlc?.low ?? null,
      close: ohlc?.close ?? null,
      price: isNum(ohlc?.close) ? ohlc.close : cot.price,
      institutional_net: cot.institutional_net,
      institutional_wow: cot.institutional_wow,
      retail_net: cot.retail_net,
      retail_wow: cot.retail_wow,
      commercial_net: cot.commercial_net,
      commercial_wow: cot.commercial_wow,
    }

    fullRows.push(row)
    if (ohlc) {
      alignedOhlcWeeks += 1
      fullWeeklyBars.push({
        time,
        date,
        open: ohlc.open,
        high: ohlc.high,
        low: ohlc.low,
        close: ohlc.close,
      })
    }
  }

  const range = computeWorkstationCommonRange(fullRows, fullWeeklyBars)
  const useCommon = !preserveFullCotHistory && Boolean(range.commonFirst && range.commonLast)
  const rows = useCommon
    ? sliceRowsToDateRange(fullRows, range.commonFirst, range.commonLast)
    : fullRows
  const weeklyBars = useCommon
    ? sliceBarsToDateRange(fullWeeklyBars, range.commonFirst, range.commonLast)
    : fullWeeklyBars

  const incomplete =
    ohlcExportBlock?.incomplete_history ??
    range.incompleteHistory ??
    Boolean(range.cotFirst && range.ohlcFirst && range.commonFirst && range.commonFirst > range.cotFirst)

  const note =
    ohlcExportBlock?.note ??
    (incomplete ? 'Price OHLC history incomplete — displaying common overlap only.' : null)

  return {
    rows,
    weeklyBars,
    priceSource: resolved.priceSource,
    meta: {
      cotWeeks: cotSeries.length,
      storeWeeklyBars: resolved.weeklyBars?.length ?? 0,
      filteredWeeklyBars: priceBars.length,
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
    },
  }
}

export function sliceWorkstationRows(rows, weeks) {
  if (!Array.isArray(rows) || !rows.length) return []
  if (!weeks || weeks >= rows.length) return rows
  return rows.slice(rows.length - weeks)
}

export function sliceWorkstationBars(bars, weeks) {
  if (!Array.isArray(bars) || !bars.length) return []
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
