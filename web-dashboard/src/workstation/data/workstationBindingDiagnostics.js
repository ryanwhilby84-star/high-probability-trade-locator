/**
 * Workstation data-binding diagnostics (dev / audit helper).
 */

import { buildCotWorkstation } from '../../cot/buildCotWorkstation.js'
import { getInstrumentPrices } from '../../priceData.js'
import { buildPositioningWorkstationSeries } from './buildPositioningWorkstationSeries.js'

function isNum(v) {
  return typeof v === 'number' && Number.isFinite(v)
}

export function diagnoseWorkstationBinding(marketId, { cotBlock, priceStore, ohlcExportBlock } = {}) {
  const model = cotBlock ? buildCotWorkstation(cotBlock) : { series: [] }
  const priceRec = priceStore?.instruments ? getInstrumentPrices(priceStore, marketId) : null
  const bound = buildPositioningWorkstationSeries(model, priceRec, ohlcExportBlock)

  const cotSeries = model.series || []
  const rows = bound.rows
  const meta = bound.meta || {}
  const range = meta.range || {}

  const timestampsMatch =
    rows.length > 0 &&
    bound.weeklyBars.length > 0 &&
    bound.weeklyBars.every((b) => rows.some((r) => r.date === b.date))

  const commercialPts = rows.filter((r) => isNum(r.commercial_net)).length
  const ncPts = rows.filter((r) => isNum(r.institutional_net)).length
  const nrPts = rows.filter((r) => isNum(r.retail_net)).length
  const pricePts = rows.filter((r) => isNum(r.close)).length

  const first = rows[0]
  const last = rows[rows.length - 1]

  return {
    market: marketId,
    priceSource: bound.priceSource,
    resolvedPriceSymbol: meta.canonicalSymbol ?? ohlcExportBlock?.canonical_symbol ?? null,
    resolvedFrom: meta.resolvedFrom ?? null,
    bindingMeta: meta,
    weeklyPriceBars: bound.weeklyBars.length,
    unifiedRows: rows.length,
    cotSeriesWeeks: cotSeries.length,
    cotRowCount: meta.cotWeeks ?? cotSeries.length,
    ohlcRowCount: meta.storeWeeklyBars ?? bound.weeklyBars.length,
    cotFirstDate: range.cotFirst ?? ohlcExportBlock?.cot_first_date ?? cotBlock?.earliest_date ?? null,
    cotLastDate: range.cotLast ?? ohlcExportBlock?.cot_last_date ?? cotBlock?.latest_date ?? null,
    ohlcFirstDate: range.ohlcFirst ?? ohlcExportBlock?.ohlc_first_date ?? null,
    ohlcLastDate: range.ohlcLast ?? ohlcExportBlock?.ohlc_last_date ?? null,
    commonFirstDate: meta.commonFirst ?? range.commonFirst ?? ohlcExportBlock?.common_first_date ?? null,
    commonLastDate: meta.commonLast ?? range.commonLast ?? ohlcExportBlock?.common_last_date ?? null,
    commonRowCount: meta.commonRows ?? rows.length,
    missingOhlcWeeks: meta.missingOhlcWeeks ?? ohlcExportBlock?.missing_ohlc_weeks ?? 0,
    incompleteHistory: meta.incompleteHistory ?? ohlcExportBlock?.incomplete_history ?? false,
    rangeNote: meta.rangeNote ?? ohlcExportBlock?.note ?? null,
    commercialPoints: commercialPts,
    nonCommercialPoints: ncPts,
    nonReportablePoints: nrPts,
    pricePoints: pricePts,
    firstDate: first?.date ?? null,
    lastDate: last?.date ?? null,
    firstClose: first?.close ?? null,
    lastClose: last?.close ?? null,
    firstCommercial: first?.commercial_net ?? null,
    lastCommercial: last?.commercial_net ?? null,
    timestampsMatchExactly: timestampsMatch,
    storePriceMid: priceRec?.price?.mid ?? null,
  }
}

export function logWorkstationBindingDiagnostics(marketId, diag) {
  const payload = {
    market: marketId,
    ...diag,
  }
  if (marketId === 'NASDAQ / NQ' || diag?.incompleteHistory) {
    console.info('[workstation-binding][coverage]', {
      market: marketId,
      cotRowCount: diag.cotRowCount,
      ohlcRowCount: diag.ohlcRowCount,
      cotFirstDate: diag.cotFirstDate,
      cotLastDate: diag.cotLastDate,
      ohlcFirstDate: diag.ohlcFirstDate,
      ohlcLastDate: diag.ohlcLastDate,
      commonFirstDate: diag.commonFirstDate,
      commonLastDate: diag.commonLastDate,
      commonRowCount: diag.commonRowCount,
      missingOhlcWeeks: diag.missingOhlcWeeks,
      resolvedPriceSymbol: diag.resolvedPriceSymbol,
      priceSource: diag.priceSource,
      resolvedFrom: diag.resolvedFrom,
      incompleteHistory: diag.incompleteHistory,
      rangeNote: diag.rangeNote,
    })
  }
  console.info('[workstation-binding]', payload)
}

