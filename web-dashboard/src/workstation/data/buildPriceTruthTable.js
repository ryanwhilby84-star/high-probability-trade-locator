/**
 * Price truth table — diagnostic for every displayed price layer.
 * All values traced to authoritative stores.
 */

import { LivePriceStore } from '../../prices/stores/LivePriceStore.js'
import { WeeklyOHLCStore } from '../../prices/stores/WeeklyOHLCStore.js'
import { HistoricalCOTStore } from '../../prices/stores/HistoricalCOTStore.js'
import { applyLivePriceToIveDisplay } from './instrumentPriceDiagnostics.js'
import { readIVE } from '../../valuation/iveDisplay.js'
import { validatePriceEngine } from '../../prices/priceEngineValidation.js'

export const TV_AUDIT_STORAGE_KEY = 'hptl_tv_audit_price'
export const GOLD_TV_AUDIT_STORAGE_KEY = TV_AUDIT_STORAGE_KEY
export const GOLD_INSTRUMENT = 'Gold'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function fmtVal(v) {
  if (v == null || v === '') return '—'
  if (isNum(v)) return v.toFixed(3)
  return String(v)
}

function tableRow({ field, value, source, timestamp, component, category, store }) {
  return {
    field,
    value: isNum(value) ? value : value ?? null,
    valueDisplay: fmtVal(value),
    source: source ?? '—',
    timestamp: timestamp ?? '—',
    component: component ?? '—',
    category: category ?? '—',
    store: store ?? '—',
  }
}

/**
 * @param {object} input
 * @param {string} input.marketId
 * @param {object} [input.priceContext]
 * @param {object} [input.displaySnapshot]
 * @param {object} [input.valuationBlock]
 * @param {number|null} [input.tradingViewAuditPrice]
 */
export function buildPriceTruthTable(input = {}) {
  const {
    marketId,
    priceContext = {},
    displaySnapshot = {},
    valuationBlock,
    tradingViewAuditPrice,
  } = input

  if (!marketId) return { marketId: null, table: [], comparisons: [] }

  const liveQuote = LivePriceStore.getQuote(marketId)
  const liveSnap = LivePriceStore.getSnapshot()
  const weekly = WeeklyOHLCStore.getCompletedWeekly(marketId)
  const weeklySnap = WeeklyOHLCStore.getSnapshot()
  const cotSnap = HistoricalCOTStore.getSnapshot()
  const liveStatus = LivePriceStore.getStatus(marketId)

  const cotDate = displaySnapshot.crosshairHeaderDate ?? displaySnapshot.candleTooltipDate ?? null
  const cotHistorical = cotDate
    ? HistoricalCOTStore.getHistoricalCloseAtDate(marketId, cotDate)
    : null

  const baseIve = readIVE(valuationBlock)
  const valuationIve = applyLivePriceToIveDisplay(baseIve, marketId)

  const table = [
    tableRow({
      field: 'Live OANDA bid',
      value: liveQuote?.bid ?? null,
      source: liveQuote?.source ?? 'OANDA',
      timestamp: liveQuote?.asOf ?? null,
      component: 'LivePriceStore',
      category: 'LIVE',
      store: LivePriceStore.STORE_NAME,
    }),
    tableRow({
      field: 'Live OANDA ask',
      value: liveQuote?.ask ?? null,
      source: liveQuote?.source ?? 'OANDA',
      timestamp: liveQuote?.asOf ?? null,
      component: 'LivePriceStore',
      category: 'LIVE',
      store: LivePriceStore.STORE_NAME,
    }),
    tableRow({
      field: 'Live OANDA mid',
      value: liveQuote?.mid ?? null,
      source: liveQuote?.source ?? 'OANDA',
      timestamp: liveQuote?.asOf ?? null,
      component: 'Live marker · valuation · header labels',
      category: liveStatus === 'STALE' ? 'STALE LIVE' : liveStatus === 'UNAVAILABLE' ? 'LIVE UNAVAILABLE' : 'LIVE',
      store: LivePriceStore.STORE_NAME,
    }),
    tableRow({
      field: 'Completed weekly close',
      value: weekly?.close ?? null,
      source: WeeklyOHLCStore.getPriceSource(marketId),
      timestamp: weekly?.date ?? null,
      component: 'Weekly chart · CandleBadge',
      category: 'WEEKLY_CLOSE',
      store: WeeklyOHLCStore.STORE_NAME,
    }),
    tableRow({
      field: 'COT historical price (crosshair)',
      value: cotHistorical,
      source: 'cot_3y_series_latest.json',
      timestamp: cotDate,
      component: 'ChartCrosshairHeader',
      category: 'COT_HISTORICAL',
      store: HistoricalCOTStore.STORE_NAME,
    }),
    tableRow({
      field: 'Live marker (rendered)',
      value: priceContext.liveMid ?? priceContext.livePrice ?? null,
      source: priceContext.livePriceSource ?? LivePriceStore.STORE_NAME,
      timestamp: priceContext.livePriceAsOf ?? null,
      component: 'WorkstationChartPane live line',
      category: 'LIVE',
      store: LivePriceStore.STORE_NAME,
    }),
    tableRow({
      field: 'Header weekly close label',
      value: priceContext.weeklyClose ?? null,
      source: priceContext.weeklyOhlcSource ?? WeeklyOHLCStore.STORE_NAME,
      timestamp: priceContext.weeklyCloseDate ?? null,
      component: 'SynchronizedWorkstationPanels header',
      category: 'WEEKLY_CLOSE',
      store: WeeklyOHLCStore.STORE_NAME,
    }),
    tableRow({
      field: 'Valuation display price',
      value: valuationIve?.valuationPriceUsed ?? priceContext.valuationLiveMid ?? null,
      source: valuationIve?.valuationPriceSource ?? priceContext.valuationPriceSource ?? '—',
      timestamp: liveQuote?.asOf ?? null,
      component: 'IVECalculationPanel',
      category: valuationIve?.valuationStaleNote || priceContext.valuationStaleNote || 'LIVE',
      store: LivePriceStore.STORE_NAME,
    }),
    tableRow({
      field: 'Crosshair header price',
      value: displaySnapshot.crosshairHeaderPrice ?? null,
      source: 'HistoricalCOTStore + OHLC binding',
      timestamp: displaySnapshot.crosshairHeaderDate ?? null,
      component: displaySnapshot.crosshairHeaderSource ?? 'ChartCrosshairHeader',
      category: 'COT_HISTORICAL',
      store: HistoricalCOTStore.STORE_NAME,
    }),
    tableRow({
      field: 'Candle tooltip close',
      value: displaySnapshot.candleTooltipClose ?? null,
      source: displaySnapshot.candleTooltipSource ?? 'Weekly OHLC',
      timestamp: displaySnapshot.candleTooltipDate ?? null,
      component: displaySnapshot.candleTooltipComponent ?? 'CandleBadge',
      category: 'WEEKLY_CLOSE',
      store: WeeklyOHLCStore.STORE_NAME,
    }),
  ]

  if (isNum(tradingViewAuditPrice)) {
    table.push(
      tableRow({
        field: 'TradingView manual audit',
        value: tradingViewAuditPrice,
        source: 'localStorage manual input',
        timestamp: '—',
        component: 'PriceTruthPanel',
        category: 'MANUAL',
        store: 'MANUAL',
      }),
    )
  }

  const validation = validatePriceEngine(marketId, {
    liveMarkerPrice: priceContext.liveMid,
    valuationLivePrice: valuationIve?.valuationPriceUsed ?? priceContext.valuationLiveMid,
    truthTableLivePrice: liveQuote?.mid ?? null,
    headerLivePrice: priceContext.liveMid,
    weeklyChartClose: priceContext.chartClose,
    candleBadgeClose: displaySnapshot.candleTooltipClose ?? priceContext.weeklyClose,
    historicalHeaderPrice: displaySnapshot.crosshairHeaderPrice,
    historicalHeaderDate: displaySnapshot.crosshairHeaderDate,
    crosshairDate: displaySnapshot.crosshairHeaderDate,
  })

  const comparisons = validation.checks.map((c) => ({
    name: c.name,
    status: c.status,
    expected: c.expected,
    actual: c.actual,
    component: c.component,
    store: c.store,
  }))

  const failCount = comparisons.filter((c) => c.status === 'FAIL').length
  const okCount = comparisons.filter((c) => c.status === 'PASS').length

  return {
    marketId,
    table,
    comparisons,
    summary: {
      overall: validation.pass ? 'PASS' : 'FAIL',
      failCount,
      warnCount: 0,
      okCount,
    },
    exports: {
      liveQuotesGeneratedAt: liveSnap.generatedAt,
      workstationOhlcGeneratedAt: weeklySnap.generatedAt,
      valuationGeneratedAt: null,
    },
    cotContext: {
      lastCotWeek: displaySnapshot.crosshairHeaderDate ?? null,
      matchedOhlcWeek: priceContext.weeklyCloseDate ?? null,
      cotRowPrice: displaySnapshot.crosshairHeaderPrice ?? null,
    },
    meta: {
      liveExportGeneratedAt: liveSnap.generatedAt,
      weeklyExportGeneratedAt: weeklySnap.generatedAt,
      cotExportGeneratedAt: cotSnap.generatedAt,
      liveStatus,
    },
    validation,
    truthTableLivePrice: liveQuote?.mid ?? null,
  }
}

export function logPriceTruthTable(result) {
  if (!result?.table?.length) return
  console.groupCollapsed(`[price-truth] ${result.marketId}`)
  console.table(result.table)
  console.groupEnd()
}

/** @deprecated use buildPriceTruthTable */
export function buildGoldPriceTruthTable(input) {
  return buildPriceTruthTable(input)
}
export function logGoldPriceTruthTable(result) {
  return logPriceTruthTable(result)
}
export function buildGoldPriceTruthAudit(input) {
  return buildPriceTruthTable(input)
}
