/**
 * Price semantics — strict store separation, no hidden fallbacks.
 */

import { LivePriceStore } from '../../prices/stores/LivePriceStore.js'
import { WeeklyOHLCStore } from '../../prices/stores/WeeklyOHLCStore.js'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

/**
 * Build chart/header price context from authoritative stores only.
 * Live fields never fall back to weekly or historical.
 */
export function buildPriceContextFromStores(marketId, { visibleBars, valuationBlock } = {}) {
  const liveQuote = LivePriceStore.getQuote(marketId)
  const freshness = LivePriceStore.getFreshness(marketId)
  const liveStatus = LivePriceStore.getStatus(marketId)
  const completedWeekly = WeeklyOHLCStore.getCompletedWeekly(marketId)
  const weeklySource = WeeklyOHLCStore.getPriceSource(marketId)

  const chartBar = visibleBars?.[visibleBars.length - 1]
  const weeklyFromChart = isNum(chartBar?.close) ? chartBar.close : null
  const weeklyDateFromChart = chartBar?.date ?? null

  const weeklyClose = completedWeekly?.close ?? weeklyFromChart
  const weeklyCloseDate = completedWeekly?.date ?? weeklyDateFromChart

  const liveMid = liveQuote?.mid ?? null
  const modelSpot = isNum(valuationBlock?.model_spot_price)
    ? valuationBlock.model_spot_price
    : isNum(valuationBlock?.spot_price)
      ? valuationBlock.spot_price
      : null
  const fairValue = isNum(valuationBlock?.fair_value) ? valuationBlock.fair_value : null

  const valuationLiveMid = liveStatus === 'LIVE' ? liveMid : null
  const displayValuationPct =
    liveStatus === 'LIVE' && isNum(fairValue) && fairValue > 0 && isNum(liveMid)
      ? Math.round((100 * (liveMid - fairValue)) / fairValue * 100) / 100
      : isNum(valuationBlock?.display_valuation_pct)
        ? valuationBlock.display_valuation_pct
        : isNum(valuationBlock?.deviation_pct)
          ? valuationBlock.deviation_pct
          : null

  return {
    marketId,
    storeLive: LivePriceStore.STORE_NAME,
    storeWeekly: WeeklyOHLCStore.STORE_NAME,

    liveMid,
    liveBid: liveQuote?.bid ?? null,
    liveAsk: liveQuote?.ask ?? null,
    livePrice: liveMid,
    livePriceSource: liveQuote?.source ?? null,
    livePriceAsOf: liveQuote?.asOf ?? null,
    liveStatus,
    liveQuoteStale: liveStatus === 'STALE',
    liveQuoteUnavailable: liveStatus === 'UNAVAILABLE',
    liveQuoteAgeMs: freshness?.ageMs ?? null,

    weeklyClose,
    weeklyCloseDate,
    chartClose: weeklyClose,
    chartCloseDate: weeklyCloseDate,
    weeklyOhlcSource: weeklySource,

    valuationLiveMid,
    valuationPriceUsed: valuationLiveMid,
    valuationPriceSource: liveStatus === 'LIVE' ? liveQuote?.source || 'LivePriceStore' : null,
    valuationStaleNote:
      liveStatus === 'STALE'
        ? 'STALE LIVE'
        : liveStatus === 'UNAVAILABLE'
          ? 'LIVE UNAVAILABLE'
          : null,

    modelSpotPrice: modelSpot,
    fairValue,
    displayValuationPct,
    modelValuationPct: isNum(valuationBlock?.deviation_pct) ? valuationBlock.deviation_pct : null,
  }
}

/**
 * IVE display overlay — live from LivePriceStore only; never substitutes weekly.
 */
export function applyLivePriceToIveDisplay(base, marketId) {
  if (!base) return base
  const liveQuote = LivePriceStore.getQuote(marketId)
  const status = LivePriceStore.getStatus(marketId)
  const freshness = LivePriceStore.getFreshness(marketId)

  if (!liveQuote?.mid) {
    return {
      ...base,
      livePrice: null,
      livePriceSource: null,
      livePriceAsOf: null,
      liveQuoteStale: false,
      liveQuoteUnavailable: true,
      liveStatus: 'UNAVAILABLE',
      valuationPriceUsed: null,
      valuationPriceSource: null,
      valuationStaleNote: 'LIVE UNAVAILABLE',
    }
  }

  const live = liveQuote.mid
  const fair = base.fairValue
  const displayPct =
    status === 'LIVE' && Number.isFinite(fair) && fair > 0
      ? Math.round((100 * (live - fair)) / fair * 100) / 100
      : base.valuationPct

  return {
    ...base,
    livePrice: live,
    livePriceSource: liveQuote.source,
    livePriceAsOf: liveQuote.asOf,
    liveQuoteStale: status === 'STALE',
    liveQuoteUnavailable: false,
    liveStatus: status,
    liveQuoteAgeMs: freshness?.ageMs ?? null,
    currentPrice: status === 'LIVE' ? live : null,
    valuationPriceUsed: status === 'LIVE' ? live : null,
    valuationPriceSource: status === 'LIVE' ? liveQuote.source : null,
    valuationStaleNote: status === 'STALE' ? 'STALE LIVE' : status === 'UNAVAILABLE' ? 'LIVE UNAVAILABLE' : null,
    valuationPct: status === 'LIVE' ? displayPct : base.modelValuationPct ?? base.valuationPct,
  }
}

/** @deprecated use logPriceEngineReport */
export function logInstrumentPriceDiagnostics(marketId, ctx) {
  if (!marketId || !ctx) return
  console.info(`[price-context] ${marketId}`, ctx)
}

/** @deprecated removed — use buildPriceContextFromStores */
export function buildPriceContextFromSources() {
  throw new Error('buildPriceContextFromSources is removed — use buildPriceContextFromStores')
}

/** @deprecated removed — use applyLivePriceToIveDisplay */
export function mergeLiveQuoteIntoDisplay() {
  throw new Error('mergeLiveQuoteIntoDisplay is removed — use applyLivePriceToIveDisplay')
}
