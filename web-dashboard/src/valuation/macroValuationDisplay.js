/** Macro valuation display — rates_curve_fair_value_v1 + usd_broad_fair_value_v1. */

import { iveSummaryLine, readIVE } from './iveDisplay.js'

export const RATES_VALUATION_MARKETS = new Set([
  'US 2-Year Treasury Yield',
  'US 10-Year Treasury Yield',
  'US 30-Year Treasury Yield',
  '10-Year Real Yield',
  '2s10s Yield Curve',
])

export const USD_INDEX_MARKET = 'US Dollar Index / DX'

export function isRatesValuationMarket(market) {
  return RATES_VALUATION_MARKETS.has(String(market || '').trim())
}

export function isUsdIndexValuationMarket(market) {
  return String(market || '').trim() === USD_INDEX_MARKET
}

export function isMacroFairValueMarket(market) {
  return isRatesValuationMarket(market) || isUsdIndexValuationMarket(market)
}

export function valuationBlockForMarket(doc, marketId) {
  if (!doc || !marketId) return null
  const instruments = doc.instruments || doc.markets || {}
  return instruments[marketId] || null
}

export function macroValuationTone(bias) {
  const b = String(bias || '').toLowerCase()
  if (b.includes('under')) return 'bullish'
  if (b.includes('over')) return 'bearish'
  return 'neutral'
}

export function macroValuationDisplay(row, valuationDoc) {
  const market = row?.market
  if (!isMacroFairValueMarket(market)) return null
  const block = valuationBlockForMarket(valuationDoc, market)
  const ive = readIVE(block)
  const reason =
    block?.unavailable_reason ||
    block?.valuation_reason ||
    'Macro fair value unavailable — insufficient history or R² gate.'

  if (block?.wired && block?.valuation_bias && String(block.valuation_bias).toUpperCase() !== 'UNAVAILABLE') {
    const label = isUsdIndexValuationMarket(market) ? 'USD fair value' : 'Rates fair value'
    return {
      wired: true,
      gap: block.deviation_pct,
      bias: block.valuation_bias || block.valuation_state,
      tone: macroValuationTone(block.valuation_bias),
      summary: [block.driver_summary || block.valuation_reason, ive ? iveSummaryLine(ive) : null]
        .filter(Boolean)
        .join(' · '),
      modelId: block.model_id,
      valuationGrade: ive?.valuationGrade ?? block.valuation_grade,
      modelStatus: ive?.modelStatus ?? block.model_status,
      fairValue: block.fair_value,
      spot: block.spot_price,
      label,
    }
  }

  return {
    wired: false,
    unavailable: true,
    modelId: block?.model_id,
    reason: String(reason).replace(/^.*?unavailable —\s*/i, ''),
    title: reason,
    summary: reason,
  }
}

