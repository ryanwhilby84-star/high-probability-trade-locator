/** Metals valuation display — metals_real_yield_v1 pillar. */

import { iveSummaryLine, readIVE } from './iveDisplay.js'

export const METALS_VALUATION_MARKETS = new Set([
  'Gold',
  'Silver',
  'Platinum',
  'Palladium',
  'Copper / HG',
])

export function isMetalsValuationMarket(market) {
  return METALS_VALUATION_MARKETS.has(String(market || '').trim())
}

export function valuationBlockForMarket(doc, marketId) {
  if (!doc || !marketId) return null
  const instruments = doc.instruments || doc.markets || {}
  return instruments[marketId] || null
}

export function metalsValuationTone(bias) {
  const b = String(bias || '').toLowerCase()
  if (b.includes('under')) return 'bullish'
  if (b.includes('over')) return 'bearish'
  return 'neutral'
}

export function mergeMetalsValuationRow(row, valuationDoc) {
  const block = valuationBlockForMarket(valuationDoc, row?.market)
  if (!block) return row || {}
  const ive = readIVE(block)
  return {
    ...row,
    valuation_wired: block.wired ?? row?.valuation_wired,
    valuation_bias: block.valuation_bias ?? row?.valuation_bias,
    valuation_state: block.valuation_state ?? row?.valuation_state,
    valuation_reason: block.valuation_reason ?? block.unavailable_reason ?? row?.valuation_reason,
    deviation_pct: block.deviation_pct ?? row?.deviation_pct,
    fair_value: block.fair_value ?? row?.fair_value,
    spot_price: block.spot_price ?? row?.spot_price,
    valuation_model_id: block.model_id ?? row?.valuation_model_id,
    valuation_grade: ive?.valuationGrade ?? block.valuation_grade ?? row?.valuation_grade,
    valuation_model_status: ive?.modelStatus ?? block.model_status ?? row?.valuation_model_status,
    valuation_pillar: block.valuation_pillar ?? row?.valuation_pillar,
  }
}

export function metalsValuationDisplay(row, valuationDoc) {
  if (!isMetalsValuationMarket(row?.market)) return null
  const merged = mergeMetalsValuationRow(row, valuationDoc)
  const block = valuationBlockForMarket(valuationDoc, row?.market)
  const ive = readIVE(block)
  const reason =
    merged.unavailable_reason ||
    merged.valuation_reason ||
    'Metals macro valuation unavailable — insufficient aligned history or R² gate.'

  if (merged.valuation_wired === true && merged.valuation_bias && String(merged.valuation_bias).toUpperCase() !== 'UNAVAILABLE') {
    const composite = block?.drivers?.composite_score ?? block?.valuation_score
    const summaryParts = [
      block?.driver_summary || merged.valuation_reason,
      ive ? iveSummaryLine(ive) : null,
      composite != null ? `Composite ${composite}` : null,
    ].filter(Boolean)
    return {
      wired: true,
      gap: merged.deviation_pct,
      bias: merged.valuation_bias || merged.valuation_state,
      tone: metalsValuationTone(merged.valuation_bias),
      summary: summaryParts.join(' · '),
      modelId: merged.valuation_model_id || 'metals_real_yield_v1',
      valuationGrade: merged.valuation_grade,
      modelStatus: merged.valuation_model_status,
      compositeScore: composite,
      fairValue: merged.fair_value,
      spot: merged.spot_price,
    }
  }

  return {
    wired: false,
    unavailable: true,
    modelId: block?.model_id || 'metals_real_yield_v1',
    phase: block?.valuation_phase || 'V3.1 Metals',
    reason: String(reason).replace(/^Metals valuation unavailable —\s*/i, ''),
    title: reason,
    summary: reason,
  }
}

