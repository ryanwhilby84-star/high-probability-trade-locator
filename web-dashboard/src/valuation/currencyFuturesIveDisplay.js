/** Currency futures IVE display — futures-native valuation only. */

import { gradeFromPct, fmtPct } from './iveDisplay.js'

export const CURRENCY_FUTURES_MARKETS = new Set([
  'US Dollar Index / DX',
  'Euro FX / 6E',
  'British Pound / 6B',
  'Australian Dollar / 6A',
  'Canadian Dollar / 6C',
  'Japanese Yen / 6J',
  'Swiss Franc / 6S',
  'NZ Dollar / 6N',
])

export function isCurrencyFuturesMarket(market) {
  return CURRENCY_FUTURES_MARKETS.has(String(market || '').trim())
}

export function readFuturesIVE(block) {
  if (!block) return null
  const blockerReason = block.blocker_reason || null
  return {
    instrument: block.instrument,
    futuresSymbol: block.futures_symbol,
    dependentSeries: block.dependent_series,
    modelFamily: block.model_family,
    currentPrice: block.current_price,
    fairValue: block.fair_value,
    valuationPct: block.valuation_pct,
    valuationLabel: block.valuation_label || '—',
    valuationGrade: block.valuation_grade || gradeFromPct(block.valuation_pct),
    modelName: block.model_name || block.model_id,
    modelStatus: block.model_status || 'MODEL_INCOMPLETE',
    blockerReason,
    blockerCodes: block.blocker_codes || [],
    sourceLineage: block.source_lineage || [],
    inputs: block.inputs || {},
    calculationBreakdown: block.calculation_breakdown || [],
    lastUpdated: block.last_updated || '—',
    legacyPairModelUsed: block.legacy_pair_model_used === true,
    legacyFxV3Used: block.legacy_fx_v3_used === true,
    wired: block.wired === true && block.model_status === 'VALIDATED',
    unavailableReason: blockerReason || buildUnavailableReason(block),
  }
}

function buildUnavailableReason(block) {
  const status = block.model_status || 'MODEL_INCOMPLETE'
  const stale = block.inputs?._stale_inputs || []
  const missing = block.inputs?._missing_inputs || []
  if (status === 'DATA_STALE') {
    return stale.length
      ? `${status} — ${stale.join(', ')}`
      : `${status} — stale price or macro inputs`
  }
  if (status === 'DATA_MISSING') {
    return missing.length ? `${status} — ${missing.join(', ')}` : `${status} — missing macro inputs`
  }
  if (status === 'MODEL_INCOMPLETE') {
    return `${status} — model incomplete`
  }
  return 'Valuation unavailable.'
}

export function futuresValuationTone(label) {
  const s = String(label || '').toLowerCase()
  if (s.includes('under')) return 'bullish'
  if (s.includes('over')) return 'bearish'
  return 'neutral'
}

export function currencyFuturesValuationDisplay(marketId, futuresDoc) {
  const block = futuresDoc?.instruments?.[marketId]
  const ive = readFuturesIVE(block)
  if (!ive) return null

  if (!ive.wired) {
    return {
      unavailable: true,
      modelStatus: ive.modelStatus,
      reason: ive.unavailableReason,
      summary: ive.unavailableReason,
      modelName: ive.modelName,
      futuresSymbol: ive.futuresSymbol,
      displayStatus: ive.blockerReason || ive.modelStatus,
    }
  }

  return {
    wired: true,
    gap: ive.valuationPct,
    bias: ive.valuationLabel,
    tone: futuresValuationTone(ive.valuationLabel),
    summary: `${ive.valuationLabel} (${fmtPct(ive.valuationPct)}) · ${ive.modelStatus} · ${ive.modelName}`,
    modelName: ive.modelName,
    modelStatus: ive.modelStatus,
    valuationGrade: ive.valuationGrade,
    fairValue: ive.fairValue,
    spot: ive.currentPrice,
    futuresSymbol: ive.futuresSymbol,
    displayStatus: ive.modelStatus,
  }
}
