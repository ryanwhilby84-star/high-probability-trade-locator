/**
 * Valuation V3.0 display helpers — fx_carry_real_yield_v3 exports only.
 *
 * **Canonical FX valuation path** for ValuationCell, FxValuationV3Panel, and
 * chart workstation overlays. Do not substitute V2 (fxInstitutionalValuation.js).
 */
import { resolveFxPairId } from './fxInstitutionalValuation.js'
import { gradeFromPct, iveSummaryLine, readIVE } from '../valuation/iveDisplay.js'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

export const FX_V3_LIVE_PAIRS = new Set([
  'EUR/USD',
  'GBP/USD',
  'AUD/USD',
  'NZD/USD',
  'USD/JPY',
  'USD/CAD',
  'USD/CHF',
  'EUR/GBP',
  'EUR/AUD',
])

export function fmtFxPrice(v) {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return n.toFixed(n >= 10 ? 4 : 5)
}

export function fmtPct(v) {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`
}

export function fmtPp(v) {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)} pp`
}

export function fmtRate(v) {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return `${n.toFixed(2)}%`
}

export function fxV3StateTone(state) {
  const s = String(state || '').toLowerCase()
  if (s.includes('under')) return 'bullish'
  if (s.includes('over')) return 'bearish'
  return 'neutral'
}

export function valuationStateToBias(state) {
  const s = String(state || '')
  if (s === 'Undervalued') return 'Bullish'
  if (s === 'Overvalued') return 'Bearish'
  if (s === 'Fair Value') return 'Neutral'
  return null
}

export function pairFromV3Doc(v3Doc, pairId) {
  if (!v3Doc?.pairs || !pairId) return null
  return v3Doc.pairs[pairId] || null
}

export function v3MarketBlock(v3Doc, marketId, pairId) {
  if (v3Doc?.markets?.[marketId]) return v3Doc.markets[marketId]
  return pairFromV3Doc(v3Doc, pairId)
}

/** Normalize V3 export + foundation into one display model. */
export function fxValuationV3FromDocs(v3Doc, foundationPair, pairId) {
  const block = pairFromV3Doc(v3Doc, pairId)
  if (!block) return null

  const foundationPass = foundationPair?.overall_status === 'PASS'
  const inLiveScope = FX_V3_LIVE_PAIRS.has(pairId) && foundationPass
  const wired = block.wired === true && inLiveScope && foundationPass
  const ive = readIVE(block)

  const blockers = foundationPair?.v3_blocker?.blockers || []
  let unavailableReason =
    block.explanation ||
    block.valuation_reason ||
    block.driver_summary ||
    'Valuation gates not cleared.'

  if (!inLiveScope) {
    unavailableReason = `${pairId} is outside V3.0 live scope.`
  } else if (!foundationPass) {
    unavailableReason = `${pairId} foundation audit FAIL — valuation not published.`
  } else if (block.audit_status !== 'PASS') {
    unavailableReason = block.explanation || block.driver_summary || 'V3 model audit FAIL.'
  }

  return {
    pairId,
    base: block.base,
    quote: block.quote,
    spot: block.spot_price,
    fairValue: block.fair_value,
    deviation: block.deviation_pct,
    state: block.valuation_state,
    valuationGrade: ive?.valuationGrade || gradeFromPct(block.deviation_pct),
    modelStatus: ive?.modelStatus || block.model_status || 'MODEL_INCOMPLETE',
    modelId: block.model_id,
    auditStatus: block.audit_status,
    driverSummary: block.driver_summary,
    explanation: block.explanation,
    inputFreshness: block.input_freshness,
    drivers: block.drivers || {},
    dxy: block.dxy_regime || {},
    treasury: block.treasury_regime || {},
    wired,
    unavailable: !wired,
    unavailableReason,
    blockers,
    foundationPass,
    foundationTone: foundationPass ? 'pass' : 'fail',
    ive,
  }
}

/** Scanner / table cell display from V3 export. */
export function fxValuationV3Display(marketId, v3Doc, foundationDoc, row) {
  const pairId = resolveFxPairId(marketId || row?.market, row?.fx_valuation)
  if (!pairId || !v3Doc) return null

  const foundationPair = foundationDoc?.pairs?.[pairId] || null
  const model = fxValuationV3FromDocs(v3Doc, foundationPair, pairId)
  if (!model) return null

  if (!model.wired) {
    return {
      unavailable: true,
      gap: null,
      bias: null,
      condition: 'Unavailable',
      valuationGrade: model.valuationGrade,
      modelStatus: model.modelStatus,
      fair: model.fairValue,
      spot: model.spot,
      model: model.modelId,
      summary: model.unavailableReason,
      reason: model.unavailableReason,
    }
  }

  const iveLine = model.ive ? iveSummaryLine(model.ive) : null
  return {
    gap: model.deviation,
    bias: valuationStateToBias(model.state),
    condition: model.state,
    valuationGrade: model.valuationGrade,
    modelStatus: model.modelStatus,
    fair: model.fairValue,
    spot: model.spot,
    model: model.modelId,
    summary: [model.driverSummary, iveLine].filter(Boolean).join(' · '),
  }
}

