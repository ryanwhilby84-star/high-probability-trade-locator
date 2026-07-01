/**
 * Valuation explainability — normalize export blocks into trader-facing evidence.
 * No new valuation math; interprets existing export fields only.
 */

import { resolveFxPairId } from '../fx/fxInstitutionalValuation.js'
import { fxValuationV3FromDocs, normalizeConfidence } from '../fx/fxValuationV3Display.js'
import { isAgriValuationMarket } from './agriValuationDisplay.js'
import { isMetalsValuationMarket, valuationBlockForMarket } from './metalsValuationDisplay.js'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

export function valuationBlock(doc, marketId) {
  return valuationBlockForMarket(doc, marketId)
}

/** FX model-fit tier ignoring stale-input downgrade (for explainability). */
export function fxModelFitTier(n, r2) {
  if (!isNum(n) || !isNum(r2) || n < 52 || r2 < 0.08) return 'None'
  if (n >= 156 && r2 >= 0.25) return 'High'
  if (n >= 52 && r2 >= 0.18) return 'Medium'
  return 'Low'
}

export function buildFxTrustAssessment(block, foundationPair) {
  const conf = normalizeConfidence(block?.confidence)
  const reg = block?.regression || {}
  const n = reg.n ?? 0
  const r2 = reg.r_squared
  const stale = block?.stale_inputs || []
  const missing = block?.missing_inputs || []
  const fitTier = fxModelFitTier(n, r2)

  const parts = []
  if (missing.length) {
    parts.push(`Missing inputs: ${missing.join(', ')}.`)
  }
  if (stale.length) {
    parts.push(
      `Published confidence is ${conf} because stale inputs are flagged (${stale.join(', ')}).`,
    )
    if (fitTier !== conf && fitTier !== 'None') {
      parts.push(
        `Model fit alone would support ${fitTier} confidence (R²=${isNum(r2) ? r2.toFixed(3) : '—'}, n=${n}).`,
      )
    }
  } else if (fitTier === 'High') {
    parts.push(`Confidence is High — stable fit (R²=${r2?.toFixed?.(3) ?? r2}, n=${n}) with fresh inputs.`)
  } else if (fitTier === 'Medium') {
    parts.push(`Confidence is Medium — acceptable fit (R²=${r2?.toFixed?.(3) ?? r2}, n=${n}).`)
  } else if (fitTier === 'Low') {
    parts.push(`Confidence is Low — marginal fit (R²=${r2?.toFixed?.(3) ?? r2}, n=${n}).`)
  } else {
    parts.push(`Confidence is ${conf} — regression gate not met (R²=${r2 ?? '—'}, n=${n}).`)
  }

  const blockers = foundationPair?.v3_blocker?.blockers || []
  if (blockers.length) {
    parts.push(`Foundation blockers: ${blockers.join('; ')}.`)
  }

  return {
    published: conf,
    modelFitTier: fitTier,
    staleInputs: stale,
    missingInputs: missing,
    narrative: parts.join(' '),
  }
}

export function buildMetalsTrustAssessment(block, backtest) {
  const trust = block?.trust_grade || 'C'
  const conf = block?.confidence || 'none'
  const reg = block?.regression || {}
  const n = reg.n ?? 0
  const r2 = reg.r_squared
  const bt = backtest?.markets?.[block?.market] || {}

  const parts = []
  if (trust === 'A') {
    parts.push(
      `Trust grade A — ${n} weekly observations, R²=${isNum(r2) ? r2.toFixed(3) : '—'}, macro inputs fresh.`,
    )
  } else if (trust === 'B') {
    parts.push(`Trust grade B — usable but below production target (n=${n}, R²=${r2?.toFixed?.(3) ?? r2}).`)
  } else {
    parts.push(`Trust grade C — do not treat deviation as high-conviction (n=${n}, R²=${r2 ?? '—'}).`)
  }

  parts.push(`Display confidence: ${conf} (mapped from trust grade).`)

  if (bt.mean_abs_deviation_pct != null) {
    parts.push(
      `Historical mean absolute deviation ${bt.mean_abs_deviation_pct}% over backtest window; ` +
        `4W forward return correlation ${bt.forward_return_correlation ?? '—'}.`,
    )
  }

  if (isNum(block?.deviation_pct) && Math.abs(block.deviation_pct) > 25) {
    parts.push(
      `Large deviation (${block.deviation_pct}%) — verify macro drivers; model explains only part of price level.`,
    )
  }

  return { trustGrade: trust, published: conf, narrative: parts.join(' ') }
}

export function buildAgriTrustAssessment(block) {
  const conf = block?.confidence || 'none'
  const n = block?.balance_sheet_observations ?? block?.data_depth ?? 0
  const modelId = block?.model_id || 'agri_fundamental_valuation'
  const parts = []

  if (conf === 'high') {
    parts.push(`Confidence High — regression path with R² gate and n≥24 balance-sheet points.`)
  } else if (conf === 'medium') {
    parts.push(`Confidence Medium — ${n} USDA balance-sheet observations aligned.`)
  } else if (conf === 'low') {
    parts.push(`Confidence Low — sparse balance-sheet history (n=${n}) or percentile-only path.`)
  } else {
    parts.push(`Confidence none — insufficient balance-sheet depth for this instrument.`)
  }

  parts.push(`Model: ${modelId}. Deviation gates: ±5% fair-value band.`)

  return { published: conf, narrative: parts.join(' ') }
}

export function buildFxDrivers(block) {
  const d = block?.drivers || {}
  return [
    {
      label: 'Policy differential',
      detail: `${block?.base} ${fmtRate(d.base_policy_rate)} vs ${block?.quote} ${fmtRate(d.quote_policy_rate)}`,
      value: fmtPp(d.policy_rate_diff),
    },
    {
      label: '2Y yield differential',
      detail: `${block?.base} 2Y ${fmtRate(d.base_yield_2y)} vs ${block?.quote} 2Y ${fmtRate(d.quote_yield_2y)}`,
      value: fmtPp(d.yield_2y_diff),
    },
    {
      label: 'Real yield differential',
      detail: `${block?.base} real ${fmtRate(d.base_real_yield)} vs ${block?.quote} real ${fmtRate(d.quote_real_yield)}`,
      value: fmtPp(d.real_yield_diff),
    },
    {
      label: 'Inflation differential',
      detail: `CPI YoY ${block?.base} ${fmtRate(d.base_cpi_yoy)} vs ${block?.quote} ${fmtRate(d.quote_cpi_yoy)}`,
      value: fmtPp(d.inflation_diff),
    },
    {
      label: 'DXY regime',
      detail: block?.dxy_regime?.regime_label || block?.dxy_regime?.regime || '—',
      value: block?.dxy_regime?.percentile_52w != null ? `${Number(block.dxy_regime.percentile_52w).toFixed(0)}th pct 52w` : '—',
    },
    {
      label: 'Treasury regime',
      detail: block?.treasury_regime?.regime_label || block?.treasury_regime?.regime || '—',
      value: block?.treasury_regime?.slope_2s10s != null ? `2s10s ${fmtPp(block.treasury_regime.slope_2s10s)}` : '—',
    },
  ]
}

export function buildMetalsDrivers(block) {
  const d = block?.drivers || {}
  const reg = block?.regression?.features || {}
  return [
    { label: '10Y real yield (DFII10)', detail: 'Primary discount-rate driver', value: fmtRate(d.real_yield_10y) },
    { label: 'Broad USD index (DXY)', detail: 'Dollar overlay on metal price', value: d.dxy_broad != null ? Number(d.dxy_broad).toFixed(2) : '—' },
    {
      label: 'Regression — real yield β',
      detail: 'log(price) sensitivity to real yield',
      value: reg.real_yield != null ? reg.real_yield.toFixed(4) : '—',
    },
    {
      label: 'Regression — log(DXY) β',
      detail: 'log(price) sensitivity to dollar',
      value: reg.log_dxy != null ? reg.log_dxy.toFixed(4) : '—',
    },
    {
      label: 'Price/fair percentile',
      detail: 'Historical ratio vs macro fair value',
      value: d.valuation_ratio_percentile != null ? `${d.valuation_ratio_percentile}th pct` : '—',
    },
    {
      label: 'Residual percentile',
      detail: 'Spot minus fair value in history',
      value: d.residual_percentile != null ? `${d.residual_percentile}th pct` : '—',
    },
    {
      label: 'Composite score',
      detail: 'Display score from percentile mapping',
      value: d.composite_score != null ? String(d.composite_score) : '—',
    },
  ]
}

export function buildAgriDrivers(block) {
  return [
    {
      label: 'Stocks-to-use',
      detail: 'Latest USDA/WASDE balance-sheet ratio',
      value: block?.stocks_to_use != null ? `${(Number(block.stocks_to_use) * 100).toFixed(2)}%` : '—',
    },
    {
      label: 'Balance-sheet observations',
      detail: 'Aligned PSD points in regression window',
      value: block?.balance_sheet_observations ?? block?.data_depth ?? '—',
    },
    {
      label: 'Model path',
      detail: block?.model_note || block?.valuation_reason || '—',
      value: block?.model_id || '—',
    },
    {
      label: 'Price source',
      detail: 'Canonical spot for fair-value anchor',
      value: block?.price_source || '—',
    },
  ]
}

function fmtRate(v) {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return `${n.toFixed(2)}%`
}

function fmtPp(v) {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)} pp`
}

function fmtPct(v) {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`
}

function fmtPrice(v, digits = 4) {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return n.toFixed(digits)
}

export function buildHistoricalPerformance(block, backtest) {
  const reg = block?.regression || {}
  const bt = backtest?.markets?.[block?.market]
  return {
    rSquared: reg.r_squared,
    nObs: reg.n ?? reg.n_obs,
    meanAbsDeviationPct: bt?.mean_abs_deviation_pct ?? null,
    forwardReturnCorrelation: bt?.forward_return_correlation ?? null,
    forwardWeeks: bt?.forward_weeks ?? null,
    sampleForwardPairs: bt?.sample_forward_pairs ?? null,
    methodology:
      block?.model_note ||
      block?.valuation_reason ||
      'Deviation = (spot − fair) / fair × 100. Fair value from published model regression.',
  }
}

/** Full workstation model for one instrument. */
export function buildValuationExplainability({
  marketId,
  valuationDoc,
  v3Doc,
  foundationDoc,
  backtestDoc,
  row,
}) {
  const block = valuationBlock(valuationDoc, marketId)
  if (!block) return null

  const pairId = resolveFxPairId(marketId, row?.fx_valuation)
  const isFx = Boolean(pairId && block.model_id === 'fx_carry_real_yield_v3')
  const isMetals = isMetalsValuationMarket(marketId)
  const isAgri = isAgriValuationMarket(marketId)

  let assetClass = 'unknown'
  if (isFx) assetClass = 'fx'
  else if (isMetals) assetClass = 'metals'
  else if (isAgri) assetClass = 'agri'

  const foundationPair = pairId ? foundationDoc?.pairs?.[pairId] : null
  const fxModel = isFx ? fxValuationV3FromDocs(v3Doc, foundationPair, pairId) : null
  const wired = isFx ? fxModel?.wired : block.wired === true

  let trust = null
  let drivers = []
  if (isFx) trust = buildFxTrustAssessment(block, foundationPair)
  else if (isMetals) trust = buildMetalsTrustAssessment(block, backtestDoc)
  else if (isAgri) trust = buildAgriTrustAssessment(block)

  if (isFx) drivers = buildFxDrivers(block)
  else if (isMetals) drivers = buildMetalsDrivers(block)
  else if (isAgri) drivers = buildAgriDrivers(block)

  const hist = buildHistoricalPerformance(block, backtestDoc)

  return {
    marketId,
    assetClass,
    wired,
    unavailable: !wired,
    unavailableReason:
      fxModel?.unavailableReason ||
      block.unavailable_reason ||
      block.valuation_reason ||
      'Valuation not published for this instrument.',
    summary: {
      spot: block.spot_price,
      fairValue: block.fair_value,
      deviationPct: block.deviation_pct,
      state: block.valuation_state || block.valuation_bias,
      confidence: normalizeConfidence(block.confidence),
      trustGrade: block.trust_grade || trust?.trustGrade || '—',
    },
    model: {
      name: block.model_id || block.valuation_model_id,
      phase: block.valuation_phase,
      sampleSize: regN(block),
      rSquared: block.regression?.r_squared,
      lastUpdated: valuationDoc?.generated_at || block.as_of_week,
      methodology: block.driver_summary || block.model_note,
    },
    drivers,
    historical: hist,
    trust,
    explanation: block.explanation || block.driver_summary || block.valuation_reason,
    inputFreshness: block.input_freshness,
    staleInputs: block.stale_inputs,
    missingInputs: block.missing_inputs,
    blockers: fxModel?.blockers || foundationPair?.v3_blocker?.blockers || [],
    pairId,
  }
}

function regN(block) {
  const r = block?.regression
  if (!r) return block?.balance_sheet_observations ?? block?.data_depth
  return r.n ?? r.n_obs
}

export { fmtPct, fmtPrice }
