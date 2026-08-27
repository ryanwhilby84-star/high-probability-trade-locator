/**
 * FX Institutional Macro V2 — display helpers (SECONDARY / PARALLEL).
 *
 * Used by FxSetupRankingPanel and FxValuationPanel only.
 * **Do not use for the main scanner valuation column** — that path is
 * fxValuationV3Display (fx_carry_real_yield_v3) in ValuationCell.jsx.
 */

const FX_MAJOR_RE = /\/ 6[A-Z0-9]/i
const FX_CROSS_RE = /^[A-Z]{3}\/[A-Z]{3}$/

export const FX_INSTITUTIONAL_MODEL = 'FX Institutional Macro V2'

export function isFxMarket(row) {
  if (!row) return false
  const ac = row?.instrument_meta?.asset_class
  if (ac === 'fx') return true
  if (row.fx_valuation_model_type || row.fx_valuation_bias) return true
  const m = String(row?.market || '').trim()
  if (!m) return false
  if (FX_CROSS_RE.test(m)) return true
  if (FX_MAJOR_RE.test(m)) return true
  return false
}

export function isInstitutionalFxValuation(row) {
  if (!row) return false
  const model = String(row.fx_valuation_model_type || row.fx_valuation?.valuation_model_type || '')
  if (model.includes('Institutional Macro V2')) return true
  return !!(row.fx_valuation_gap_pct != null || row.fx_valuation?.valuation_gap_pct != null)
}

export function fxValuationFromRow(row) {
  const fx = row?.fx_valuation || {}
  const bias = row?.fx_valuation_bias ?? fx.valuation_bias
  const gap = row?.fx_valuation_gap_pct ?? fx.valuation_gap_pct
  const condition = fx.value_condition ?? row?.fx_valuation_condition
  const score = row?.fx_valuation_score ?? fx.valuation_score
  const fair = row?.fx_fair_value_estimate ?? fx.fair_value_estimate
  const spot = fx.spot
  return { bias, gap, condition, score, fair, spot, fx }
}

function safeList(value) {
  return Array.isArray(value) ? value : []
}

function normalizeCtx(ctx) {
  if (ctx && typeof ctx === 'object') return ctx
  return {}
}

export function valuationTone(condition, bias) {
  const c = String(condition || '').toLowerCase()
  const b = String(bias || '').toLowerCase()
  if (c.includes('under') || b === 'bullish') return 'bullish'
  if (c.includes('over') || b === 'bearish') return 'bearish'
  return 'neutral'
}

export function valuationBiasLabel(bias) {
  const b = String(bias || '').toUpperCase()
  if (b === 'BULLISH') return 'Bullish'
  if (b === 'BEARISH') return 'Bearish'
  if (b === 'NEUTRAL') return 'Neutral'
  return '—'
}

export function fmtGapPct(gap) {
  const n = Number(gap)
  if (!Number.isFinite(n)) return '—'
  return `${n >= 0 ? '+' : ''}${n.toFixed(1)}%`
}

export function currencyScoreStatus(score) {
  const s = Number(score)
  if (!Number.isFinite(s)) return '—'
  if (s >= 75) return 'Extremely Undervalued'
  if (s >= 25) return 'Undervalued'
  if (s > -25) return 'Fair Value'
  if (s > -75) return 'Overvalued'
  return 'Extremely Overvalued'
}

/** Map institutional pair score differential to pillar pass (long = undervalued/bullish). */
export function valuationPillarPass(bias, direction) {
  if (!bias || bias === 'UNAVAILABLE') return false
  if (direction === 'long') return bias === 'Bullish'
  if (direction === 'short') return bias === 'Bearish'
  return bias === 'Neutral'
}

/** Map raw bias strings to pillar display values, or null when missing. */
export function normalizePillarBias(raw) {
  const s = String(raw || '').trim()
  if (!s || s.toUpperCase() === 'N/A' || s.toUpperCase() === 'UNAVAILABLE' || s.toUpperCase() === 'PENDING') {
    return null
  }
  const u = s.toUpperCase()
  if (u === 'BULLISH' || u.includes('UNDER')) return 'Bullish'
  if (u === 'BEARISH' || u.includes('OVER')) return 'Bearish'
  if (u === 'NEUTRAL' || u.includes('FAIR')) return 'Neutral'
  const low = s.toLowerCase()
  if (low.includes('bull')) return 'Bullish'
  if (low.includes('bear')) return 'Bearish'
  if (low.includes('neutral')) return 'Neutral'
  return null
}

const COT_MARKET_TO_PAIR = {
  'Euro FX / 6E': 'EUR/USD',
  'British Pound / 6B': 'GBP/USD',
  'Australian Dollar / 6A': 'AUD/USD',
  'NZ Dollar / 6N': 'NZD/USD',
  'Japanese Yen / 6J': 'USD/JPY',
  'Canadian Dollar / 6C': 'USD/CAD',
  'Swiss Franc / 6S': 'USD/CHF',
}

export function resolveFxPairId(market, fxBlock) {
  if (fxBlock?.pair) return String(fxBlock.pair).toUpperCase()
  const m = String(market || '').trim()
  if (/^[A-Z]{3}\/[A-Z]{3}$/i.test(m)) return m.toUpperCase()
  return COT_MARKET_TO_PAIR[m] || null
}

function pairBlockV2(fxValuationDoc, pairId) {
  if (!pairId) return null
  const found = safeList(fxValuationDoc?.pairs).find(
    (p) => p?.pair && String(p.pair).toUpperCase() === pairId,
  )
  if (!found || found.supported === false) return null
  const model = String(found.valuation_model_type || '')
  if (!model.includes('Institutional Macro V2')) return null
  return found
}

/** Valuation pillar — V2 live path only (never V1 yield differential). */
export function valuationV2BiasFromRow(row, fxValuationDoc) {
  const fx = row?.fx_valuation
  const rowModel = String(row?.fx_valuation_model_type || fx?.valuation_model_type || '')
  if (rowModel.includes('Institutional Macro V2')) {
    return normalizePillarBias(row?.fx_valuation_bias ?? fx?.valuation_bias)
  }
  const pairId = resolveFxPairId(row?.market, fx)
  const block = pairBlockV2(fxValuationDoc, pairId)
  return block ? normalizePillarBias(block.valuation_bias) : null
}

/** V2 valuation display fields for scanner cells (ignores V1 confluence scalars). */
export function fxValuationV2Display(row, fxValuationDoc) {
  const fx = row?.fx_valuation
  const rowModel = String(row?.fx_valuation_model_type || fx?.valuation_model_type || '')
  if (rowModel.includes('Institutional Macro V2')) {
    return fxValuationFromRow(row)
  }
  const pairId = resolveFxPairId(row?.market, fx)
  const block = pairBlockV2(fxValuationDoc, pairId)
  if (!block) return null
  return {
    bias: block.valuation_bias,
    gap: block.valuation_gap_pct,
    condition: block.value_condition,
    score: block.valuation_score,
    fair: block.fair_value_estimate,
    spot: block.spot,
    fx: block,
  }
}

export function rsBiasFromRow(row, relativeStrength) {
  const pairId = resolveFxPairId(row?.market, row?.fx_valuation)
  const pairs = safeList(relativeStrength?.pair_opportunities)
  if (pairId) {
    const opp = pairs.find((p) => p?.pair && String(p.pair).toUpperCase() === pairId)
    if (opp?.directional_bias) return normalizePillarBias(opp.directional_bias)
  }
  const cotMarket = String(row?.market || '').trim()
  const leg = safeList(relativeStrength?.currency_leaderboard).find(
    (r) => r?.cot_market === cotMarket,
  )
  if (leg && Number.isFinite(Number(leg.final_score))) {
    const score = Number(leg.final_score)
    if (score >= 5) return 'Bullish'
    if (score <= -5) return 'Bearish'
    return 'Neutral'
  }
  return null
}

export function seasonalityBiasFromRow(row) {
  if (row?.seasonality_wired === false) return null
  return normalizePillarBias(row?.seasonality_bias)
}

export function cotBiasFromRow(row) {
  const bias = row?.cot_bias ?? row?.final_calculated_cot_bias
  return normalizePillarBias(bias)
}

export function macroPositioningBiasFromPair(fxValuationDoc, pairId) {
  if (!pairId) return null
  const found = safeList(fxValuationDoc?.pairs).find(
    (p) => p?.pair && String(p.pair).toUpperCase() === pairId,
  )
  if (!found) return null
  const overlay = found.macro_positioning_overlay || {}
  return normalizePillarBias(overlay.positioning_bias)
}

/** Four-pillar FX alignment: RS, Valuation (+ TFF positioning), Seasonality, COT. */
export function buildFxFourPillarAlignment({
  rsBias,
  valuationBias,
  seasonalityBias,
  cotBias,
  macroPositioningBias,
  compact = false,
}) {
  const macro = normalizePillarBias(macroPositioningBias)
  const val = normalizePillarBias(valuationBias)
  let valEffective = val
  if (val && macro) {
    if (val === macro) valEffective = val
    else if (val === 'Neutral' || macro === 'Neutral') valEffective = val !== 'Neutral' ? val : macro
    else valEffective = null
  } else {
    valEffective = val || macro
  }

  const pillars = [
    { key: 'rs', label: compact ? 'RS' : 'Relative Strength', bias: normalizePillarBias(rsBias) },
    {
      key: 'valuation',
      label: compact ? 'Val+Pos' : 'Valuation + Positioning',
      bias: valEffective,
    },
    { key: 'seasonality', label: 'Seasonality', bias: normalizePillarBias(seasonalityBias) },
    { key: 'cot', label: 'COT', bias: normalizePillarBias(cotBias) },
  ].map((p) => ({
    ...p,
    display: p.bias || 'Missing',
    state: p.bias ? String(p.bias).toLowerCase() : 'missing',
  }))

  const active = pillars.filter((p) => p.bias)
  const bullish = active.filter((p) => p.bias === 'Bullish').length
  const bearish = active.filter((p) => p.bias === 'Bearish').length
  const neutral = active.filter((p) => p.bias === 'Neutral').length

  let dominant = null
  if (bullish > bearish && bullish >= neutral) dominant = 'Bullish'
  else if (bearish > bullish && bearish >= neutral) dominant = 'Bearish'
  else if (neutral > 0 && neutral >= bullish && neutral >= bearish) dominant = 'Neutral'

  const total = 4
  const aligned = dominant ? pillars.filter((p) => p.bias === dominant).length : 0

  let confidence = 'C'
  if (aligned === 4) confidence = 'A+'
  else if (aligned === 3) confidence = 'A'
  else if (aligned === 2) confidence = 'B'

  return {
    pillars,
    aligned,
    total,
    dominant,
    confidence,
    alignmentLabel: `${aligned} / ${total}`,
  }
}

/** Build four-pillar alignment for an expanded FX scanner row. */
export function buildFxScannerPillarAlignment(row, ctx) {
  const { relativeStrength, fxValuation } = normalizeCtx(ctx)
  const pairId = resolveFxPairId(row?.market, row?.fx_valuation)
  const macroPos = macroPositioningBiasFromPair(fxValuation, pairId)
  return buildFxFourPillarAlignment({
    rsBias: rsBiasFromRow(row, relativeStrength),
    valuationBias: valuationV2BiasFromRow(row, fxValuation),
    seasonalityBias: seasonalityBiasFromRow(row),
    cotBias: cotBiasFromRow(row),
    macroPositioningBias: macroPos,
    compact: true,
  })
}

const FX_GRADE_RANK = { 'A+': 3, A: 2, B: 1, C: 0 }

/** Four-pillar alignment grade for an FX scanner row, or null for non-FX. */
export function fxAlignmentGradeForRow(row, ctx) {
  if (!isFxMarket(row)) return null
  try {
    return buildFxScannerPillarAlignment(row, ctx).confidence
  } catch {
    return null
  }
}

/** Full alignment summary for table badges (grade + fraction label). */
export function fxAlignmentSummaryForRow(row, ctx) {
  if (!isFxMarket(row)) return null
  try {
    const alignment = buildFxScannerPillarAlignment(row, ctx)
    const pillars = safeList(alignment?.pillars)
    const activeCount = pillars.filter((p) => p?.bias).length
    const total = alignment?.total ?? 4
    if (activeCount === 0) {
      return {
        grade: 'Missing',
        fraction: '',
        aligned: 0,
        total,
        alignmentLabel: '0 / 4',
        dominant: null,
        missing: true,
        alignment,
      }
    }
    const grade = alignment.confidence
    const fraction = `${alignment.aligned}/${alignment.total}`
    return {
      grade,
      fraction,
      aligned: alignment.aligned,
      total: alignment.total,
      alignmentLabel: alignment.alignmentLabel,
      dominant: alignment.dominant,
      missing: false,
      alignment,
    }
  } catch {
    return {
      grade: 'Missing',
      fraction: '',
      aligned: 0,
      total: 4,
      alignmentLabel: '0 / 4',
      dominant: null,
      missing: true,
      alignment: null,
    }
  }
}

/**
 * Scanner FX grade filter thresholds.
 * - all: no grade filter
 * - aplus: A+ only (4/4)
 * - a_plus: A and above (3/4+)
 * - b_plus: B and above (2/4+)
 */
export function fxGradeMeetsFilter(grade, filter) {
  if (!filter || filter === 'all') return true
  if (grade === 'Missing') return false
  const rank = FX_GRADE_RANK[grade] ?? -1
  if (filter === 'aplus') return grade === 'A+'
  if (filter === 'a_plus') return rank >= FX_GRADE_RANK.A
  if (filter === 'b_plus') return rank >= FX_GRADE_RANK.B
  return true
}

export function hasFxValuationV2Live(row, fxValuationDoc) {
  return valuationV2BiasFromRow(row, fxValuationDoc) != null || fxValuationV2Display(row, fxValuationDoc) != null
}
