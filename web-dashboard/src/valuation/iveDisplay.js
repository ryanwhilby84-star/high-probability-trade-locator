/**
 * Institutional Valuation Engine (IVE) — Phase 0 display helpers.
 * No confidence. Fair value + calculation audit only.
 */

export function readIVE(block) {
  if (!block) return null
  const ive = block.ive || block

  const wired = block.wired === true || ive.wired === true
  const modelStatus =
    ive.model_status ||
    block.model_status ||
    (wired ? 'VALIDATED' : 'MODEL_INCOMPLETE')

  if (!ive || (ive.instrument == null && !block.market && !block.model_name && !block.model_id)) {
    if (!wired && !block.fair_value) return null
  }

  const valuationPct = ive.valuation_pct ?? block.deviation_pct

  return {
    instrument: ive.instrument || block.market,
    currentPrice: ive.current_price ?? block.spot_price,
    fairValue: ive.fair_value ?? block.fair_value,
    valuationPct,
    valuationLabel: ive.valuation_label || block.valuation_state || block.valuation_bias || '—',
    valuationGrade: ive.valuation_grade || block.trust_grade || gradeFromPct(valuationPct),
    modelName: ive.model_name || block.model_id || '—',
    modelStatus,
    sourceNames: ive.source_names || [],
    sourceDates: ive.source_dates || [],
    sourceLineage: ive.source_lineage || [],
    inputs: ive.inputs || {},
    calculationBreakdown: ive.calculation_breakdown || [],
    lastUpdated: ive.last_updated || block.as_of_week || block.input_freshness?.price_as_of || '—',
    wired,
    unavailableReason:
      block.unavailable_reason || block.valuation_reason || 'Valuation model incomplete.',
  }
}

export function gradeFromPct(pct) {
  const n = Number(pct)
  if (!Number.isFinite(n)) return '—'
  const mag = Math.abs(n)
  if (mag <= 5) return 'FAIR'
  if (mag <= 15) return 'MILD'
  if (mag <= 30) return 'SIGNIFICANT'
  return 'EXTREME'
}

export function statusTone(status) {
  const s = String(status || '').toUpperCase()
  if (s === 'VALIDATED') return 'pass'
  if (s === 'DATA_STALE') return 'near'
  return 'fail'
}

export function labelTone(label) {
  const s = String(label || '').toLowerCase()
  if (s.includes('under')) return 'bullish'
  if (s.includes('over')) return 'bearish'
  return 'neutral'
}

export function fmtPrice(v, digits = 4) {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return n.toFixed(digits)
}

export function fmtPct(v) {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`
}

export function iveSummaryLine(ive) {
  if (!ive) return ''
  return [
    `${ive.valuationLabel} (${fmtPct(ive.valuationPct)})`,
    `Grade: ${ive.valuationGrade}`,
    `Status: ${ive.modelStatus}`,
  ].join(' · ')
}

export function valuationBlockForMarket(doc, marketId) {
  if (!doc || !marketId) return null
  const instruments = doc.instruments || doc.markets || {}
  return instruments[marketId] || null
}