/** Institutional scanner model (L1–L5). Scores are internal only — not shown on scanner. */

export function getInstitutionalContext(row) {
  const ctx = row?.institutional_context
  return ctx && typeof ctx === 'object' ? ctx : null
}

function fromScannerDisplay(row) {
  const ctx = getInstitutionalContext(row)
  return ctx?.scanner_display && typeof ctx.scanner_display === 'object' ? ctx.scanner_display : null
}

/** Five narrative lines for scanner card UI. */
export function scannerContextLines(row) {
  const sd = fromScannerDisplay(row)
  if (sd?.lines?.length) return sd.lines
  const ctx = getInstitutionalContext(row)
  if (!ctx) return null
  return [
    { layer: 'STRUCTURAL', value: structuralRegimeLabel(row) },
    { layer: 'FLOW', value: flowMomentumLabel(row) },
    { layer: 'MACRO', value: macroAlignmentLabel(row) },
    { layer: 'EXHAUSTION', value: exhaustionLabel(row) },
    { layer: 'TACTICAL', value: tacticalActionLabel(row) || '—' },
  ]
}

export function structuralRegimeLabel(row) {
  const sd = fromScannerDisplay(row)
  if (sd?.structural) return sd.structural
  const ctx = getInstitutionalContext(row)
  if (ctx?.structural_regime_label) {
    const lab = ctx.structural_regime_label
    return lab.startsWith('Structural ') ? lab.slice(11) : lab
  }
  return '—'
}

export function flowMomentumLabel(row) {
  const sd = fromScannerDisplay(row)
  if (sd?.flow) return sd.flow
  return getInstitutionalContext(row)?.flow_momentum_label || '—'
}

export function exhaustionLabel(row) {
  const sd = fromScannerDisplay(row)
  if (sd?.exhaustion) return sd.exhaustion
  const ctx = getInstitutionalContext(row)
  if (!ctx) return '—'
  const ex = ctx.positioning_extreme
  if (!ex || ex === 'none') return 'Balanced'
  return ctx.positioning_extreme_label || ex
}

export function macroAlignmentLabel(row) {
  const sd = fromScannerDisplay(row)
  if (sd?.macro) return sd.macro
  const ctx = getInstitutionalContext(row)
  return ctx?.macro_alignment_label || '—'
}

export function tacticalActionLabel(row) {
  const sd = fromScannerDisplay(row)
  if (sd?.tactical) return sd.tactical
  return getInstitutionalContext(row)?.tactical_posture_label || null
}

export function hasInstitutionalContext(row) {
  return !!getInstitutionalContext(row)
}

export function structuralBiasTone(row) {
  const ctx = getInstitutionalContext(row)
  const r = (ctx?.structural_regime || '').toLowerCase()
  if (r.includes('bullish') || r === 'accumulation') return 'bull'
  if (r.includes('bearish') || r === 'distribution') return 'bear'
  return 'neutral'
}

export function flowConflictDetail(row) {
  const ctx = getInstitutionalContext(row)
  if (!ctx?.flow_l1_l2_conflict) return null
  return ctx.flow_conflict_narrative || null
}

export function weeksInRegime(row) {
  const ctx = getInstitutionalContext(row)
  const w = ctx?.weeks_in_regime
  return Number.isFinite(w) && w > 0 ? w : null
}

export function dominantNarrative(row) {
  const ctx = getInstitutionalContext(row)
  return ctx?.attention?.dominant_narrative || null
}

export function attentionAlerts(row) {
  return getInstitutionalContext(row)?.attention?.alerts || []
}

export function priorityTier(row) {
  return getInstitutionalContext(row)?.attention?.priority_tier || 'low_priority'
}

export function priorityLabel(row) {
  return getInstitutionalContext(row)?.attention?.priority_label || 'LOW PRIORITY'
}
