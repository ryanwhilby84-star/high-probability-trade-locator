/**
 * Valuation confidence v2 display helpers.
 */

export function normalizeConfidenceBand(v) {
  const s = String(v || '').trim().toLowerCase()
  if (!s || s === 'none' || s === 'null') return 'None'
  return s.charAt(0).toUpperCase() + s.slice(1)
}

export function readConfidenceV2(block) {
  if (!block) return null
  const subscores = block.confidence_subscores || {}
  const subBands = block.confidence_subscore_bands || {}
  return {
    band: normalizeConfidenceBand(block.confidence),
    composite: block.confidence_v2_score,
    fit: subBands.fit || scoreToBand(subscores.fit_score),
    data: subBands.data || scoreToBand(subscores.data_score),
    error: subBands.error || scoreToBand(subscores.error_score),
    subscores,
    explanation: block.confidence_explanation || '',
    v1: block.confidence_v1,
  }
}

export function scoreToBand(score) {
  const n = Number(score)
  if (!Number.isFinite(n)) return '—'
  if (n >= 65) return 'High'
  if (n >= 40) return 'Medium'
  return 'Low'
}

/** Compact multi-line label for scanner tooltips. */
export function confidenceTooltipLines(conf) {
  if (!conf) return []
  const lines = [`Confidence: ${conf.band}`]
  if (conf.fit) lines.push(`Fit: ${conf.fit}`)
  if (conf.data) lines.push(`Data: ${conf.data}`)
  if (conf.error) lines.push(`Error: ${conf.error}`)
  if (conf.explanation) lines.push(conf.explanation)
  return lines
}

export function confidenceSummaryLine(conf) {
  if (!conf) return ''
  const parts = [`Confidence: ${conf.band}`]
  if (conf.fit) parts.push(`Fit: ${conf.fit}`)
  if (conf.data) parts.push(`Data: ${conf.data}`)
  if (conf.error) parts.push(`Error: ${conf.error}`)
  return parts.join(' · ')
}
