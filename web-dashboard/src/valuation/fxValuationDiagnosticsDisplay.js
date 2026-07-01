/** Shared FX valuation pipeline diagnostics display helpers. */

export function fmtRawGap(v) {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return `${n >= 0 ? '+' : ''}${n.toFixed(6)}%`
}

export function freshnessTone(status) {
  const s = String(status || '').toLowerCase()
  if (s === 'fresh') return 'pass'
  if (s === 'stale') return 'warn'
  if (s === 'missing') return 'fail'
  return null
}

export function freshnessLabel(status) {
  const s = String(status || '').toLowerCase()
  if (s === 'fresh') return 'Fresh'
  if (s === 'stale') return 'Stale input'
  if (s === 'missing') return 'Missing input'
  return status || '—'
}

export function diagnosticsSummary(diag) {
  if (!diag) return ''
  const parts = [
    diag.freshness_status ? `Input status: ${freshnessLabel(diag.freshness_status)}` : null,
    diag.spot_date ? `Spot as-of ${diag.spot_date}` : null,
    diag.cache_generated_at ? `Cache ${String(diag.cache_generated_at).slice(0, 19)}Z` : null,
    diag.raw_gap_pct_unrounded != null ? `Raw gap ${fmtRawGap(diag.raw_gap_pct_unrounded)}` : null,
  ]
  return parts.filter(Boolean).join(' · ')
}

export function readValuationDiagnostics(block) {
  return block?.valuation_diagnostics || null
}
