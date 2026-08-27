const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

/** Compact terminal-style COT value (levels and deltas). */
export function formatCotStatValue(value, { signed = false } = {}) {
  if (!isNum(value)) return '—'
  const abs = Math.abs(value)
  let sign = ''
  if (signed && value > 0) sign = '+'
  if (value < 0) sign = '-'

  if (abs >= 1_000_000) return `${sign}${(abs / 1_000_000).toFixed(1)}M`
  if (abs >= 10_000) return `${sign}${Math.round(abs / 1_000)}k`
  if (abs >= 1_000) return `${sign}${(abs / 1_000).toFixed(1)}k`
  return `${sign}${Math.round(abs)}`
}

/** Current net + week-over-week changes from aligned weekly line points. */
export function computeCotPanelStats(linePoints) {
  if (!linePoints?.length) return null
  const lastIdx = linePoints.length - 1
  const current = linePoints[lastIdx]?.value
  if (!isNum(current)) return null

  const deltaWeeks = (weeks) => {
    const idx = lastIdx - weeks
    if (idx < 0) return null
    const prior = linePoints[idx]?.value
    if (!isNum(prior)) return null
    return current - prior
  }

  return {
    current,
    w1: deltaWeeks(1),
    w4: deltaWeeks(4),
    w12: deltaWeeks(12),
  }
}
