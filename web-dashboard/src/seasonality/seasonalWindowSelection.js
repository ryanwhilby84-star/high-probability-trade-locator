// Shared interval selection for annual return archives. Do not turn opposing
// overlapping horizons into a single directional signal or count variants twice.
export function selectDistinctWindows(candidates, limit = 8) {
  const kept = []
  for (const candidate of candidates) {
    const duplicate = kept.find(edge => {
      if (edge.direction !== candidate.direction) return false
      const overlap = Math.max(0, Math.min(edge.end_offset, candidate.end_offset) - Math.max(edge.days_until_start, candidate.days_until_start))
      const union = Math.max(edge.end_offset, candidate.end_offset) - Math.min(edge.days_until_start, candidate.days_until_start)
      const shorter = Math.min(edge.end_offset - edge.days_until_start, candidate.end_offset - candidate.days_until_start)
      const longer = Math.max(edge.end_offset - edge.days_until_start, candidate.end_offset - candidate.days_until_start)
      return overlap / union >= .6 || (overlap / shorter >= .8 && longer / shorter <= 3)
    })
    if (duplicate) { duplicate.variant_count += 1; continue }
    if (kept.length < limit) kept.push({ ...candidate, variant_count: 1 })
  }
  return kept.sort((a, b) => a.days_until_start - b.days_until_start || a.end_offset - b.end_offset).map(edge => ({
    ...edge,
    opposing_windows: kept.filter(other => other.direction !== edge.direction && Math.min(other.end_offset, edge.end_offset) > Math.max(other.days_until_start, edge.days_until_start)).map(other => ({ window: other.window, direction: other.direction })),
  }))
}

export function directionalStats(outcomes, direction) {
  const values = outcomes.map(row => row.returnPct)
  const sorted = [...values].sort((a, b) => a - b)
  const wins = values.filter(value => direction === 'Bullish' ? value > 0 : value < 0).length
  const mean = values.length ? values.reduce((a, b) => a + b, 0) / values.length : null
  const median = sorted.length ? (sorted[Math.floor((sorted.length - 1) / 2)] + sorted[Math.floor(sorted.length / 2)]) / 2 : null
  return { n: values.length, wins, frequency: values.length ? wins / values.length : null, mean, median }
}

export function validationPass(stats, direction) {
  const sign = direction === 'Bullish' ? 1 : -1
  return stats.n >= 6 && stats.frequency > .6 && stats.mean * sign > 0 && stats.median * sign > 0
}
