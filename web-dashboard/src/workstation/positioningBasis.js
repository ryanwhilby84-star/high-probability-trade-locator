const windows = { '3y': 156, '5y': 260, '10y': 520, '15y': 780 }
export function formatRangeIndex(value) {
  return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(2) : 'Unavailable'
}
export function positioningBasis(history, date, key, rangeId) {
  if (rangeId !== 'all' && !windows[rangeId]) return null
  const rows = (history || []).filter(r => r.date && r.date <= date).sort((a,b) => a.date.localeCompare(b.date))
  if (!rows.length || rows.at(-1).date !== date) return null
  const selected = rangeId === 'all' ? rows : rows.slice(-windows[rangeId])
  const valid = selected.filter(r => typeof r[key] === 'number' && Number.isFinite(r[key]))
  const current = selected.at(-1)[key]
  if (typeof current !== 'number' || !Number.isFinite(current) || valid.length < 2) return null
  const minimum = Math.min(...valid.map(r => r[key]))
  const maximum = Math.max(...valid.map(r => r[key]))
  return { current, minimum, maximum, count: valid.length, start: selected[0].date, end: date,
    index: maximum === minimum ? null : 100 * (current - minimum) / (maximum - minimum) }
}

export function describeIndexSummary(text) {
  return String(text || '').replace(/(\d+)(?:st|nd|rd|th) net percentile/g, '$1 / 100 COT range index').replace(/percentile points/g, 'index points')
}
