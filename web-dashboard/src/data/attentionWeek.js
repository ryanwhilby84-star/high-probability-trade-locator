// Copy the Inspector's published values; never recalculate participant ranks.
export function attentionForWeek(doc, date) {
  const indices = { '3y': 4, '5y': 12, '10y': 14, '15y': 13, all: 15 }
  const rows = []
  for (const [name, market] of Object.entries(doc?.markets || {})) {
    if (!market?.available) continue
    const week = market.rows?.find(row => row[0] === date)
    if (!week) continue
    const [, c, nc, nr] = week
    if (![c, nc, nr].every(Array.isArray)) continue
    const value = (group, index) => Number.isFinite(group[index]) ? group[index] : null
    rows.push({ name, date, c: value(c, 4), nc: value(nc, 4), nr: value(nr, 4),
      change: value(c, 1), ncChange: value(nc, 1),
      percentilesByWindow: Object.fromEntries(Object.entries(indices).map(([window, index]) =>
        [window, { c: value(c, index), nc: value(nc, index), nr: value(nr, index) }])) })
  }
  return { latestDate: date, eligibleCount: rows.length, rows }
}
