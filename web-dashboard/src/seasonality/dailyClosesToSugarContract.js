/** Convert verified daily closes to the exact return-row contract consumed by
 * the original Sugar seasonal-window calculations. No forward filling.
 * Generic daily prices are not equivalent to a roll-audited futures archive.
 */
export function dailyClosesToSugarContract(bars) {
  if (!Array.isArray(bars)) throw new Error('Expected daily close rows')
  const sorted = bars.map(b => Array.isArray(b)
    ? { date: b[0], close: Number(b[1]) }
    : { date: b.date, close: Number(b.close) })
    .filter(b => /^\d{4}-\d{2}-\d{2}$/.test(b.date) && Number.isFinite(b.close) && b.close > 0)
    .sort((a, b) => a.date.localeCompare(b.date))
  if (sorted.length < 2) throw new Error('Insufficient valid daily observations')
  for (let i = 1; i < sorted.length; i++) {
    if (sorted[i].date <= sorted[i - 1].date) throw new Error('Duplicate daily dates')
  }
  const coverage = new Map()
  for (const b of sorted) {
    const year = +b.date.slice(0, 4)
    const rec = coverage.get(year) || { count: 0, first: b.date, last: b.date }
    rec.count += 1
    rec.last = b.date
    coverage.set(year, rec)
  }
  // Reject partial first/last years; a minimum bar count alone is insufficient.
  const full_years = [...coverage.entries()]
    .filter(([year, rec]) => rec.count >= 180 && rec.first <= `${year}-01-15` && rec.last >= `${year}-12-15`)
    .map(([year]) => year).sort((a,b)=>a-b)
  if (!full_years.length) throw new Error('No complete historical years with adequate daily observations')
  // Sugar evaluates cross-year windows only when the following year has data.\n  // The engine enforces that boundary; a complete final year is still eligible\n  // for windows ending in the same year.\n  const rows = []
  for (let i = 1; i < sorted.length; i++) {
    const prev = sorted[i - 1], cur = sorted[i]
    const ret = cur.close / prev.close - 1
    const gap = (Date.parse(cur.date) - Date.parse(prev.date)) / 86400000
    // An unverified discontinuity or lengthy missing interval is excluded in strict mode.
    const eligible = gap <= 5 && Math.abs(ret) <= 0.1 ? 1 : 0
    rows.push([cur.date, prev.date, ret, eligible, 'UNVERIFIED_DAILY_CLOSE'])
  }
  return {
    schema_version: 1, instrument: 'Generic daily closes',
    source: 'Daily close import; roll continuity unverified',
    first_date: sorted[0].date, last_date: sorted.at(-1).date,
    full_years, rows,
  }
}
