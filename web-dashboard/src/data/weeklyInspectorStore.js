// One validated report snapshot per page session for Radar and Weekly Inspector.
// A full reload fetches the newly published release; failed requests may retry.
export const INSPECTOR_PATH = '/data/cot_weekly_inspector_latest.json'
let pending = null

export function validateWeeklyInspector(doc) {
  if (doc?.engine !== 'weekly_inspector_flow' || doc.measure !== 'net_positioning_rolling_156_week_percentile') {
    throw new Error('Incompatible Weekly Inspector dataset')
  }
  const markets = Object.entries(doc.markets || {})
  if (markets.length !== 26) throw new Error('Weekly Inspector must contain all 26 COT markets')
  let latest = null
  for (const [name, market] of markets) {
    const row = market?.rows?.at(-1)
    if (!market.available || !row || !/^\d{4}-\d{2}-\d{2}$/.test(row[0])) throw new Error('Missing Inspector report for ' + name)
    if (latest && latest !== row[0]) throw new Error('Inspector market report dates disagree')
    latest = row[0]
    if (![row[1], row[2], row[3]].every(g => Array.isArray(g) && [0, 1, 4].every(i => Number.isFinite(g[i])) && g[4] >= 0 && g[4] <= 100)) {
      throw new Error('Incomplete Inspector participant readings for ' + name)
    }
  }
  return doc
}

export function inspectorDates(doc) {
  const markets = Object.values(doc?.markets || {})
  if (!markets.length) return []
  const sets = markets.slice(1).map(m => new Set(m.rows.map(r => r[0])))
  return markets[0].rows.map(r => r[0]).filter(d => sets.every(s => s.has(d))).sort()
}

export function loadWeeklyInspector() {
  if (!pending) pending = fetch(INSPECTOR_PATH, { cache: 'no-store' })
    .then(r => { if (!r.ok) throw new Error('Inspector HTTP ' + r.status); return r.json() })
    .then(validateWeeklyInspector)
    .catch(error => { pending = null; throw error })
  return pending
}
