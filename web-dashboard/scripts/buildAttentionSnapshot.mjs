import { readFileSync, writeFileSync } from 'node:fs'
import path from 'node:path'

/**
 * The Weekly Inspector export is the sole authority for participant percentiles.
 * Never independently re-rank raw COT observations in the Attention Board.
 * Inspector compact row: [date, commercial, noncommercial, nonreportable, cross].
 * Participant pack: [net, 1w_change, 4w_change, 12w_change, percentile, ...].
 */
export function buildAttentionSnapshot(root) {
  const inspectorPath = path.join(root, 'public/data/cot_weekly_inspector_latest.json')
  const target = path.join(root, 'public/data/cot_attention_80_20_latest.json')
  const doc = JSON.parse(readFileSync(inspectorPath, 'utf8'))
  if (doc.engine !== 'weekly_inspector_flow' || !doc.markets || typeof doc.markets !== 'object') {
    throw new Error('Missing authoritative Weekly Inspector export; refusing independent percentile calculation')
  }
  const eligible = []
  const valid = v => typeof v === 'number' && Number.isFinite(v)
  for (const [name, market] of Object.entries(doc.markets)) {
    if (!market?.available || !Array.isArray(market.rows) || !market.rows.length) continue
    const last = market.rows.at(-1)
    if (!Array.isArray(last) || !/^\d{4}-\d{2}-\d{2}$/.test(String(last[0]))) {
      throw new Error('Invalid inspector date for ' + name)
    }
    const [date, cGroup, ncGroup, nrGroup] = last
    if (![cGroup, ncGroup, nrGroup].every(Array.isArray)) throw new Error('Invalid inspector groups for ' + name)
    const c = cGroup[4], nc = ncGroup[4], nr = nrGroup[4]
    if (![c, nc, nr].some(valid)) continue
    for (const [group, p] of [['commercial', c], ['noncommercial', nc], ['nonreportable', nr]]) {
      if (p != null && (!valid(p) || p < 0 || p > 100)) throw new Error('Invalid ' + group + ' percentile for ' + name)
    }
    eligible.push({
      name, date, c, nc, nr,
      change: valid(cGroup[1]) ? cGroup[1] : null,
      ncChange: valid(ncGroup[1]) ? ncGroup[1] : null,
    })
  }
  if (!eligible.length) throw new Error('No eligible Weekly Inspector rows: refusing empty attention export')
  const latestDate = eligible.map(r => r.date).sort().at(-1)
  const rows = eligible.filter(r => r.date === latestDate).map(r => ({
    ...r,
    commercialExtreme: r.c != null && (r.c >= 80 || r.c <= 20),
    retailExtreme: r.nr != null && (r.nr >= 80 || r.nr <= 20),
  })).sort((a, b) =>
    Number(b.commercialExtreme || b.retailExtreme) - Number(a.commercialExtreme || a.retailExtreme) ||
    Math.max(Math.abs((b.c ?? 50) - 50), Math.abs((b.nr ?? 50) - 50)) -
    Math.max(Math.abs((a.c ?? 50) - 50), Math.abs((a.nr ?? 50) - 50))
  )
  writeFileSync(target, JSON.stringify({
    schema: 2,
    percentile_source: 'cot_weekly_inspector_latest.json',
    percentile_measure: doc.measure,
    source_generated_at: doc.generated_at || null,
    latestDate, eligibleCount: eligible.length, rows,
  }))
  console.log('[attention] generated from Weekly Inspector', rows.length, 'markets for', latestDate)
}
