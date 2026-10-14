import { readFileSync, writeFileSync } from 'node:fs'
import path from 'node:path'

/** Generate a small attention snapshot from the same COT source as the workstation. */
export function buildAttentionSnapshot(root) {
  const source = path.join(root, 'public/data/cot_3y_series_latest.json')
  const target = path.join(root, 'public/data/cot_attention_80_20_latest.json')
  const doc = JSON.parse(readFileSync(source, 'utf8'))
  const blocks = doc.markets || doc.instruments || doc.data
  if (!blocks || typeof blocks !== 'object') throw new Error('Missing COT markets: refusing empty attention export')
  const entries = Array.isArray(blocks) ? blocks.map(b => [b.market || b.instrument, b]) : Object.entries(blocks)
  function rank(series, field) {
    const values = series.slice(-156).map(r => r[field] == null ? NaN : Number(r[field])).filter(Number.isFinite)
    const n = Number(series.at(-1)?.[field])
    if (values.length < 52 || !Number.isFinite(n)) return null
    return Math.round(1000 * (values.filter(v => v < n).length + (values.filter(v => v === n).length - 1) / 2) / (values.length - 1)) / 10
  }
  const eligible = entries.flatMap(([key, block]) => {
    const series = Array.isArray(block?.series) ? block.series : []
    if (!series.length) return []
    const last = series.at(-1), prev = series.at(-2)
    const c = rank(series, 'commercial_net'), nc = rank(series, 'institutional_net'), nr = rank(series, 'retail_net')
    if (c == null && nc == null && nr == null) return []
    const current = Number(last.commercial_net), previous = Number(prev?.commercial_net)
    return [{ name: block.market || key, date: String(last.date || '').slice(0, 10), c, nc, nr,
      change: Number.isFinite(current) && Number.isFinite(previous) ? current - previous : null }]
  })
  if (!eligible.length) throw new Error('No valid 52-week COT series: refusing empty attention export')
  const latestDate = eligible.map(r => r.date).sort().at(-1)
  if (!/^\\d{4}-\\d{2}-\\d{2}$/.test(latestDate)) throw new Error('Invalid latest COT report date')
  const rows = eligible.filter(r => r.date === latestDate).map(r => ({
    ...r, commercialExtreme: r.c != null && (r.c >= 80 || r.c <= 20),
    retailExtreme: r.nr != null && (r.nr >= 80 || r.nr <= 20),
  })).sort((a,b) => Number(b.commercialExtreme || b.retailExtreme) - Number(a.commercialExtreme || a.retailExtreme) ||
    Math.max(Math.abs((b.c ?? 50) - 50), Math.abs((b.nr ?? 50) - 50)) - Math.max(Math.abs((a.c ?? 50) - 50), Math.abs((a.nr ?? 50) - 50)))
  writeFileSync(target, JSON.stringify({ schema: 1, source_generated_at: doc.generated_at || null, latestDate, eligibleCount: eligible.length, rows }))
  console.log('[attention] generated', rows.length, 'markets for', latestDate)
}
