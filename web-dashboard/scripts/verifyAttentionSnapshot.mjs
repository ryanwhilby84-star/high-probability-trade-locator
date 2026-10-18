import { readFileSync } from 'node:fs'
import path from 'node:path'
import { buildAttentionSnapshot } from './buildAttentionSnapshot.mjs'

const root = path.resolve(import.meta.dirname || path.dirname(new URL(import.meta.url).pathname), '..')
buildAttentionSnapshot(root)
const inspector = JSON.parse(readFileSync(path.join(root, 'public/data/cot_weekly_inspector_latest.json'), 'utf8'))
const attention = JSON.parse(readFileSync(path.join(root, 'public/data/cot_attention_80_20_latest.json'), 'utf8'))
const failures = []
let checked = 0
for (const row of attention.rows) {
  const market = inspector.markets[row.name]
  const week = market?.rows?.at(-1)
  if (!week || week[0] !== row.date) { failures.push(row.name + ': missing or mismatched date'); continue }
  for (const [label, actual, group] of [['commercial', row.c, week[1]], ['noncommercial', row.nc, week[2]], ['nonreportable', row.nr, week[3]]]) {
    if (actual !== group?.[4]) failures.push(row.name + ' ' + label + ': ' + actual + ' vs ' + group?.[4])
    checked++
  }
  for (const [window, index] of Object.entries({ "3y": 4, "5y": 12, "10y": 14, "15y": 13, "all": 15 })) {
    for (const [label, group, key] of [["commercial", week[1], "c"], ["noncommercial", week[2], "nc"], ["nonreportable", week[3], "nr"]]) {
      const expected = group?.[index] ?? null
      const actual = row.percentilesByWindow?.[window]?.[key] ?? null
      if (expected !== actual) failures.push(row.name + " " + window + " " + label + ": " + actual + " vs " + expected)
      checked++
    }
  }
  if (row.change !== (typeof week[1]?.[1] === 'number' ? week[1][1] : null)) failures.push(row.name + ': commercial change mismatch')
  if (row.ncChange !== (typeof week[2]?.[1] === 'number' ? week[2][1] : null)) failures.push(row.name + ': noncommercial change mismatch')
  if (row.commercialExtreme !== (row.c != null && (row.c >= 80 || row.c <= 20))) failures.push(row.name + ': commercial alert mismatch')
  if (row.retailExtreme !== (row.nr != null && (row.nr >= 80 || row.nr <= 20))) failures.push(row.name + ': nonreportable alert mismatch')
}
if (!checked || failures.length) {
  console.error(JSON.stringify({ checked, failures }, null, 2))
  process.exit(1)
}
console.log('PASS: ' + checked + ' participant percentile comparisons, ' + attention.rows.length + ' markets, ' + attention.latestDate)
