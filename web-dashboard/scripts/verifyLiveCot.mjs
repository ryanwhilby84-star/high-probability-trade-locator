// Verify deployed participant readings across both display models and every lookback.
import assert from 'node:assert/strict'
import { validateWeeklyInspector, inspectorDates } from '../src/data/weeklyInspectorStore.js'
import { attentionForWeek } from '../src/data/attentionWeek.js'
const base = process.argv[2] || 'https://institutional.ztack.co.uk'
const expected = process.argv[3]
if (!expected) throw new Error('Usage: node verifyLiveCot.mjs URL YYYY-MM-DD')
const get = async name => {
  const response = await fetch(`${base}/data/${name}?verify=${Date.now()}`, {cache:'no-store'})
  assert.equal(response.status,200,name)
  return response.json()
}
const doc = validateWeeklyInspector(await get('cot_weekly_inspector_latest.json'))
assert.equal(inspectorDates(doc).at(-1),expected)
const radar = attentionForWeek(doc,expected)
assert.equal(radar.rows.length,26)
let checked=0
for (const row of radar.rows) {
  const week=doc.markets[row.name].rows.at(-1)
  for (const [window,index] of Object.entries({'3y':4,'5y':12,'10y':14,'15y':13,all:15})) {
    for (const [key,group] of [['c',1],['nc',2],['nr',3]]) {
      assert.equal(row.percentilesByWindow[window][key],week[group][index] ?? null, `${row.name} ${window} ${key}`)
      checked++
    }
  }
  assert.equal(row.change,week[1][1]);assert.equal(row.ncChange,week[2][1])
}
const snapshot=await get('cot_attention_80_20_latest.json')
assert.equal(snapshot.latestDate,expected)
assert.equal(snapshot.rows.length,26)
for (const row of snapshot.rows) {
  const matching=radar.rows.find(r=>r.name===row.name)
  for (const key of ['c','nc','nr','change','ncChange']) assert.equal(row[key],matching[key],`${row.name} ${key}`)
}
const startup=await get('confluence_bootstrap_latest.json')
assert.equal(startup.bootstrap.latest_date,expected)
console.log(`PASS: production ${expected}; 26 markets; ${checked} Radar/Inspector lookback readings; weekly changes, Attention snapshot and startup dates agree`)
console.log(JSON.stringify(radar.rows.filter(r=>['Sugar','Silver','Natural Gas / NG'].includes(r.name)),null,2))
