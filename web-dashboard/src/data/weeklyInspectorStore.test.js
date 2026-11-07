import test from 'node:test'
import assert from 'node:assert/strict'
import { validateWeeklyInspector, inspectorDates, loadWeeklyInspector, INSPECTOR_PATH } from './weeklyInspectorStore.js'
const group = () => [100, -5, 0, 0, 20]
function document() {
  return { engine: 'weekly_inspector_flow', measure: 'net_positioning_rolling_156_week_percentile', markets: Object.fromEntries(Array.from({length:26}, (_, i) => [String(i), {available:true, rows:[['2026-10-27', group(),group(),group()], ['2026-11-03',group(),group(),group()]]}])) }
}
test('all 26 markets must share the latest report and complete participants', () => {
  assert.equal(validateWeeklyInspector(document()).markets['0'].rows.at(-1)[0], '2026-11-03')
  for (const mutate of [d=>delete d.markets['0'], d=>d.markets['0'].rows.pop(), d=>d.markets['0'].rows.at(-1)[2][4]=null, d=>d.measure='legacy']) {
    const d=document(); mutate(d); assert.throws(()=>validateWeeklyInspector(d))
  }
})
test('date selector only offers dates available for every market', () => {
  const d=document(); d.markets['0'].rows.shift()
  assert.deepEqual(inspectorDates(d), ['2026-11-03'])
})
test('Radar and Inspector share one validated network response', async () => {
  const old=globalThis.fetch; let calls=0
  globalThis.fetch=async (url, options)=>{calls++;assert.equal(url, INSPECTOR_PATH);assert.equal(options.cache,'no-store');return {ok:true,json:async()=>document()}}
  try {const [a,b]=await Promise.all([loadWeeklyInspector(),loadWeeklyInspector()]);assert.equal(calls,1);assert.equal(a,b)} finally {globalThis.fetch=old}
})
