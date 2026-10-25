import test from 'node:test'
import assert from 'node:assert/strict'
import { attentionForWeek } from './attentionWeek.js'
const group = (pct, change) => [100, change, null, null, pct, null, null, null, 156, 0, 0, 0, pct+1, pct+2, pct+3, pct+4]
const doc = { markets: { Sugar: { available: true, rows: [
  ['2026-10-13', group(10,-20),group(90,20),group(40,0)],
  ['2026-10-20', group(15,30),group(85,-30),group(45,1)]
] } } }
test('Selected week changes published indices and weekly changes together', () => {
  const old = attentionForWeek(doc,'2026-10-13'), current = attentionForWeek(doc,'2026-10-20')
  assert.equal(old.rows[0].c,10)
  assert.equal(current.rows[0].c,15)
  assert.equal(old.rows[0].change,-20)
  assert.equal(current.rows[0].change,30)
  assert.equal(old.rows[0].percentilesByWindow.all.c,14)
  assert.equal(current.rows[0].percentilesByWindow.all.c,19)
})
test('Missing week does not silently reuse latest numbers', () => {
  assert.deepEqual(attentionForWeek(doc,'2026-10-06').rows,[])
})
