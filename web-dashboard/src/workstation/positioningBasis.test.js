import test from 'node:test'
import assert from 'node:assert/strict'
import {readFileSync} from 'node:fs'
import {positioningBasis, formatRangeIndex} from './positioningBasis.js'
test('Palladium live screenshot: different baselines give 13.26 and 38.55 without changing the net', () => {
 const doc=JSON.parse(readFileSync(new URL('../../public/data/cot_weekly_inspector_latest.json', import.meta.url)))
 const history=doc.markets.Palladium.rows.map(r=>({date:r[0],net:r[2][0]}))
 const all=positioningBasis(history,'2026-10-13','net','all'); const three=positioningBasis(history,'2026-10-13','net','3y')
 assert.equal(all.current,-8078);assert.equal(three.current,-8078)
 assert.equal(formatRangeIndex(all.index),'13.26');assert.equal(formatRangeIndex(three.index),'38.55')
 assert.equal(all.minimum,-13914);assert.equal(all.maximum,30090);assert.equal(three.maximum,1225)
 assert.equal(three.count,156)
})
test('evidence is point-in-time; missing, zero and flat values are handled explicitly',()=>{
 const h=[{date:'2020-01-01',net:-10},{date:'2020-01-08',net:0},{date:'2020-01-15',net:1000}]
 assert.equal(positioningBasis(h,'2020-01-08','net','all').index,100)
 assert.equal(positioningBasis(h,'2020-01-09','net','all'),null)
 assert.equal(positioningBasis(h,'2020-01-08','net','bad'),null)
 assert.equal(positioningBasis(h.map(r=>({...r,net:0})),'2020-01-15','net','all').index,null)
 assert.equal(formatRangeIndex(null),'Unavailable')
})
