import {build} from 'esbuild'
import {pathToFileURL} from 'node:url'
import {unlink} from 'node:fs/promises'
import assert from 'node:assert/strict'
import React from 'react'
import {renderToStaticMarkup} from 'react-dom/server'
import {readFileSync} from 'node:fs'
const out=new URL('../.inspector-test.mjs',import.meta.url)
try {
 await build({entryPoints:[new URL('../src/workstation/WeeklyInspector.jsx',import.meta.url).pathname],outfile:out.pathname,bundle:true,platform:'node',format:'esm',external:['react','react-dom']})
 const {WeeklyInspector}=await import(pathToFileURL(out.pathname))
 const doc=JSON.parse(readFileSync(new URL('../public/data/cot_weekly_inspector_latest.json',import.meta.url)))
 const rows=doc.markets.Palladium.rows
 const row=rows.find(r=>r[0]==='2026-10-13'); const nc=row[2]
 const history=rows.map(r=>({date:r[0],commercial_net:r[1][0],institutional_net:r[2][0],retail_net:r[3][0]}))
 const participant={net:nc[0],percentilesByWindow:{'3y':nc[4],all:nc[15]},percentileChange4w:nc[6],stateLabel:'Rotation weakening',summaryLine:'Non-Commercial positioning is at the 39th net percentile.'}
 const week={instrument:'Palladium',date:row[0],integrityOk:true,nonCommercial:participant,spreads:{}}
 for(const [rangeId,expected] of [['all','13.26'],['3y','38.55']]) {
  const html=renderToStaticMarkup(React.createElement(WeeklyInspector,{open:true,week,rangeId,history}))
  assert.ok(html.includes(`${expected} / 100`),html)
  assert.ok(html.includes('calculation evidence'));assert.ok(html.includes('3Y analysis:'))
  assert.ok(html.includes('Not a statistical percentile rank'));assert.ok(!html.includes('DATA MISMATCH'))
  assert.ok(!html.includes('39th net percentile'));assert.ok(html.includes('39 / 100 COT range index'))
 }
 const html=renderToStaticMarkup(React.createElement(WeeklyInspector,{open:true,week:{...week,nonCommercial:{...participant,percentilesByWindow:{all:99}}},rangeId:'all',history}))
 assert.ok(html.includes('DATA MISMATCH'));assert.ok(!html.includes('99.00 / 100'))
 console.log('PASS: rendered Palladium ALL/3Y cards, baseline labels, evidence, terminology and mismatch guard')
} finally {await unlink(out).catch(()=>{})}
