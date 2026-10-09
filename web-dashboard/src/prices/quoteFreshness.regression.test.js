import test from 'node:test'
import assert from 'node:assert/strict'
import { quoteDisplayStatus, quoteAgeMs, nullableNumber } from './quoteFreshness.js'
import { rollingInspector } from '../workstation/data/rollingInspector.js'
import { buildRolling3yContextFromWeeks } from '../cot/rolling3yFromLegacyWeeks.js'

test('silent connected feed expires; heartbeat is not a fresh quote', () => {
  const timestamp = '2026-10-07T06:00:00Z', now = Date.parse(timestamp)
  const price = { status:'LIVE', timestamp }
  assert.equal(quoteDisplayStatus(price, 'connected', now + 59000), 'LIVE')
  assert.equal(quoteDisplayStatus(price, 'connected', now + 61000), 'STALE')
  assert.equal(quoteDisplayStatus({status:'LIVE'}, 'connected', now), 'STALE')
  assert.equal(quoteDisplayStatus(price, 'disconnected', now), 'BACKEND OFFLINE')
  assert.equal(quoteAgeMs('bad', now), null)
  assert.equal(quoteAgeMs('2026-10-08', now), null)
  assert.equal(nullableNumber(null), null)
  assert.equal(nullableNumber(0), 0)
})

test('3Y inspector agrees with raw sheet after old historical extremes roll out', () => {
  const weeks = Array.from({length:300}, (_, i) => ({
    date:new Date(Date.UTC(2020,0,7+i*7)).toISOString().slice(0,10),
    commercial:{net:i < 144 ? 100000 : i},
    noncommercial:{net:i < 144 ? -100000 : -i},
    nonreportable:{net:i*2},
  }))
  const full = rollingInspector({weeks}), last = full.weeks.at(-1)
  const sheet = buildRolling3yContextFromWeeks(weeks.map(w=>({report_date:w.date,net:w.commercial.net})))
  assert.equal(last.commercial.percentile_observation_count,156)
  assert.ok(last.commercial.percentile > 99)
  assert.equal(last.commercial.percentile,sheet.net_percentile)
  assert.equal(last.cross.comm_nc_spread, Math.round((last.commercial.percentile-last.noncommercial.percentile)*100)/100)
  const truncated = rollingInspector({weeks:weeks.slice(0,200)})
  assert.deepEqual(full.weeks[199],truncated.weeks[199])
})

test('missing positions are excluded, never converted to zero', () => {
  const sheet=buildRolling3yContextFromWeeks([{report_date:'2026-09-22',net:null},{report_date:'2026-09-29',net:10}])
  assert.equal(sheet.rows_used,1)
  assert.equal(sheet.net_percentile,50)
  const missing=buildRolling3yContextFromWeeks([{report_date:'2026-09-29',net:null}])
  assert.equal(missing.net_percentile,null)
})

test('shared price stores expire the rendered snapshot and reject older reconnect quotes', async () => {
  const originalFetch=globalThis.fetch, originalNow=Date.now
  let now=Date.parse('2026-10-07T06:00:00Z'), tick, socket
  Date.now=()=>now
  globalThis.window={location:{protocol:'http:',host:'localhost:5173'},setTimeout,clearTimeout,setInterval:(fn)=>{tick=fn;return 1},clearInterval:()=>{}}
  globalThis.WebSocket=class {
    static OPEN=1; static CONNECTING=0
    constructor(){socket=this;this.readyState=0}
    close(){this.readyState=3}
  }
  globalThis.fetch=async (url)=>({ok:true,json:async()=>url.includes('weekly-candles')?{weekly_candles:{}}:url.includes('prices_latest')?{instruments:{}}:{prices:{}}})
  const {LivePriceStore}=await import('./stores/LivePriceStore.js')
  const {CurrentPriceStreamStore}=await import('./stores/CurrentPriceStreamStore.js')
  const unsub=LivePriceStore.subscribe(()=>{})
  try {
    await new Promise(setImmediate); await new Promise(setImmediate)
    socket.readyState=1;socket.onopen()
    socket.onmessage({data:JSON.stringify({type:'snapshot',prices:{Sugar:{internal_key:'Sugar',status:'LIVE',timestamp:'2026-10-07T06:00:00Z',mid:15,bid:null,ask:null}}})})
    assert.equal(LivePriceStore.getQuote('Sugar').bid,null)
    assert.equal(LivePriceStore.getStatus('Sugar'),'LIVE')
    const before=LivePriceStore.getSnapshot()
    now+=61000;tick()
    assert.notEqual(LivePriceStore.getSnapshot(),before)
    assert.equal(LivePriceStore.getStatus('Sugar'),'STALE')
    assert.equal(LivePriceStore.getFreshness('Sugar').ageMs,61000)
    socket.onmessage({data:JSON.stringify({type:'snapshot',prices:{Sugar:{internal_key:'Sugar',status:'LIVE',timestamp:'2026-10-07T05:00:00Z',mid:9}}})})
    assert.equal(CurrentPriceStreamStore.getPrice('Sugar').mid,15)
    assert.equal(LivePriceStore.getQuote('Gold'),null)
  } finally {
    unsub();globalThis.fetch=originalFetch;Date.now=originalNow
    delete globalThis.window;delete globalThis.WebSocket
  }
})

test('raw archive fetch errors remain visible and can be retried', async () => {
  const original=globalThis.fetch
  const {loadLegacyCotLatest}=await import('../legacyCotData.js')
  let attempts=0
  globalThis.fetch=async()=>{attempts++;return attempts===1?{ok:false,status:503}:{ok:true,json:async()=>({instruments:{Sugar:{groups:{}}}})}}
  try {
    await assert.rejects(loadLegacyCotLatest(), /503/)
    assert.ok((await loadLegacyCotLatest()).instruments.Sugar)
    assert.equal(attempts,2)
  } finally {globalThis.fetch=original}
})
