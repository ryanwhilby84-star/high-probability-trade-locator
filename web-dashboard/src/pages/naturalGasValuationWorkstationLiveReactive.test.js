import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

import {
  LIVE_QUOTE_STALE_MS,
  UPDATE_MODE,
  applyQuoteToLiveState,
  buildHeartbeat,
  computeLiveDeviationPct,
  reduceLiveQuoteTick,
  resolveUpdateMode,
} from './naturalGasValuationWorkstationLive.js'

const HERE = dirname(fileURLToPath(import.meta.url))
const FAIR = 2.7609

describe('NG valuation workstation reactive live market', () => {
  it('incoming WebSocket quote updates price and deviation; fair value fixed', () => {
    const a = reduceLiveQuoteTick(
      null,
      { price: 2.785, source_type: 'websocket', source: 'ws', timestamp: 't1' },
      FAIR,
    )
    const b = reduceLiveQuoteTick(
      a,
      { price: 2.812, source_type: 'websocket', source: 'ws', timestamp: 't2' },
      FAIR,
    )
    assert.equal(a.physical_fair_value, FAIR)
    assert.equal(b.physical_fair_value, FAIR)
    assert.notEqual(a.market_price, b.market_price)
    assert.notEqual(a.live_deviation_pct, b.live_deviation_pct)
    assert.equal(b.zoom_reset, false)
    assert.ok(Math.abs(b.live_deviation_pct - computeLiveDeviationPct(2.812, FAIR)) < 1e-9)
  })

  it('socket failure resolves to POLLING when poll quote present', () => {
    const mode = resolveUpdateMode({
      streamIsLive: false,
      hasPollingQuote: true,
      quote: { price: 2.8, receivedAtMs: Date.now() },
      nowMs: Date.now(),
    })
    assert.equal(mode, UPDATE_MODE.POLLING)
  })

  it('polling updates without reload keep fair value and recompute deviation', () => {
    const poll1 = applyQuoteToLiveState({
      marketPrice: 2.79,
      physicalFairValue: FAIR,
      updateMode: UPDATE_MODE.POLLING,
      source: 'OANDA REST poll',
      sourceType: 'polling',
      ageMs: 1000,
    })
    const poll2 = applyQuoteToLiveState({
      marketPrice: 2.81,
      physicalFairValue: FAIR,
      updateMode: UPDATE_MODE.POLLING,
      source: 'OANDA REST poll',
      sourceType: 'polling',
      ageMs: 1000,
    })
    assert.equal(poll1.physical_fair_value, poll2.physical_fair_value)
    assert.notEqual(poll1.live_deviation_pct, poll2.live_deviation_pct)
    assert.equal(poll2.deviation_trusted, true)
    assert.equal(poll2.comparison_status, 'Current')
  })

  it('stale threshold marks update mode STALE', () => {
    const now = Date.now()
    const mode = resolveUpdateMode({
      streamIsLive: true,
      hasPollingQuote: false,
      quote: { price: 2.8, timestamp: new Date(now - LIVE_QUOTE_STALE_MS - 5_000).toISOString() },
      nowMs: now,
      staleMs: LIVE_QUOTE_STALE_MS,
    })
    assert.equal(mode, UPDATE_MODE.STALE)
  })

  it('quote older than allowed age is not presented as current', () => {
    const now = Date.now()
    const mode = resolveUpdateMode({
      streamIsLive: false,
      hasPollingQuote: false,
      quote: {
        price: 2.785,
        timestamp: new Date(now - LIVE_QUOTE_STALE_MS - 60_000).toISOString(),
        source_type: 'snapshot',
      },
      nowMs: now,
      staleMs: LIVE_QUOTE_STALE_MS,
    })
    assert.equal(mode, UPDATE_MODE.STALE)
    const state = applyQuoteToLiveState({
      marketPrice: 2.785,
      physicalFairValue: FAIR,
      updateMode: mode,
      source: 'OANDA snapshot · prices_latest',
      sourceType: 'snapshot',
      timestamp: new Date(now - LIVE_QUOTE_STALE_MS - 60_000).toISOString(),
      ageMs: LIVE_QUOTE_STALE_MS + 60_000,
    })
    assert.equal(state.price_label, UPDATE_MODE.STALE)
    assert.equal(state.comparison_status, 'Stale')
    assert.equal(state.deviation_trusted, false)
    assert.notEqual(state.comparison_status, 'Current')
  })

  it('reconnect restores LIVE mode when stream is live again', () => {
    const polling = resolveUpdateMode({
      streamIsLive: false,
      hasPollingQuote: true,
      quote: { price: 2.8, receivedAtMs: Date.now() },
      nowMs: Date.now(),
    })
    const live = resolveUpdateMode({
      streamIsLive: true,
      hasPollingQuote: true,
      quote: { price: 2.805, timestamp: new Date().toISOString(), ageSeconds: 2 },
      nowMs: Date.now(),
    })
    assert.equal(polling, UPDATE_MODE.POLLING)
    assert.equal(live, UPDATE_MODE.LIVE)
  })

  it('heartbeat badge exposes age and mode', () => {
    const hb = buildHeartbeat({
      updateMode: UPDATE_MODE.LIVE,
      ageMs: 4000,
      connectionState: 'connected',
      reconnectAttempts: 0,
    })
    assert.match(hb.badge, /LIVE/)
    assert.match(hb.badge, /4s ago/)
    assert.equal(hb.connection_status, 'connected')
  })

  it('historical lock / zoom invariants hold across quote ticks', () => {
    const locked = { historical_lock_preserved: true }
    const tick = reduceLiveQuoteTick(
      locked,
      { price: 2.9, source_type: 'websocket', source: 'ws', timestamp: 't3' },
      FAIR,
    )
    assert.equal(tick.historical_lock_preserved, true)
    assert.equal(tick.zoom_reset, false)
  })

  it('page uses reactive hook + ng-live-price path; no static price poll', () => {
    const page = readFileSync(join(HERE, 'NaturalGasValuationWorkstationPage.jsx'), 'utf8')
    const hook = readFileSync(join(HERE, 'useNaturalGasLiveMarket.js'), 'utf8')
    assert.match(page, /useNaturalGasLiveMarket/)
    assert.match(page, /ngvw-heartbeat/)
    assert.match(hook, /\/api\/ng-live-price/)
    assert.match(hook, /30_000/)
    assert.match(hook, /CurrentPriceStreamStore\.reconnect/)
    assert.doesNotMatch(page, /prices_latest\.json/)
    assert.doesNotMatch(page, /run_weekly_cot/)
  })

  it('vite exposes dedicated ng-live-price middleware', () => {
    const vite = readFileSync(join(HERE, '../../vite.config.js'), 'utf8')
    assert.match(vite, /ngLivePricePlugin/)
    assert.match(vite, /\/api\/ng-live-price/)
    assert.match(vite, /fetch_natural_gas_live_quote\.py/)
  })
})
