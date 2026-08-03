import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

import {
  INTERACTION_MODE,
  alignPointsToTimeline,
  assertLivePriceMovesDeviationOnly,
  assertSharedVisibleRange,
  buildLiveValuationState,
  computeLiveDeviationPct,
  extractPhysicalFairValueTip,
  historicalSeriesFingerprint,
  reconcileDisplayedDeviation,
  resolveCurrentPriceSource,
  resolveInteractionMode,
} from './naturalGasValuationWorkstationLive.js'
import { assertLinkedVisibleRanges } from './naturalGasValuationWorkstationModel.js'

const HERE = dirname(fileURLToPath(import.meta.url))

describe('NG valuation workstation live wiring', () => {
  it('market price and fair value come from different fields', () => {
    const tip = extractPhysicalFairValueTip(
      {
        instrument: {
          fair_value: 2.7609,
          model_anchor_price: 2.799,
          as_of_week: '2026-07-30',
          active_model: 'ng_storage_production_v2',
        },
        verdict: { verdict: 'Useful confluence' },
      },
      [],
      'walkforward',
    )
    const priceSource = resolveCurrentPriceSource({
      connected: false,
      status: 'FALLBACK',
      pricesLatestSnapshot: {
        mid: 2.812,
        as_of: '2026-08-03T12:00:00Z',
      },
    })
    const state = buildLiveValuationState({
      physicalTip: tip,
      priceSource,
      researchVerdict: 'Useful confluence',
    })
    assert.equal(state.physical_fair_value, 2.7609)
    assert.equal(state.market_price, 2.812)
    assert.notEqual(state.market_price, state.physical_fair_value)
    assert.notEqual(state.market_price, tip.model_anchor_price)
  })

  it('displayed deviation reconciles exactly for 2.812 / 2.761', () => {
    const market = 2.812
    const fair = 2.761
    const raw = computeLiveDeviationPct(market, fair)
    const displayed = reconcileDisplayedDeviation(market, fair, 2)
    assert.ok(Math.abs(raw - 1.8471568) < 0.001)
    assert.equal(displayed, 1.85)
    const state = buildLiveValuationState({
      physicalTip: { physical_fair_value: fair, model_verdict: 'Useful confluence' },
      priceSource: {
        price: market,
        status: 'SNAPSHOT',
        label: 'SNAPSHOT',
        source: 'OANDA snapshot',
        source_type: 'snapshot',
        trusted_for_comparison: true,
        comparison: 'Current',
        timestamp: '2026-08-03T12:00:00Z',
      },
    })
    assert.equal(state.live_deviation_pct_display, 1.85)
    assert.equal(state.deviation_trusted, true)
    assert.doesNotMatch(state.state_headline, /UNTRUSTED/i)
  })

  it('price update changes deviation while fair value stays fixed', () => {
    const tip = { physical_fair_value: 2.7609, model_as_of: '2026-07-30', model_verdict: 'Useful confluence' }
    const a = buildLiveValuationState({
      physicalTip: tip,
      priceSource: {
        price: 2.761,
        status: 'SNAPSHOT',
        trusted_for_comparison: true,
        comparison: 'Current',
        source_type: 'snapshot',
        source: 'snap',
      },
    })
    const b = buildLiveValuationState({
      physicalTip: tip,
      priceSource: {
        price: 2.812,
        status: 'SNAPSHOT',
        trusted_for_comparison: true,
        comparison: 'Current',
        source_type: 'snapshot',
        source: 'snap',
      },
    })
    const check = assertLivePriceMovesDeviationOnly(a, b)
    assert.equal(check.ok, true)
    assert.equal(check.fair_value_stable, true)
    assert.notEqual(a.live_deviation_pct, b.live_deviation_pct)
    assert.equal(a.physical_fair_value, b.physical_fair_value)
  })

  it('uses prices_latest snapshot even when LivePriceStore status is FALLBACK', () => {
    const src = resolveCurrentPriceSource({
      connected: false,
      status: 'FALLBACK',
      quote: {
        mid: 2.785,
        asOf: '2026-08-03T12:22:56Z',
        providerSymbol: 'NATGAS_USD',
        fallbackClose: 2.799,
        status: 'FALLBACK',
      },
      freshness: { status: 'FALLBACK', ageMs: 60_000 },
    })
    assert.equal(src.price, 2.785)
    assert.equal(src.status, 'SNAPSHOT')
    assert.equal(src.comparison, 'Current')
    assert.equal(src.trusted_for_comparison, true)
    assert.notEqual(src.label, 'LIVE')
  })

  it('forming bar can be Current and is not automatically untrusted', () => {
    const src = resolveCurrentPriceSource({
      connected: false,
      status: 'BACKEND OFFLINE',
      valuationPriceFreshness: {
        forming_daily: {
          date: new Date().toISOString().slice(0, 10),
          close: 2.8,
        },
      },
    })
    assert.equal(src.status, 'FORMING BAR')
    assert.equal(src.comparison, 'Current')
    assert.equal(src.trusted_for_comparison, true)
    const state = buildLiveValuationState({
      physicalTip: { physical_fair_value: 2.7609, model_verdict: 'Useful confluence' },
      priceSource: src,
      researchVerdict: 'Useful confluence',
    })
    assert.equal(state.model_verdict, 'Useful confluence')
    assert.equal(state.comparison_status, 'Current')
    assert.equal(state.deviation_trusted, true)
    assert.doesNotMatch(state.state_headline, /untrusted/i)
  })

  it('stale price does not alter model verdict', () => {
    const state = buildLiveValuationState({
      physicalTip: { physical_fair_value: 2.7609, model_verdict: 'Useful confluence' },
      priceSource: {
        price: 2.4,
        status: 'FALLBACK',
        label: 'FALLBACK',
        source: 'Last completed daily bar',
        source_type: 'completed_daily',
        trusted_for_comparison: false,
        comparison: 'Stale',
      },
      researchVerdict: 'Useful confluence',
    })
    assert.equal(state.model_verdict, 'Useful confluence')
    assert.equal(state.comparison_status, 'Stale')
    assert.equal(state.deviation_trusted, false)
    assert.doesNotMatch(state.state_headline, /UNTRUSTED/)
  })

  it('keeps interaction modes distinct; lock not overwritten by live helpers', () => {
    assert.equal(resolveInteractionMode({ lockedTime: null, hoverTime: null }), INTERACTION_MODE.LIVE)
    assert.equal(
      resolveInteractionMode({ lockedTime: null, hoverTime: 100 }),
      INTERACTION_MODE.HOVER_PREVIEW,
    )
    assert.equal(
      resolveInteractionMode({ lockedTime: 200, hoverTime: 100 }),
      INTERACTION_MODE.LOCKED_HISTORY,
    )
  })

  it('charts cannot hold independent ranges', () => {
    const ok = assertSharedVisibleRange({ from: 10, to: 40 }, { from: 10, to: 40 })
    const bad = assertSharedVisibleRange({ from: 10, to: 40 }, { from: 11, to: 40 })
    assert.equal(ok.ok, true)
    assert.equal(bad.ok, false)
  })

  it('hover preview does not change visible range contract', () => {
    const range = { from: 10, to: 40 }
    const before = assertLinkedVisibleRanges(range, range, null)
    const afterHover = assertLinkedVisibleRanges(range, { ...range }, '2020-01-01')
    assert.equal(before.ok, true)
    assert.equal(afterHover.ok, true)
    assert.equal(before.price_visible_from, afterHover.price_visible_from)
    assert.equal(before.price_visible_to, afterHover.price_visible_to)
  })

  it('aligned timeline keeps identical logical length across panes', () => {
    const timeline = [
      { time: 1, date: 'a' },
      { time: 2, date: 'b' },
      { time: 3, date: 'c' },
    ]
    const price = alignPointsToTimeline(timeline, [
      { time: 1, value: 2 },
      { time: 2, value: 3 },
      { time: 3, value: 4 },
    ])
    const valuation = alignPointsToTimeline(timeline, [{ time: 3, value: 1.5 }])
    assert.equal(price.length, valuation.length)
    assert.equal(price.length, 3)
    assert.equal(valuation[0].value, undefined)
    assert.equal(valuation[2].value, 1.5)
  })

  it('historical series fingerprint unchanged when live price moves', () => {
    const weeks = [
      { model_week: '2026-07-23', walk_forward: { fair_value: 2.7, deviation_pct: 1 } },
      { model_week: '2026-07-30', walk_forward: { fair_value: 2.7609, deviation_pct: 1.38 } },
    ]
    const fp = historicalSeriesFingerprint(weeks, 'walkforward')
    const tip = extractPhysicalFairValueTip(
      { instrument: { fair_value: 2.7609 } },
      weeks,
      'walkforward',
    )
    buildLiveValuationState({
      physicalTip: tip,
      priceSource: {
        price: 2.9,
        status: 'LIVE',
        trusted_for_comparison: true,
        comparison: 'Current',
        source_type: 'websocket',
        source: 'ws',
      },
      historicalSeriesFingerprint: fp,
    })
    assert.equal(historicalSeriesFingerprint(weeks, 'walkforward'), fp)
  })

  it('page contracts: Return to Live, syncFollower, no untrusted model label, no COT wiring', () => {
    const page = readFileSync(join(HERE, 'NaturalGasValuationWorkstationPage.jsx'), 'utf8')
    assert.match(page, /data-testid="ngvw-live-card"/)
    assert.match(page, /data-testid="ngvw-return-live"/)
    assert.match(page, /data-testid="ngvw-comparison-status"/)
    assert.match(page, /data-testid="ngvw-model-verdict"/)
    assert.match(page, /syncFollower/)
    assert.match(page, /alignPointsToTimeline/)
    assert.match(page, /assertSharedVisibleRange/)
    assert.match(page, /prices_latest\.json/)
    assert.match(page, /ngvw-live-diag/)
    assert.doesNotMatch(page, /untrusted/i)
    assert.doesNotMatch(page, /run_weekly_cot/)
    assert.doesNotMatch(page, /HPTL_SKIP_VALUATION/)
    assert.match(page, /CURRENT NATURAL GAS VALUATION/)
    assert.match(page, /LOCKED_HISTORY \? lockedTime : null/)
  })
})
