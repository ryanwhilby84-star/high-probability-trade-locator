import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

import {
  DEFAULT_SCALE_MODE,
  FOCUS_SCALE_LIMIT,
  SIGN_CONVENTION,
  applyFocusScale,
  assertLinkedVisibleRanges,
  buildBucketStripCells,
  buildDeviationPoints,
  buildSharedTimeline,
  currentStateFromWeeks,
  decisiveInterpretation,
  returnTone,
  selectedWeekCardModel,
  inspectorForWeek,
  seriesKey,
  signInterpretation,
  timelinesAreSynchronized,
} from './naturalGasValuationWorkstationModel.js'

const HERE = dirname(fileURLToPath(import.meta.url))

const SAMPLE_WEEKS = [
  {
    model_week: '2022-07-01',
    market_price: 5.82,
    storage_surplus_bcf: -40,
    production_yoy_pct: -2,
    quality_status: 'OK',
    walk_forward: {
      fair_value: 5.52,
      deviation_pct: 5.4,
      valuation_bucket: 'overvalued',
      model_type: 'Walk-forward point-in-time',
      coefficients: { intercept: 1.2, storage_surplus_bcf: -0.0008, production_yoy_pct: -0.02 },
    },
    frozen_v2: {
      fair_value: 5.4,
      deviation_pct: 7.8,
      valuation_bucket: 'overvalued',
      model_type: 'Frozen v2 diagnostic',
      coefficients: { intercept: 1.231183, storage_surplus_bcf: -0.000799, production_yoy_pct: -0.021977 },
    },
  },
  {
    model_week: '2022-07-08',
    market_price: 4.0,
    quality_status: 'OK',
    walk_forward: {
      fair_value: 5.0,
      deviation_pct: -20,
      valuation_bucket: 'materially_undervalued',
      coefficients: { intercept: 1.2, storage_surplus_bcf: -0.0008, production_yoy_pct: -0.02 },
    },
    frozen_v2: {
      fair_value: 5.1,
      deviation_pct: -21.5,
      valuation_bucket: 'materially_undervalued',
    },
  },
  {
    model_week: '2022-07-15',
    market_price: 8.0,
    quality_status: 'OK',
    walk_forward: {
      fair_value: 5.0,
      deviation_pct: 60,
      valuation_bucket: 'materially_overvalued',
      coefficients: { intercept: 1.2, storage_surplus_bcf: -0.0008, production_yoy_pct: -0.02 },
    },
    frozen_v2: {
      fair_value: 5.0,
      deviation_pct: 60,
      valuation_bucket: 'materially_overvalued',
    },
  },
]

describe('NG valuation workstation rebuild', () => {
  it('defaults to Focus scale and clips extremes with overflow markers', () => {
    assert.equal(DEFAULT_SCALE_MODE, 'focus')
    const pts = buildDeviationPoints(SAMPLE_WEEKS, 'walkforward')
    const focus = applyFocusScale(pts, 'focus', FOCUS_SCALE_LIMIT)
    assert.equal(focus.clipped, true)
    assert.equal(focus.scaleMin, -40)
    assert.equal(focus.scaleMax, 40)
    const clipped = focus.displayPoints.find((p) => p.value === 40)
    assert.ok(clipped)
    assert.ok(focus.overflowMarkers.some((m) => m.direction === 'up' && m.trueValue === 60))

    const full = applyFocusScale(pts, 'full', FOCUS_SCALE_LIMIT)
    assert.equal(full.clipped, false)
    assert.ok(full.displayPoints.some((p) => p.value === 60))
  })

  it('keeps true extreme values in the inspector while plotting clipped points', () => {
    const insp = inspectorForWeek(SAMPLE_WEEKS[2], 'walkforward', SAMPLE_WEEKS, 2)
    assert.equal(insp.deviation_pct, 60)
    const focus = applyFocusScale(buildDeviationPoints(SAMPLE_WEEKS, 'walkforward'), 'focus')
    assert.ok(focus.displayPoints.every((p) => Math.abs(p.value) <= 40))
  })

  it('builds a large selected-week card with decisive positive/negative wording', () => {
    const insp = inspectorForWeek(SAMPLE_WEEKS[0], 'walkforward', SAMPLE_WEEKS, 0)
    const card = selectedWeekCardModel({ locked: true, inspector: insp, current: null })
    assert.match(card.title, /SELECTED WEEK/)
    assert.equal(card.deviation, 5.4)
    assert.match(card.headline, /MILDLY OVERVALUED/)
    assert.match(card.detail, /not a reversal signal/i)

    const under = decisiveInterpretation(-18.6)
    assert.match(under.headline, /MATERIALLY UNDERVALUED/)
    assert.match(under.detail, /stronger forward returns/i)

    const near = decisiveInterpretation(-1.2)
    assert.match(near.headline, /NEAR FAIR VALUE/)
    assert.match(near.detail, /should not materially affect/i)
  })

  it('uses consistent valuation-deviation sign convention (not standard deviation)', () => {
    assert.match(SIGN_CONVENTION, /Valuation deviation/)
    assert.match(SIGN_CONVENTION, /Positive = market above model fair value = overvalued/)
    assert.match(SIGN_CONVENTION, /Negative = market below model fair value = undervalued/)
    assert.doesNotMatch(SIGN_CONVENTION, /standard deviation/i)
    assert.equal(signInterpretation(5.4).side, 'overvalued')
    assert.equal(signInterpretation(-18.6).side, 'undervalued')
  })

  it('builds bucket-state strip cells aligned to weekly timeline', () => {
    const { times } = buildSharedTimeline(SAMPLE_WEEKS)
    const cells = buildBucketStripCells(SAMPLE_WEEKS, 'walkforward')
    assert.equal(cells.length, times.length)
    assert.equal(cells[0].time, times[0])
    assert.equal(cells[0].bucket, 'overvalued')
    assert.equal(cells[1].bucket, 'materially_undervalued')
    assert.equal(cells[2].color, '#7f1d1d')
  })

  it('colours forward returns by sign', () => {
    assert.equal(returnTone(3.2), 'positive')
    assert.equal(returnTone(-2.1), 'negative')
    assert.equal(returnTone(0.1), 'neutral')
  })

  it('enforces synchronized visible ranges across panes', () => {
    assert.equal(timelinesAreSynchronized(SAMPLE_WEEKS, 'walkforward'), true)
    const ok = assertLinkedVisibleRanges({ from: 1, to: 9 }, { from: 1, to: 9 }, '2022-07-01')
    assert.equal(ok.ok, true)
    assert.equal(ok.independent_navigation_forbidden, true)
    const bad = assertLinkedVisibleRanges({ from: 1, to: 9 }, { from: 2, to: 9 }, '2022-07-01')
    assert.equal(bad.ok, false)
  })

  it('keeps walk-forward and frozen modes distinct', () => {
    assert.notEqual(seriesKey('walkforward'), seriesKey('frozen'))
    const a = inspectorForWeek(SAMPLE_WEEKS[0], 'walkforward', SAMPLE_WEEKS, 0)
    const b = inspectorForWeek(SAMPLE_WEEKS[0], 'frozen', SAMPLE_WEEKS, 0)
    assert.notEqual(a.fair_value, b.fair_value)
  })

  it('page markup matches the rebuilt workstation contract', () => {
    const page = readFileSync(join(HERE, 'NaturalGasValuationWorkstationPage.jsx'), 'utf8')
    const css = readFileSync(join(HERE, 'naturalGasValuationWorkstation.css'), 'utf8')
    assert.match(page, /data-testid="ngvw-selected-week-card"/)
    assert.match(page, /data-testid="ngvw-historical-inspector"/)
    assert.match(page, /data-testid="ngvw-bucket-strip"/)
    assert.match(page, /data-testid="ngvw-zone-layer"/)
    assert.match(page, /data-testid="ngvw-scale-toggle"/)
    assert.match(page, /Focus scale/)
    assert.match(page, /Full scale/)
    assert.match(page, /DEFAULT_SCALE_MODE/)
    assert.match(page, /applyFocusScale/)
    assert.match(page, /data-floating-tabs="off"/)
    assert.match(page, /Return to Live/)
    assert.match(page, /data-testid="ngvw-live-card"/)
    assert.match(page, /CURRENT NATURAL GAS VALUATION/)
    assert.doesNotMatch(page, /standard deviation/i)
    assert.doesNotMatch(page, /ngvw-side/)
    assert.match(css, /ngvw-plot--valuation/)
    assert.match(css, /max\(320px, 34vh\)/)
    assert.match(css, /ngvw-live-card/)
    assert.match(css, /ngvw-bucket-strip/)
    assert.doesNotMatch(page, /run_weekly_cot/)
    assert.doesNotMatch(page, /HPTL_SKIP_VALUATION/)
  })

  it('current card falls back when no week is locked', () => {
    const cur = currentStateFromWeeks(SAMPLE_WEEKS, 'walkforward', 5.9)
    const card = selectedWeekCardModel({ locked: false, inspector: null, current: cur })
    assert.match(card.title, /CURRENT VALUATION/)
    assert.equal(card.locked, false)
  })
})
