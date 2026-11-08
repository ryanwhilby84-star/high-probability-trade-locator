import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import {
  ROADMAP_HORIZON_WEEKS,
  ROADMAP_METHOD_DESCRIPTION,
  ROADMAP_METHOD_LABEL,
  classifyRoadmapHorizon,
  defaultSeasonalView,
  resolveRoadmapSeriesSource,
} from './roadmapView.js'

describe('Seasonal Roadmap view helpers', () => {
  it('defaults active view to roadmap', () => {
    assert.equal(defaultSeasonalView(undefined), 'roadmap')
    assert.equal(defaultSeasonalView({ seasonal_view: 'roadmap' }), 'roadmap')
    assert.equal(defaultSeasonalView({ seasonal_view: 'freeze_index' }), 'freeze_index')
  })

  it('exposes volatility-normalised daily production method and validated horizons', () => {
    assert.equal(ROADMAP_METHOD_LABEL, 'Seasonal Roadmap')
    assert.equal(
      ROADMAP_METHOD_DESCRIPTION,
      'Historical daily moves are normalised by each year’s typical daily volatility, aggregated by trading day, then rescaled to the market’s recent daily volatility. No synthetic noise, interpolation or smoothing.',
    )
    assert.deepEqual(ROADMAP_HORIZON_WEEKS, [4, 8, 12])
  })

  it('classifies horizons from stats only', () => {
    assert.equal(classifyRoadmapHorizon({ mean: 0.01, median: 0.02, bullish_frequency: 0.6, bearish_frequency: 0.4, n: 15 }), 'Bullish')
    assert.equal(classifyRoadmapHorizon({ mean: -0.01, median: -0.02, bullish_frequency: 0.3, bearish_frequency: 0.7, n: 15 }), 'Bearish')
    assert.equal(classifyRoadmapHorizon({ mean: 0.01, median: -0.01, bullish_frequency: 0.55, bearish_frequency: 0.45, n: 15 }), 'Mixed')
  })

  it('production payload always resolves to unsmoothed observations', () => {
    const roadmap = {
      available: true,
      method: { version: 'volatility_normalised_daily_texture_v4' },
      smoothed: null,
      unsmoothed: { full_year: [{ price: 3 }, { price: 4 }] },
    }
    const requestedSmooth = resolveRoadmapSeriesSource(roadmap, true)
    const raw = resolveRoadmapSeriesSource(roadmap, false)
    assert.equal(requestedSmooth.sourcePath, 'payload.seasonal_roadmap.unsmoothed.full_year')
    assert.equal(raw.sourcePath, 'payload.seasonal_roadmap.unsmoothed.full_year')
    assert.equal(requestedSmooth.datasetName, 'volatility_normalised_daily_texture_v4')
    assert.equal(raw.datasetName, 'volatility_normalised_daily_texture_v4')
  })
})

import { calendarRoadmaps } from './roadmapView.js'

describe('calendar projection with stale prices', () => {
  const roadmap = { available: true, asof: '2026-08-13', anchor_price: 100,
    unsmoothed: { full_year: [
      { date: '2026-08-13', price: 100, trading_day: 160, segment: 'today' },
      { date: '2026-11-06', price: 90, trading_day: 220, segment: 'forward' },
      { date: '2026-12-07', price: 99, trading_day: 241, segment: 'forward' },
      { date: '2026-12-31', price: 105, trading_day: 259, segment: 'forward' },
    ] } }
  const weekly = { available: true, current_week: 33, actual_price: { latest_price_date: '2026-08-13' },
    weekly_points: Array.from({ length: 52 }, (_, i) => ({ week: i + 1, price: 100 + i })) }
  it('moves Sunday marker to today, rebases both curves, and keeps source immutable', () => {
    const original = JSON.stringify({ roadmap, weekly })
    const result = calendarRoadmaps(roadmap, weekly, true, new Date('2026-11-08T07:00:00Z'))
    assert.equal(result.roadmap.asof, '2026-11-08')
    assert.equal(result.weekly.current_week, 45)
    assert.equal(result.roadmap.unsmoothed.full_year.find(p => p.segment === 'today').price, 100)
    assert.equal(result.weekly.weekly_points.find(p => p.segment === 'today').price, 100)
    assert.equal(result.roadmap.calendar_horizons['4w'].direction, 'Bullish')
    assert.equal(result.roadmap.calendar_horizons['12w'].direction, 'Unavailable')
    assert.equal(result.weekly.actual_price.stale.stale, true)
    assert.equal(result.roadmap.price_reference_date, '2026-08-13')
    assert.equal(JSON.stringify({ roadmap, weekly }), original)
  })
  it('preserves integrity gates and handles absent payloads', () => {
    assert.deepEqual(calendarRoadmaps(null, null, true), { roadmap: null, weekly: null })
    const failed = { ...roadmap, available: false }
    assert.equal(calendarRoadmaps(failed, weekly, true).roadmap, failed)
  })
})
