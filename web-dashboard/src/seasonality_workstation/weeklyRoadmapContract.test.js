import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import {
  SWS_EQUAL_CHART_HEIGHT,
  WEEKLY_ROADMAP_CANONICAL_KEY,
  assertEqualChartPanelCount,
  resolveWeeklyRoadmap,
  weeklyRoadmapRenderState,
} from './weeklyRoadmapContract.js'

function samplePoints(n = 52) {
  return Array.from({ length: n }, (_, i) => ({
    week: i + 1,
    average_return: 0.001,
    cumulative_return: 0.001 * (i + 1),
    sample_count: 8,
    quality_flag: 'ok',
    price: 100 + i,
    segment: i < 30 ? 'historical' : i === 30 ? 'today' : 'forward',
  }))
}

describe('Weekly Roadmap frontend contract', () => {
  it('uses canonical snake_case key weekly_roadmap', () => {
    assert.equal(WEEKLY_ROADMAP_CANONICAL_KEY, 'weekly_roadmap')
    const payload = {
      weekly_roadmap: { available: true, weekly_points: samplePoints(), quality_status: 'valid' },
    }
    assert.equal(resolveWeeklyRoadmap(payload), payload.weekly_roadmap)
  })

  it('falls back to seasonality.weekly_roadmap only', () => {
    const nested = { available: true, weekly_points: samplePoints(), quality_status: 'valid' }
    assert.equal(resolveWeeklyRoadmap({ seasonality: { weekly_roadmap: nested } }), nested)
    assert.equal(resolveWeeklyRoadmap({ weeklyRoadmap: nested }), null)
  })

  it('warning status still renders chart', () => {
    const state = weeklyRoadmapRenderState({
      available: true,
      quality_status: 'warning',
      quality_reasons: ['integrity_warning:thin_years:[2016]'],
      weekly_points: samplePoints(),
      current_week: 30,
    })
    assert.equal(state.mode, 'ready')
    assert.equal(state.points.length, 52)
    assert.ok(state.reasons.length >= 1)
  })

  it('unavailable status renders failure message', () => {
    const state = weeklyRoadmapRenderState({
      available: false,
      quality_status: 'unavailable',
      quality_reasons: ['excessive_discontinuities:223'],
      weekly_points: [],
    })
    assert.equal(state.mode, 'unavailable')
    assert.match(state.message, /unavailable/i)
  })

  it('missing payload renders clear error', () => {
    const state = weeklyRoadmapRenderState(null)
    assert.equal(state.mode, 'missing')
    assert.match(state.message, /missing from API payload/i)
  })

  it('three equal chart panels share height constant', () => {
    assert.equal(SWS_EQUAL_CHART_HEIGHT, 280)
    assert.equal(
      assertEqualChartPanelCount([
        SWS_EQUAL_CHART_HEIGHT,
        SWS_EQUAL_CHART_HEIGHT,
        SWS_EQUAL_CHART_HEIGHT,
      ]),
      true,
    )
    assert.equal(assertEqualChartPanelCount([280, 260, 280]), false)
  })

  it('current-week marker field is present on ready payload', () => {
    const wr = {
      available: true,
      quality_status: 'valid',
      weekly_points: samplePoints(),
      current_week: 30,
    }
    const state = weeklyRoadmapRenderState(wr)
    assert.equal(state.mode, 'ready')
    assert.equal(wr.current_week, 30)
  })
})
