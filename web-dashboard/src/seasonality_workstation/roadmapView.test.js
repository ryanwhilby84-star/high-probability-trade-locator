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

  it('exposes production method label, description, and validated horizons', () => {
    assert.equal(ROADMAP_METHOD_LABEL, 'Seasonal Roadmap')
    assert.equal(
      ROADMAP_METHOD_DESCRIPTION,
      'Robust ISO week-to-week historical returns, compounded and rebased to the current price. No synthetic interpolation or default smoothing.',
    )
    assert.deepEqual(ROADMAP_HORIZON_WEEKS, [4, 8, 12])
  })

  it('classifies horizons from existing stats only', () => {
    assert.equal(
      classifyRoadmapHorizon({
        mean: 0.01,
        median: 0.02,
        bullish_frequency: 0.6,
        bearish_frequency: 0.4,
        n: 15,
      }),
      'Bullish',
    )
    assert.equal(
      classifyRoadmapHorizon({
        mean: -0.01,
        median: -0.02,
        bullish_frequency: 0.3,
        bearish_frequency: 0.7,
        n: 15,
      }),
      'Bearish',
    )
    assert.equal(
      classifyRoadmapHorizon({
        mean: 0.01,
        median: -0.01,
        bullish_frequency: 0.55,
        bearish_frequency: 0.45,
        n: 15,
      }),
      'Mixed',
    )
    assert.equal(
      classifyRoadmapHorizon({
        mean: 0.01,
        median: 0.01,
        bullish_frequency: 0.5,
        bearish_frequency: 0.5,
        n: 15,
      }),
      'Mixed',
    )
  })

  it('robust production payload falls back to the unsmoothed series', () => {
    const roadmap = {
      available: true,
      method: { version: 'robust_weekly_returns_v2' },
      smoothed: null,
      unsmoothed: { full_year: [{ price: 3 }, { price: 4 }] },
    }
    const requestedSmooth = resolveRoadmapSeriesSource(roadmap, true)
    const raw = resolveRoadmapSeriesSource(roadmap, false)
    assert.equal(requestedSmooth.sourcePath, 'payload.seasonal_roadmap.unsmoothed.full_year')
    assert.equal(raw.sourcePath, 'payload.seasonal_roadmap.unsmoothed.full_year')
    assert.equal(requestedSmooth.datasetName, 'robust_weekly_returns_v2')
    assert.equal(raw.datasetName, 'robust_weekly_returns_v2')
  })

  it('retains explicit legacy smooth selection only when a legacy smooth series exists', () => {
    const roadmap = {
      available: true,
      method: { version: 'seasonal_roadmap_v1' },
      smoothed: { full_year: [{ price: 1 }, { price: 2 }] },
      unsmoothed: { full_year: [{ price: 3 }, { price: 4 }] },
    }
    const smooth = resolveRoadmapSeriesSource(roadmap, true)
    const raw = resolveRoadmapSeriesSource(roadmap, false)
    assert.equal(smooth.sourcePath, 'payload.seasonal_roadmap.smoothed.full_year')
    assert.equal(raw.sourcePath, 'payload.seasonal_roadmap.unsmoothed.full_year')
  })
})
