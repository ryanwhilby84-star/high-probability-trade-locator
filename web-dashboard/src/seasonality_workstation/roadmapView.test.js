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

  it('exposes daily production method and validated horizons', () => {
    assert.equal(ROADMAP_METHOD_LABEL, 'Seasonal Roadmap')
    assert.equal(
      ROADMAP_METHOD_DESCRIPTION,
      'Robust daily close-to-close historical returns, compounded trading-day by trading-day and rebased to the current price. No synthetic interpolation or default smoothing.',
    )
    assert.deepEqual(ROADMAP_HORIZON_WEEKS, [4, 8, 12])
  })

  it('classifies horizons from stats only', () => {
    assert.equal(classifyRoadmapHorizon({ mean: 0.01, median: 0.02, bullish_frequency: 0.6, bearish_frequency: 0.4, n: 15 }), 'Bullish')
    assert.equal(classifyRoadmapHorizon({ mean: -0.01, median: -0.02, bullish_frequency: 0.3, bearish_frequency: 0.7, n: 15 }), 'Bearish')
    assert.equal(classifyRoadmapHorizon({ mean: 0.01, median: -0.01, bullish_frequency: 0.55, bearish_frequency: 0.45, n: 15 }), 'Mixed')
  })

  it('daily production payload always resolves to unsmoothed observations', () => {
    const roadmap = {
      available: true,
      method: { version: 'robust_daily_returns_v3' },
      smoothed: null,
      unsmoothed: { full_year: [{ price: 3 }, { price: 4 }] },
    }
    const requestedSmooth = resolveRoadmapSeriesSource(roadmap, true)
    const raw = resolveRoadmapSeriesSource(roadmap, false)
    assert.equal(requestedSmooth.sourcePath, 'payload.seasonal_roadmap.unsmoothed.full_year')
    assert.equal(raw.sourcePath, 'payload.seasonal_roadmap.unsmoothed.full_year')
    assert.equal(requestedSmooth.datasetName, 'robust_daily_returns_v3')
    assert.equal(raw.datasetName, 'robust_daily_returns_v3')
  })
})
