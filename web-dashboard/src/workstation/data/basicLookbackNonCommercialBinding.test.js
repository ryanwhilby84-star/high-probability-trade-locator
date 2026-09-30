/**
 * @vitest-environment node
 */
import { describe, expect, it } from 'vitest'

import { buildBasicLookback } from './buildBasicLookback.js'

function week({ close, cPct, ncPct, cMove = 0, ncMove = 0 }) {
  return {
    price: { close, high: close * 1.02, low: close * 0.98 },
    commercial: { percentile: cPct, percentileChange1w: cMove },
    nonCommercial: { percentile: ncPct, percentileChange1w: ncMove },
  }
}

describe('basic lookback adaptive selected-week cohort', () => {
  it('uses both Commercial and Non-Commercial positioning instead of silently matching Commercial only', () => {
    const dates = [
      '2026-01-06','2026-01-13','2026-01-20','2026-01-27','2026-02-03','2026-02-10',
      '2026-02-17','2026-02-24','2026-03-03','2026-03-10','2026-03-17','2026-03-24',
      '2026-03-31','2026-04-07',
    ]
    const weeklyView = Object.create(null)
    dates.forEach((date, index) => {
      weeklyView[date] = week({ close: 3 + index * 0.05, cPct: 50, ncPct: 50 })
    })
    weeklyView[dates[0]] = week({ close: 3, cPct: 98, ncPct: 2 })
    weeklyView[dates[1]] = week({ close: 3.05, cPct: 98, ncPct: 50 })
    weeklyView[dates[dates.length - 1]] = week({ close: 3.65, cPct: 98, ncPct: 2 })

    const out = buildBasicLookback({ weeklyView, dates, selectedDate: dates[dates.length - 1], horizons: [1] })

    expect(out.basis).toBe('commercial_plus_noncommercial_adaptive_episode')
    expect(out.selectedNcPercentile).toBe(2)
    expect(out.cohortLabel).toContain('Non-Commercial')
    expect(out.priorMatchCount).toBe(1)
    expect(out.priorEpisodeCount).toBe(1)
  })

  it('centres the cohort on the selected week rather than reusing a fixed percentile bucket', () => {
    const dates = [
      '2025-01-07','2025-01-14','2025-01-21','2025-01-28','2025-02-04','2025-02-11',
      '2025-02-18','2025-02-25','2025-03-04','2025-03-11','2025-03-18','2025-03-25',
      '2025-04-01','2025-04-08','2025-04-15','2025-04-22',
    ]
    const weeklyView = Object.create(null)
    dates.forEach((date, index) => {
      weeklyView[date] = week({ close: 3 + index * 0.03, cPct: 50, ncPct: 50 })
    })
    weeklyView[dates[0]] = week({ close: 3.00, cPct: 94, ncPct: 8 })
    weeklyView[dates[2]] = week({ close: 3.06, cPct: 99, ncPct: 1 })
    weeklyView[dates[14]] = week({ close: 3.42, cPct: 94, ncPct: 8 })
    weeklyView[dates[15]] = week({ close: 3.45, cPct: 99, ncPct: 1 })

    const a = buildBasicLookback({ weeklyView, dates, selectedDate: dates[14], horizons: [1] })
    const b = buildBasicLookback({ weeklyView, dates, selectedDate: dates[15], horizons: [1] })

    expect(a.cohortLabel).not.toBe(b.cohortLabel)
    expect(a.selectedPercentile).toBe(94)
    expect(b.selectedPercentile).toBe(99)
    expect(a.selectedNcPercentile).toBe(8)
    expect(b.selectedNcPercentile).toBe(1)
  })
})
