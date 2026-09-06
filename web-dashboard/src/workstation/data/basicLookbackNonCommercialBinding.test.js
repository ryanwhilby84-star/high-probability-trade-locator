/**
 * @vitest-environment node
 */
import { describe, expect, it } from 'vitest'

import { buildBasicLookback } from './buildBasicLookback.js'

function week({ close, cPct, ncPct }) {
  return {
    price: { close },
    commercial: { percentile: cPct },
    nonCommercial: { percentile: ncPct },
  }
}

describe('basic lookback non-commercial binding', () => {
  it('uses nonCommercial percentile when forming opposing-extreme cohorts', () => {
    const dates = [
      '2026-01-06',
      '2026-01-13',
      '2026-01-20',
      '2026-01-27',
      '2026-02-03',
      '2026-02-10',
      '2026-02-17',
      '2026-02-24',
      '2026-03-03',
      '2026-03-10',
      '2026-03-17',
      '2026-03-24',
      '2026-03-31',
      '2026-04-07',
    ]

    const weeklyView = Object.create(null)
    dates.forEach((date, index) => {
      weeklyView[date] = week({
        close: 3 + index * 0.05,
        cPct: index < 2 || index === dates.length - 1 ? 98 : 50,
        ncPct: index === 0 || index === dates.length - 1 ? 2 : index === 1 ? 50 : 50,
      })
    })

    const out = buildBasicLookback({
      weeklyView,
      dates,
      selectedDate: dates[dates.length - 1],
      horizons: [1],
    })

    expect(out.basis).toBe('commercial_plus_noncommercial_extreme_episode')
    expect(out.selectedNcPercentile).toBe(2)
    expect(out.cohortLabel).toContain('Non-Commercial ≤ 5th percentile')
    // Only the first historical week qualifies. The second has Commercial 98th
    // but NC 50th and must not be admitted to the cross-group cohort.
    expect(out.priorMatchCount).toBe(1)
    expect(out.priorEpisodeCount).toBe(1)
  })
})
