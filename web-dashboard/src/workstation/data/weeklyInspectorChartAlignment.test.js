/**
 * @vitest-environment node
 */
import { describe, expect, it } from 'vitest'

import { buildCotWorkstation } from '../../cot/buildCotWorkstation.js'
import { expandWeeklyInspectorMarket } from './expandWeeklyInspector.js'
import { buildWeeklyViewModel } from './buildWeeklyViewModel.js'

const t = (date) => Math.floor(Date.parse(`${date}T12:00:00Z`) / 1000)

function compactGroup({ net, w1, pct, pct1 = 0, pct4 = 0, pct12 = 0 }) {
  // [net, 1w, 4w, 12w, pct, pctΔ1w, pctΔ4w, pctΔ12w, obs, dir, temp, extreme]
  return [net, w1, w1, w1, pct, pct1, pct4, pct12, 504, 2, 9, false]
}

describe('weekly inspector chart alignment', () => {
  it('uses the exact plotted net series for contract values and 1W deltas', () => {
    const model = buildCotWorkstation({
      market: 'Copper / HG',
      series: [
        {
          date: '2026-08-25',
          commercial_net: -97991,
          institutional_net: 89663,
          retail_net: 11000,
        },
        {
          date: '2026-09-01',
          commercial_net: -95049,
          institutional_net: 85266,
          retail_net: 11238,
          // Deliberately wrong exported delta: this used to override the actual
          // line-series difference and reproduce the inspector/chart mismatch.
          one_week_net_change: 5518,
        },
      ],
    })

    expect(model.series[1].commercial_wow).toBe(2942)
    expect(model.series[1].institutional_wow).toBe(-4397)

    const timelineRows = model.series.map((row) => ({
      ...row,
      time: t(row.date),
      close: row.date === '2026-09-01' ? 6.5065 : 6.45,
    }))

    const weeklyInspector = expandWeeklyInspectorMarket({
      available: true,
      rows: [
        [
          '2026-09-01',
          compactGroup({ net: -95049, w1: -4909, pct: 0, pct1: -0.2, pct4: -5.2 }),
          compactGroup({ net: 85266, w1: 5518, pct: 100, pct1: 0.6, pct4: 4.6 }),
          compactGroup({ net: 9783, w1: -609, pct: 85, pct1: -3, pct4: 4.3 }),
          [0, 100, 85, -100, 0, 0, 0, -100, 0, 1, 6],
        ],
      ],
    })

    // The compact inspector payload may contain separately generated contract
    // values, but it is metadata-only for percentiles/flow in the UI.
    expect(weeklyInspector.weeks[0].commercial.weekly_change).toBeNull()
    expect(weeklyInspector.weeks[0].noncommercial.weekly_change).toBeNull()

    const { weeklyView } = buildWeeklyViewModel({
      timelineRows,
      researchBlock: { weekly_inspector: weeklyInspector, markers: [] },
      instrument: 'Copper / HG',
      loadedLatestDate: '2026-09-01',
    })

    const week = weeklyView['2026-09-01']
    expect(week.commercial.net).toBe(-95049)
    expect(week.commercial.change1w).toBe(2942)
    expect(week.nonCommercial.net).toBe(85266)
    expect(week.nonCommercial.change1w).toBe(-4397)

    // Percentile intelligence still comes from the dedicated inspector export.
    expect(week.commercial.percentile).toBe(0)
    expect(week.nonCommercial.percentile).toBe(100)
  })
})
