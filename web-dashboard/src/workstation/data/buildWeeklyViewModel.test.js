/**
 * @vitest-environment node
 */
import { describe, it, expect } from 'vitest'
import {
  buildWeekSummaryText,
  buildWeeklyViewModel,
  missingRequiredInspectorFields,
  researchEventId,
  resolveInspectedWeek,
  resolveInspectorWeekForDate,
} from './buildWeeklyViewModel.js'
import { toResearchPins, eventTone } from '../researchEventUi.js'

function inspectorWeek(date, overrides = {}) {
  const base = {
    date,
    commercial: {
      net: 1000,
      weekly_change: 10,
      four_week_change: 40,
      twelve_week_change: 120,
      percentile: 80,
      percentile_change_1w: 1,
      percentile_change_4w: 4,
      percentile_change_12w: 12,
      percentile_observation_count: 200,
      temperature: 'elevated_stable',
      state_label: 'Elevated / stable',
      direction: 'increasing',
      direction_arrow: '▲',
    },
    noncommercial: {
      net: -500,
      weekly_change: -10,
      four_week_change: -40,
      twelve_week_change: -120,
      percentile: 20,
      percentile_change_1w: -1,
      percentile_change_4w: -4,
      percentile_change_12w: -12,
      percentile_observation_count: 200,
      temperature: 'depressed_stable',
      state_label: 'Depressed / stable',
      direction: 'decreasing',
      direction_arrow: '▼',
    },
    nonreportable: {
      net: 200,
      weekly_change: 5,
      four_week_change: 20,
      twelve_week_change: 60,
      percentile: 40,
      percentile_change_1w: 0.5,
      percentile_change_4w: 2,
      percentile_change_12w: 6,
      percentile_observation_count: 200,
      temperature: 'neutral',
      state_label: 'Neutral',
      direction: 'stable',
      direction_arrow: '→',
    },
    cross: {
      commercial_percentile: 80,
      noncommercial_percentile: 20,
      nonreportable_percentile: 40,
      comm_nc_spread: 60,
      comm_nc_spread_percentile: 50,
      comm_nc_spread_change_1w: 2,
      comm_nc_spread_change_4w: 8,
      comm_nr_spread: 40,
      comm_nr_spread_percentile: 50,
      relationship: 'opposed',
      flow: 'opposition_widening',
    },
    summaries: {},
  }
  return { ...base, ...overrides, date }
}

function row(date, i, nets = {}) {
  const time = Math.floor(Date.parse(`${date}T12:00:00Z`) / 1000)
  return {
    date,
    label: date,
    time,
    close: 1 + i * 0.01,
    price: 1 + i * 0.01,
    commercial_net: nets.c ?? 1000 + i * 100,
    commercial_wow: nets.cw ?? 100,
    institutional_net: nets.nc ?? -500 - i * 50,
    institutional_wow: nets.ncw ?? -50,
    retail_net: nets.nr ?? 200 + i * 10,
    retail_wow: nets.nrw ?? 10,
  }
}

function priceOnlyRow(date, i) {
  const r = row(date, i)
  return {
    ...r,
    commercial_net: null,
    commercial_wow: null,
    institutional_net: null,
    institutional_wow: null,
    retail_net: null,
    retail_wow: null,
  }
}

describe('buildWeeklyViewModel', () => {
  it('builds a week for every timeline date, including weeks without events', () => {
    const timelineRows = [
      row('2026-07-07', 0),
      row('2026-07-14', 1),
      row('2026-07-21', 2),
    ]
    const { weeklyView, latestDate } = buildWeeklyViewModel({
      timelineRows,
      researchBlock: {
        source_week: '2026-07-21',
        markers: [],
        spread_series: [{ date: '2026-07-21', spread: 12, spread_percentile: 80 }],
        current_state: {
          commercial: {
            date: '2026-07-21',
            net: 1200,
            percentiles: { long_history: 87 },
            velocity: { '1w': { net_change: 8420, percentile_change: 2 } },
          },
          noncommercial: {
            date: '2026-07-21',
            net: -600,
            percentiles: { long_history: 19 },
            velocity: { '1w': { net_change: -6110, percentile_change: -3 } },
          },
          nonreportable: {
            date: '2026-07-21',
            net: 200,
            percentiles: { long_history: 40 },
          },
          spread: { date: '2026-07-21', spread: 12, spread_percentile: 80 },
        },
      },
      instrument: 'NZ Dollar / 6N',
      loadedLatestDate: '2026-07-21',
    })

    expect(latestDate).toBe('2026-07-21')
    expect(weeklyView['2026-07-14']).toBeTruthy()
    expect(weeklyView['2026-07-14'].events).toEqual([])
    expect(weeklyView['2026-07-14'].commercial.net).toBeTruthy()
    expect(weeklyView['2026-07-21'].freshness).toBe('latest')
    expect(weeklyView['2026-07-21'].commercial.percentile).toBeTruthy()
    expect(weeklyView['2026-07-21'].summary).toMatch(/Commercial positioning/)
  })

  it('lists multiple events on the same week', () => {
    const timelineRows = [row('2017-12-26', 0)]
    const markers = [
      {
        date: '2017-12-26',
        event_type: 'major_rotation',
        group: 'commercial',
        side: 'commercial_rotation',
        label: 'Commercial rotation',
        commercial: { net: 1, long_history_percentile: 90 },
      },
      {
        date: '2017-12-26',
        event_type: 'major_rotation',
        group: 'noncommercial',
        side: 'noncommercial_rotation',
        label: 'NC rotation',
        noncommercial: { net: -1, long_history_percentile: 10 },
      },
    ]
    const { weeklyView } = buildWeeklyViewModel({
      timelineRows,
      researchBlock: { markers, spread_series: [] },
      instrument: 'Natural Gas / NG',
    })
    expect(weeklyView['2017-12-26'].events).toHaveLength(2)
    expect(weeklyView['2017-12-26'].commercial.rotationState).toBeTruthy()
    expect(weeklyView['2017-12-26'].nonCommercial.rotationState).toBeTruthy()
  })

  it('keeps selectedWeek over hoveredWeek when resolving inspector week', () => {
    const weeklyView = {
      '2026-01-01': { date: '2026-01-01' },
      '2026-02-01': { date: '2026-02-01' },
      '2026-03-01': { date: '2026-03-01' },
    }
    const week = resolveInspectedWeek({
      weeklyView,
      selectedWeek: '2026-01-01',
      hoveredWeek: '2026-02-01',
      latestDate: '2026-03-01',
    })
    expect(week.date).toBe('2026-01-01')
  })

  it('builds a deterministic summary without an LLM', () => {
    const text = buildWeekSummaryText({
      commercial: {
        change1w: 8420,
        percentile: 87,
        extremeState: 'Commercial extreme',
        direction: 'rising',
      },
      nonCommercial: {
        change1w: -6110,
        percentile: 19,
        direction: 'falling',
      },
      nonReportable: {},
      spreads: { commNcAlignment: 'opposed' },
      events: [{ event_type: 'comm_nr_divergence', label: 'Divergence' }],
    })
    expect(text).toContain('rose by 8,420 contracts')
    expect(text).toContain('87th net percentile')
    expect(text).toContain('fell by 6,110 contracts')
    expect(text).toContain('19th net percentile')
    expect(text).toContain('divergence')
  })

  it('as-of joins Friday price weeks to Tuesday COT inspector weeks', () => {
    const map = new Map([
      ['2026-07-07', inspectorWeek('2026-07-07')],
      ['2026-07-14', inspectorWeek('2026-07-14', {
        commercial: { ...inspectorWeek('2026-07-14').commercial, percentile: 93.75 },
      })],
      ['2026-07-21', inspectorWeek('2026-07-21', {
        commercial: { ...inspectorWeek('2026-07-21').commercial, percentile: 91.63 },
      })],
    ])
    const fri = resolveInspectorWeekForDate(map, '2026-07-17')
    expect(fri.exact).toBe(false)
    expect(fri.asOfDate).toBe('2026-07-14')
    expect(fri.week.commercial.percentile).toBe(93.75)
    const tue = resolveInspectorWeekForDate(map, '2026-07-21')
    expect(tue.exact).toBe(true)
    expect(tue.week.commercial.percentile).toBe(91.63)
  })

  it('marks incomplete derived fields as integrity failure', () => {
    const timelineRows = [row('2026-07-14', 0)]
    const { weeklyView } = buildWeeklyViewModel({
      timelineRows,
      researchBlock: {
        source_week: '2026-07-14',
        markers: [],
        weekly_inspector: {
          available: true,
          weeks: [{
            date: '2026-07-14',
            commercial: { net: 1, weekly_change: 1 },
            noncommercial: { net: 1, weekly_change: 1 },
            nonreportable: { net: 1, weekly_change: 1 },
            cross: {},
          }],
        },
      },
      instrument: 'Crude Oil / CL',
      loadedLatestDate: '2026-07-14',
    })
    const week = weeklyView['2026-07-14']
    expect(week.integrityOk).toBe(false)
    expect(week.integrityMissing.length).toBeGreaterThan(0)
    expect(missingRequiredInspectorFields(week)).toEqual(week.integrityMissing)
  })

  it('keeps price-only rows from becoming duplicate COT observations', () => {
    const timelineRows = [
      row('2026-08-25', 0, { c: 1000, nc: -500, nr: 200 }),
      priceOnlyRow('2026-08-28', 1),
      row('2026-09-01', 2, { c: 1100, nc: -550, nr: 220 }),
    ]
    const { weeklyView } = buildWeeklyViewModel({
      timelineRows,
      researchBlock: {
        markers: [],
        weekly_inspector: {
          available: true,
          weeks: [inspectorWeek('2026-08-25')],
        },
      },
      instrument: 'Crude Oil / CL',
      loadedLatestDate: '2026-09-01',
    })

    const priceWeek = weeklyView['2026-08-28']
    expect(priceWeek.isCotReport).toBe(false)
    expect(priceWeek.commercial.net).toBe(null)
    expect(priceWeek.commercial.percentile).toBe(null)
    expect(priceWeek.nonCommercial.net).toBe(null)
    expect(priceWeek.nonReportable.net).toBe(null)
  })

  it('computes 1W/4W/12W changes across genuine reports even with price rows inserted', () => {
    const timelineRows = []
    const inspectorWeeks = []
    const start = Date.parse('2026-06-02T12:00:00Z')

    for (let i = 0; i < 14; i += 1) {
      const reportDate = new Date(start + i * 7 * 86400000).toISOString().slice(0, 10)
      timelineRows.push(row(reportDate, i, {
        c: 1000 + i * 100,
        nc: -500 - i * 50,
        nr: 200 + i * 10,
        cw: 100,
        ncw: -50,
        nrw: 10,
      }))
      // Insert a price-only row between reports.
      if (i < 13) {
        const priceDate = new Date(start + i * 7 * 86400000 + 3 * 86400000)
          .toISOString()
          .slice(0, 10)
        timelineRows.push(priceOnlyRow(priceDate, i))
      }
      // Deliberately leave the latest report out of the inspector export.
      if (i < 13) inspectorWeeks.push(inspectorWeek(reportDate))
    }

    const latestDate = timelineRows.filter((r) => r.commercial_net != null).at(-1).date
    const { weeklyView } = buildWeeklyViewModel({
      timelineRows,
      researchBlock: {
        markers: [],
        weekly_inspector: { available: true, weeks: inspectorWeeks },
      },
      instrument: 'Crude Oil / CL',
      loadedLatestDate: latestDate,
    })

    const latest = weeklyView[latestDate]
    expect(latest.isCotReport).toBe(true)
    expect(latest.inspectorExact).toBe(false)
    expect(latest.commercial.change1w).toBe(100)
    expect(latest.commercial.change4w).toBe(400)
    expect(latest.commercial.change12w).toBe(1200)
    expect(latest.nonCommercial.change12w).toBe(-600)
    expect(latest.nonReportable.change12w).toBe(120)
    expect(latest.commercial.percentile).not.toBe(null)
    expect(latest.commercial.percentileChange1w).not.toBe(null)
    expect(latest.commercial.percentileChange12w).not.toBe(null)
    expect(latest.commercial.percentileSource).toBe('timeline_cot_rolling_156')
    expect(latest.integrityMissing).not.toContain('commercial.change12w')
    expect(latest.integrityMissing).not.toContain('non_commercial.change12w')
    expect(latest.integrityMissing).not.toContain('non_reportable.change12w')
  })
})

describe('toResearchPins', () => {
  it('maps rotations to compact circle markers and stacks multi-events', () => {
    const timelineRows = [row('2017-12-26', 0)]
    const events = [
      {
        date: '2017-12-26',
        event_type: 'absolute_extreme',
        group: 'commercial',
        side: 'bullish',
        label: 'EX',
      },
      {
        date: '2017-12-26',
        event_type: 'major_rotation',
        group: 'commercial',
        side: 'commercial_rotation',
        label: 'ROT',
      },
    ]
    const pins = toResearchPins(events, timelineRows, '2017-12-26')
    expect(pins.length).toBe(2)
    expect(pins.some((p) => p.shape === 'circle')).toBe(true)
    expect(pins.some((p) => p.shape === 'diamond')).toBe(true)
    expect(eventTone(events[1])).toBe('rotation')
    expect(researchEventId(events[0])).toContain('2017-12-26')
  })
})
