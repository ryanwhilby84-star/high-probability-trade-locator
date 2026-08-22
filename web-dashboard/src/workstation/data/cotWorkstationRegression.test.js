/**
 * Regression tests for the COT workstation inspector / rotation-marker failure.
 * @vitest-environment node
 */
import { describe, it, expect } from 'vitest'
import { readFileSync, existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

import {
  DEFAULT_LAYER_STATE,
  eventMatchesLayers,
  toResearchPins,
  classifyResearchLayer,
} from '../researchEventUi.js'
import { buildWeeklyViewModel, resolveInspectedWeek } from './buildWeeklyViewModel.js'
import {
  expandWeeklyInspectorMarket,
  stateLabelFromTemperature,
} from './expandWeeklyInspector.js'

const __dirname = dirname(fileURLToPath(import.meta.url))
const publicData = join(__dirname, '../../../public/data')

function row(date, i = 0) {
  const time = Math.floor(Date.parse(`${date}T12:00:00Z`) / 1000)
  return {
    date,
    label: date,
    time,
    close: 1800 + i,
    commercial_net: 1000 + i * 10,
    commercial_wow: 50,
    institutional_net: -500 - i * 5,
    institutional_wow: -20,
    retail_net: 200,
    retail_wow: 5,
  }
}

function rotationEvent(date, group, label) {
  return {
    date,
    event_type: 'major_rotation',
    group,
    side: `${group}_rotation`,
    label,
    explanation: `${label}: Net positioning remains elevated but has rotated.`,
  }
}

describe('rotation marker persistence', () => {
  it('defaults commercial, NC, and NR rotation layers ON', () => {
    expect(DEFAULT_LAYER_STATE.commercial_rotations).toBe(true)
    expect(DEFAULT_LAYER_STATE.noncommercial_rotations).toBe(true)
    expect(DEFAULT_LAYER_STATE.nr_extremes).toBe(true)
  })

  it('keeps all rotation pins after selecting one marker', () => {
    const dates = ['2018-01-02', '2021-06-15', '2026-07-14']
    const timelineRows = dates.map((d, i) => row(d, i))
    const events = [
      rotationEvent(dates[0], 'commercial', 'Commercial rotation early'),
      rotationEvent(dates[1], 'noncommercial', 'NC rotation mid'),
      rotationEvent(dates[2], 'nonreportable', 'NR rotation late'),
      rotationEvent(dates[2], 'commercial', 'Commercial rotation late'),
    ]
    expect(events.every((e) => eventMatchesLayers(e, DEFAULT_LAYER_STATE))).toBe(true)

    const before = toResearchPins(events, timelineRows, null, null)
    expect(before.length).toBe(4)

    const selected = before[1]
    const after = toResearchPins(
      events,
      timelineRows,
      selected.date,
      selected.eventId,
    )
    expect(after.length).toBe(before.length)
    expect(after.filter((p) => p.selected).length).toBeGreaterThanOrEqual(1)
    expect(after.map((p) => p.eventId).sort()).toEqual(before.map((p) => p.eventId).sort())
  })

  it('layer toggles independently filter rotation categories', () => {
    const c = rotationEvent('2020-01-07', 'commercial', 'C')
    const nc = rotationEvent('2020-01-07', 'noncommercial', 'NC')
    const nr = rotationEvent('2020-01-07', 'nonreportable', 'NR')
    expect(classifyResearchLayer(c)).toBe('commercial_rotations')
    expect(classifyResearchLayer(nc)).toBe('noncommercial_rotations')
    expect(classifyResearchLayer(nr)).toBe('nr_extremes')

    const onlyNc = {
      ...DEFAULT_LAYER_STATE,
      commercial_rotations: false,
      noncommercial_rotations: true,
      nr_extremes: false,
    }
    expect(eventMatchesLayers(c, onlyNc)).toBe(false)
    expect(eventMatchesLayers(nc, onlyNc)).toBe(true)
    expect(eventMatchesLayers(nr, onlyNc)).toBe(false)
  })
})

describe('inspector selection persistence', () => {
  it('keeps selected week after weekly_inspector merge (percentile fetch)', () => {
    const timelineRows = [row('2026-07-14', 0), row('2026-07-21', 1)]
    const markers = [
      rotationEvent('2026-07-14', 'noncommercial', 'NC rotation'),
    ]
    const researchOnly = {
      available: true,
      markers,
      weekly_inspector: null,
    }
    const withInspector = {
      available: true,
      markers,
      weekly_inspector: {
        available: true,
        weeks: [
          {
            date: '2026-07-14',
            commercial: {
              net: 1000,
              percentile: 68,
              percentile_change_1w: 2,
              percentile_change_4w: 11.4,
              percentile_observation_count: 400,
              direction_arrow: '▲',
              temperature: 'heating',
              state_label: 'Deeper into extreme',
              is_extreme: false,
            },
            noncommercial: {
              net: -900,
              percentile: 9,
              percentile_change_4w: 14,
              temperature: 'recovering_strong',
              state_label: 'Strong rotation away from extreme',
              is_extreme: true,
            },
            nonreportable: { net: 200, percentile: 40 },
            cross: {
              commercial_percentile: 68,
              noncommercial_percentile: 9,
              nonreportable_percentile: 40,
            },
          },
        ],
      },
    }

    const selectedWeek = '2026-07-14'
    const before = buildWeeklyViewModel({
      timelineRows,
      researchBlock: researchOnly,
      instrument: 'Gold',
    })
    const after = buildWeeklyViewModel({
      timelineRows,
      researchBlock: withInspector,
      instrument: 'Gold',
    })

    const inspectedBefore = resolveInspectedWeek({
      weeklyView: before.weeklyView,
      selectedWeek,
      hoveredWeek: null,
      latestDate: null,
    })
    const inspectedAfter = resolveInspectedWeek({
      weeklyView: after.weeklyView,
      selectedWeek,
      hoveredWeek: '2026-07-21',
      latestDate: '2026-07-21',
    })

    expect(inspectedBefore.date).toBe(selectedWeek)
    expect(inspectedAfter.date).toBe(selectedWeek)
    expect(after.weeklyView[selectedWeek].events).toHaveLength(1)
    expect(after.weeklyView[selectedWeek].nonCommercial.percentile).toBe(9)
    expect(after.weeklyView[selectedWeek].commercial.stateLabel).toBe(
      'Deeper into extreme',
    )
  })

  it('does not clear selection semantics on marker regeneration', () => {
    const timelineRows = [row('2021-06-15')]
    const events = [rotationEvent('2021-06-15', 'commercial', 'C rot')]
    const pins1 = toResearchPins(events, timelineRows, '2021-06-15', null)
    // Simulate price refresh / pin rebuild with same selection.
    const pins2 = toResearchPins(events, [...timelineRows], '2021-06-15', pins1[0].eventId)
    expect(pins2.length).toBe(pins1.length)
    expect(pins2[0].selected).toBe(true)
  })
})

describe('percentiles', () => {
  it('restores interpretation wording from temperature tokens', () => {
    expect(stateLabelFromTemperature('heating_rapidly')).toBe('Deeper into extreme')
    expect(stateLabelFromTemperature('cooling_from_extreme')).toBe('Cooling from extreme')
    expect(stateLabelFromTemperature('deepening_extreme')).toBe('Deeper into low extreme')
    expect(stateLabelFromTemperature('recovering_strong')).toBe(
      'Strong rotation away from extreme',
    )
    expect(stateLabelFromTemperature('building')).toBe('Rotation strengthening')
    expect(stateLabelFromTemperature('weakening')).toBe('Rotation weakening')
  })

  it('expands compact weekly inspector with non-null percentiles when net exists', () => {
    const path = join(publicData, 'cot_weekly_inspector_latest.json')
    if (!existsSync(path)) return
    const doc = JSON.parse(readFileSync(path, 'utf8'))
    const compact = doc.markets?.Gold
    expect(compact).toBeTruthy()
    const expanded = expandWeeklyInspectorMarket(compact)
    expect(expanded.weeks.length).toBeGreaterThan(100)

    let missing = 0
    let checked = 0
    for (const w of expanded.weeks) {
      for (const key of ['commercial', 'noncommercial', 'nonreportable']) {
        const g = w[key]
        if (!g || g.net == null || !Number.isFinite(Number(g.net))) continue
        checked += 1
        if (g.percentile == null || !Number.isFinite(Number(g.percentile))) {
          missing += 1
          continue
        }
        expect(g.percentile).toBeGreaterThanOrEqual(0)
        expect(g.percentile).toBeLessThanOrEqual(100)
      }
    }
    expect(checked).toBeGreaterThan(0)
    expect(missing).toBe(0)
  })

  it('Natural Gas inspector tip matches cot_3y tip and 1W change invariant', () => {
    const wiPath = join(publicData, 'cot_weekly_inspector_latest.json')
    const s3Path = join(publicData, 'cot_3y_series_latest.json')
    if (!existsSync(wiPath) || !existsSync(s3Path)) return
    const wi = JSON.parse(readFileSync(wiPath, 'utf8'))
    const s3 = JSON.parse(readFileSync(s3Path, 'utf8'))
    const rows = wi.markets?.['Natural Gas / NG']?.rows || []
    const series = s3.markets?.['Natural Gas / NG']?.series || []
    expect(rows.length).toBeGreaterThan(2)
    expect(series.length).toBeGreaterThan(2)
    const last = rows[rows.length - 1]
    const prev = rows[rows.length - 2]
    const tip = series[series.length - 1]
    expect(last[0]).toBe(tip.date)
    expect(last[2][0]).toBe(tip.institutional_net)
    // current_net - previous_net == displayed_1w_change for C / NC / NR
    for (const gi of [1, 2, 3]) {
      expect(last[gi][0] - prev[gi][0]).toBeCloseTo(last[gi][1], 6)
    }
    // Final NC chart segment direction agrees with NC 1W sign
    const ncDelta = tip.institutional_net - series[series.length - 2].institutional_net
    expect(Math.sign(ncDelta)).toBe(Math.sign(last[2][1]) || 0)
  })
})

describe('layout clipping guards', () => {
  it('drawer CSS no longer uses a fixed 148px clip height', () => {
    const cssPath = join(__dirname, '../cotWorkstation.css')
    const css = readFileSync(cssPath, 'utf8')
    expect(css).toContain('.cot-ws-weekly-inspector--drawer')
    expect(css).not.toMatch(
      /\.cot-ws-weekly-inspector--drawer\s*\{[^}]*height:\s*148px/s,
    )
    expect(css).toMatch(
      /\.cot-ws-insp-metric-value\s*\{[^}]*overflow:\s*visible/s,
    )
  })
})

describe('historical rotation clicks across time', () => {
  it('resolves early / mid / late rotation weeks with narrative + percentile', () => {
    const path = join(publicData, 'cot_positioning_research_latest.json')
    const wiPath = join(publicData, 'cot_weekly_inspector_latest.json')
    if (!existsSync(path) || !existsSync(wiPath)) return

    const research = JSON.parse(readFileSync(path, 'utf8')).markets.Gold
    const inspector = expandWeeklyInspectorMarket(
      JSON.parse(readFileSync(wiPath, 'utf8')).markets.Gold,
    )
    const rotations = (research.markers || []).filter(
      (e) => e.event_type === 'major_rotation' || e.event_type === 'rapid_velocity',
    )
    expect(rotations.length).toBeGreaterThan(10)

    const sorted = [...rotations].sort((a, b) => String(a.date).localeCompare(String(b.date)))
    const picks = [sorted[0], sorted[Math.floor(sorted.length / 2)], sorted[sorted.length - 1]]
    const byDate = new Map(inspector.weeks.map((w) => [w.date, w]))

    for (const ev of picks) {
      const week = byDate.get(String(ev.date).slice(0, 10))
      expect(week, `missing inspector week ${ev.date}`).toBeTruthy()
      const groupKey =
        ev.group === 'noncommercial'
          ? 'noncommercial'
          : ev.group === 'nonreportable'
            ? 'nonreportable'
            : 'commercial'
      const pack = week[groupKey]
      expect(pack?.net == null || pack.percentile != null).toBe(true)
      expect(ev.label || ev.explanation || ev.event_type).toBeTruthy()
    }
  })
})
