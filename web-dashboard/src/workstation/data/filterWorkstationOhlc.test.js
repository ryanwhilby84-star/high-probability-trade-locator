import { describe, expect, it } from 'vitest'

import {
  filterCompletedWorkstationOhlc,
  matchOhlcBarForCotWeek,
} from './filterWorkstationOhlc.js'

const bars = [
  { date: '2026-05-31', open: 1, high: 2, low: 0.5, close: 1.5, time: 1 },
  { date: '2026-06-07', open: 1.4, high: 2.1, low: 1.2, close: 1.8, time: 2 },
  { date: '2026-06-09', open: 1.3, high: 1.5, low: 1.1, close: 1.4, time: 3 },
]

describe('filterCompletedWorkstationOhlc', () => {
  it('rejects partial current ISO week and bars after COT last', () => {
    const asOf = new Date('2026-06-12T12:00:00Z')
    const { bars: kept, rejected } = filterCompletedWorkstationOhlc(bars, {
      cotLastDate: '2026-06-07',
      asOf,
    })
    expect(kept.map((b) => b.date)).toEqual(['2026-05-31', '2026-06-07'])
    expect(rejected.some((r) => r.reason === 'incomplete_iso_week')).toBe(true)
    expect(rejected.some((r) => r.reason === 'after_cot_last')).toBe(false)
  })
})

describe('matchOhlcBarForCotWeek', () => {
  it('does not reuse the same OHLC bar for consecutive COT weeks', () => {
    const filtered = filterCompletedWorkstationOhlc(bars, {
      cotLastDate: '2026-06-16',
      asOf: new Date('2026-06-20T12:00:00Z'),
    }).bars

    const first = matchOhlcBarForCotWeek('2026-06-09', filtered, null)
    const second = matchOhlcBarForCotWeek('2026-06-16', filtered, first?.date ?? null)

    expect(first?.date).toBe('2026-06-07')
    expect(second).toBeNull()
  })
})
