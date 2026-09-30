/**
 * @vitest-environment node
 */
import { describe, expect, it } from 'vitest'
import { mergeWeeklyHistory } from './WeeklyOHLCStore.js'

describe('mergeWeeklyHistory', () => {
  it('keeps older legacy weeks when the canonical feed is shorter', () => {
    const legacy = [
      { date: '2021-01-08', open: 1000, high: 1020, low: 990, close: 1010 },
      { date: '2024-01-05', open: 1200, high: 1220, low: 1190, close: 1210 },
      { date: '2026-08-28', open: 1300, high: 1320, low: 1290, close: 1310 },
    ]
    const canonical = [
      { date: '2026-08-28', open: 1305, high: 1330, low: 1300, close: 1325 },
      { date: '2026-09-04', open: 1325, high: 1340, low: 1310, close: 1335 },
    ]

    const merged = mergeWeeklyHistory(legacy, canonical)

    expect(merged.map((bar) => bar.date)).toEqual([
      '2021-01-08',
      '2024-01-05',
      '2026-08-28',
      '2026-09-04',
    ])
    expect(merged.find((bar) => bar.date === '2026-08-28')?.close).toBe(1325)
  })
})
