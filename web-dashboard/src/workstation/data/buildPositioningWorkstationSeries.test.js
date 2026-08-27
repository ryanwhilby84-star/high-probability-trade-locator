/**
 * @vitest-environment node
 */
import { describe, it, expect } from 'vitest'
import { buildCotWorkstation } from '../../cot/buildCotWorkstation.js'
import {
  buildPositioningWorkstationSeries,
  shouldUseStoreOhlc,
} from './buildPositioningWorkstationSeries.js'

describe('buildPositioningWorkstationSeries', () => {
  it('keeps one timeline row per COT week when OHLC is absent', () => {
    const cotSeries = [
      { date: '2024-01-02', price: 2000, institutional_net: 1, retail_net: 2, commercial_net: 3 },
      { date: '2024-01-09', price: 2010, institutional_net: 4, retail_net: 5, commercial_net: 6 },
    ]
    const model = buildCotWorkstation({
      market: 'Test',
      series: cotSeries,
      weeks: 2,
      has_price: true,
      has_commercial: true,
      has_retail: true,
    })
    const bound = buildPositioningWorkstationSeries(model, null)
    expect(bound.rows).toHaveLength(2)
    expect(bound.rows[0].commercial_net).toBe(3)
    expect(bound.weeklyBars).toHaveLength(0)
  })

  it('continues price candles past the latest COT report', () => {
    const cotSeries = [
      {
        date: '2026-07-07',
        price: 3,
        institutional_net: 1,
        retail_net: 2,
        commercial_net: 3,
      },
      {
        date: '2026-07-14',
        price: 3.1,
        institutional_net: 4,
        retail_net: 5,
        commercial_net: 6,
      },
      {
        date: '2026-07-21',
        price: 3.2,
        institutional_net: 7,
        retail_net: 8,
        commercial_net: 9,
      },
    ]
    const model = buildCotWorkstation({
      market: 'Natural Gas / NG',
      series: cotSeries,
      weeks: 3,
      has_price: true,
      has_commercial: true,
      has_retail: true,
    })
    const ohlcExportBlock = {
      weekly_ohlc: [
        { date: '2026-07-03', open: 1, high: 2, low: 0.5, close: 1.5 },
        { date: '2026-07-10', open: 1.5, high: 2.5, low: 1.4, close: 2.0 },
        { date: '2026-07-17', open: 2.0, high: 3.0, low: 1.9, close: 2.5 },
      ],
      cot_last_date: '2026-07-21',
      ohlc_last_date: '2026-07-17',
    }
    const bound = buildPositioningWorkstationSeries(model, null, ohlcExportBlock, {
      preserveFullCotHistory: true,
    })
    expect(bound.weeklyBars.at(-1)?.date).toBe('2026-07-17')
    expect(bound.weeklyBars.map((b) => b.date)).toContain('2026-07-17')
    expect(bound.weeklyBars.map((b) => b.date)).toContain('2026-07-10')
    expect(bound.meta.priceLastDate).toBe('2026-07-17')
    expect(bound.meta.priceNotTruncatedToCot).toBe(true)
    // Price continues on provider week dates even though COT last is 2026-07-21.
    expect(bound.meta.cotLastDate).toBe('2026-07-21')
    const lastPriceRow = bound.rows.find((r) => r.date === '2026-07-17')
    expect(lastPriceRow?.close).toBe(2.5)
  })

  it('rejects short mismatched store OHLC for indices', () => {
    const cotSeries = Array.from({ length: 100 }, (_, i) => ({
      date: `2024-01-${String((i % 28) + 1).padStart(2, '0')}`,
      price: 15000 + i,
    }))
    const priceRec = {
      weekly: [{ date: '2024-01-15', open: 700, high: 710, low: 690, close: 705 }],
    }
    expect(shouldUseStoreOhlc(priceRec, cotSeries)).toBe(false)
  })
})
