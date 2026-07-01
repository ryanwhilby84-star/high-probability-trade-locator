/**
 * @vitest-environment node
 */
import { describe, it, expect } from 'vitest'
import { buildCotWorkstation } from '../cot/buildCotWorkstation.js'
import { getInstrumentPrices } from '../priceData.js'
import {
  buildPositioningWorkstationSeries,
  shouldUseStoreOhlc,
} from './buildPositioningWorkstationSeries.js'

describe('buildPositioningWorkstationSeries', () => {
  it('aligns one row per COT week with matching candle timestamps', () => {
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
    expect(bound.weeklyBars).toHaveLength(2)
    expect(bound.rows[0].date).toBe(bound.weeklyBars[0].date)
    expect(bound.rows[0].time).toBe(bound.weeklyBars[0].time)
    expect(bound.rows[0].commercial_net).toBe(3)
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
