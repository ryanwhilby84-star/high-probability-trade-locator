import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { normalizeDrawingForPersist } from '../workstation/canvas/workstationDrawingCoords.js'
import { validatePriceEngine } from './priceEngineValidation.js'
import { LivePriceStore } from './stores/LivePriceStore.js'
import { WeeklyOHLCStore } from './stores/WeeklyOHLCStore.js'
import { HistoricalCOTStore } from './stores/HistoricalCOTStore.js'

describe('price engine stores', () => {
  beforeEach(() => {
    vi.spyOn(LivePriceStore, 'getQuote').mockReturnValue({
      mid: 3977.52,
      bid: 3977.4,
      ask: 3977.64,
      source: 'oanda:XAU_USD',
      asOf: '2026-06-30T16:57:58Z',
    })
    vi.spyOn(LivePriceStore, 'getStatus').mockReturnValue('LIVE')
    vi.spyOn(WeeklyOHLCStore, 'getCompletedWeekly').mockReturnValue({
      close: 4016.955,
      date: '2026-06-28',
    })
    vi.spyOn(HistoricalCOTStore, 'getHistoricalCloseAtDate').mockReturnValue(4010.5)
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('validatePriceEngine passes when consumers match stores', () => {
    const result = validatePriceEngine('Gold', {
      liveMarkerPrice: 3977.52,
      valuationLivePrice: 3977.52,
      truthTableLivePrice: 3977.52,
      headerLivePrice: 3977.52,
      weeklyChartClose: 4016.955,
      candleBadgeClose: 4016.955,
      historicalHeaderPrice: 4010.5,
      historicalHeaderDate: '2026-06-28',
      crosshairDate: '2026-06-28',
    })
    expect(result.pass).toBe(true)
  })

  it('validatePriceEngine fails when live marker diverges', () => {
    const result = validatePriceEngine('Gold', {
      liveMarkerPrice: 4000,
      valuationLivePrice: 3977.52,
    })
    expect(result.pass).toBe(false)
    expect(result.failures.length).toBeGreaterThan(0)
  })
})

describe('normalizeDrawingForPersist', () => {
  it('strips ephemeral bar times', () => {
    const v = normalizeDrawingForPersist({ type: 'vline', date: '2026-01-01', time: 99 })
    expect(v.time).toBeUndefined()
    expect(v.date).toBe('2026-01-01')
  })
})
