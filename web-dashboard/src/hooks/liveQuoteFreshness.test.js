import { describe, expect, it, vi, afterEach, beforeEach } from 'vitest'

import {
  getLiveQuoteFreshness,
  LIVE_QUOTE_STALE_MS,
  parseIsoMs,
} from './liveQuoteFreshness.js'
import { applyLivePriceToIveDisplay } from '../workstation/data/instrumentPriceDiagnostics.js'
import { LivePriceStore } from '../prices/stores/LivePriceStore.js'

describe('liveQuoteFreshness', () => {
  afterEach(() => {
    vi.useRealTimers()
  })

  it('marks quote stale when older than 60s', () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-06-30T16:00:00.000Z'))
    const quote = { live_price_as_of: '2026-06-30T15:58:00.000Z', live_price: 4030 }
    const fresh = getLiveQuoteFreshness(quote, { generated_at: '2026-06-30T15:58:00.000Z' })
    expect(fresh.isStale).toBe(true)
    expect(fresh.ageMs).toBeGreaterThan(LIVE_QUOTE_STALE_MS)
  })

  it('marks quote fresh when within 60s', () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-06-30T16:00:30.000Z'))
    const quote = { live_price_as_of: '2026-06-30T16:00:00.000Z', live_price: 4030 }
    const fresh = getLiveQuoteFreshness(quote, null)
    expect(fresh.isStale).toBe(false)
  })

  it('parseIsoMs returns null for invalid', () => {
    expect(parseIsoMs('')).toBeNull()
    expect(parseIsoMs('2026-06-30T16:00:00.000Z')).toBeTypeOf('number')
  })
})

describe('applyLivePriceToIveDisplay', () => {
  beforeEach(() => {
    LivePriceStore.clearCache()
    vi.spyOn(LivePriceStore, 'getQuote').mockReturnValue({
      mid: 4028,
      source: 'oanda:XAU_USD',
      asOf: '2026-06-30T16:00:00.000Z',
    })
    vi.spyOn(LivePriceStore, 'getStatus').mockReturnValue('STALE')
    vi.spyOn(LivePriceStore, 'getFreshness').mockReturnValue({ isStale: true, ageMs: 120_000 })
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('does not substitute weekly for stale live valuation price', () => {
    const base = { modelSpotPrice: 4072, currentPrice: 4072, fairValue: 3500, valuationPct: 5 }
    const out = applyLivePriceToIveDisplay(base, 'Gold')
    expect(out.liveQuoteStale).toBe(true)
    expect(out.valuationStaleNote).toBe('STALE LIVE')
    expect(out.currentPrice).toBeNull()
    expect(out.valuationPriceUsed).toBeNull()
    expect(out.livePrice).toBe(4028)
  })
})
