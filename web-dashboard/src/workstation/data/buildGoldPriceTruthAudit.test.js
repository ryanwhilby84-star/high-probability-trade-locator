import { describe, expect, it } from 'vitest'

import { buildGoldPriceTruthTable } from './buildGoldPriceTruthAudit.js'

const baseInput = {
  marketId: 'Gold',
  liveQuote: {
    live_bid: 4028.07,
    live_ask: 4028.64,
    live_price: 4028.355,
    live_price_source: 'oanda:XAU_USD',
    live_price_as_of: '2026-06-30T16:13:53.286626272Z',
    live_fetch_ok: true,
    latest_completed_ohlc_close: 4016.955,
    latest_completed_ohlc_date: '2026-06-28',
  },
  exportBlock: {
    price_source: 'oanda:XAU_USD',
    cot_last_date: '2026-06-23',
    weekly_ohlc: [{ date: '2026-06-28', close: 4016.955 }],
    tail_alignment_audit: {
      final_12_matched: [
        {
          cot_date: '2026-06-23',
          ohlc_date: '2026-06-28',
          close: 4016.955,
          cot_price: 4072.015,
          matched: true,
        },
      ],
    },
  },
  priceContext: {
    weeklyClose: 4016.955,
    weeklyCloseDate: '2026-06-28',
    livePrice: 4028.355,
    livePriceSource: 'oanda:XAU_USD',
    valuationPriceUsed: 4028.355,
    valuationPriceSource: 'oanda:XAU_USD',
  },
  displaySnapshot: {
    crosshairHeaderPrice: 4072.015,
    crosshairHeaderDate: '2026-06-23',
    liveMarkerPrice: 4028.355,
    latestVisibleBarClose: 4016.955,
    candleTooltipClose: 4016.955,
    candleTooltipDate: '2026-06-28',
  },
  valuationBlock: { spot_price: 4072.015, fair_value: 3525.87 },
}

describe('buildGoldPriceTruthTable', () => {
  it('builds 10 mandatory rows for Gold', () => {
    const result = buildGoldPriceTruthTable(baseInput)
    expect(result.table).toHaveLength(10)
    expect(result.table[2].field).toContain('mid')
    expect(result.table[2].category).toBe('LIVE')
    expect(result.table[4].category).toBe('WEEKLY_CLOSE')
  })

  it('fails when live marker does not match OANDA mid', () => {
    const result = buildGoldPriceTruthTable({
      ...baseInput,
      displaySnapshot: { ...baseInput.displaySnapshot, liveMarkerPrice: 4016.955 },
    })
    const fail = result.comparisons.find((c) => c.name === 'Red live marker uses OANDA mid')
    expect(fail?.status).toBe('FAIL')
    expect(result.summary.failCount).toBeGreaterThan(0)
  })

  it('ok when live layers match OANDA mid and weekly stays separate', () => {
    const result = buildGoldPriceTruthTable(baseInput)
    const liveMarker = result.comparisons.find((c) => c.name === 'Red live marker uses OANDA mid')
    const valuation = result.comparisons.find((c) => c.name === 'Valuation current price uses OANDA mid')
    expect(liveMarker?.status).toBe('OK')
    expect(valuation?.status).toBe('OK')
    expect(result.table.find((r) => r.field.includes('tooltip')).value).toBe(4016.955)
  })

  it('returns null for non-Gold', () => {
    expect(buildGoldPriceTruthTable({ ...baseInput, marketId: 'Silver' })).toBeNull()
  })
})
