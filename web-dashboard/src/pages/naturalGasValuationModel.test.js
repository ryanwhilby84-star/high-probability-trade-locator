import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import {
  NG_HEADLINE_V2,
  NG_MODEL_V1,
  NG_MODEL_V2,
  assertNgV2Contract,
  contributionRows,
  resolveNgValuationView,
} from './naturalGasValuationModel.js'

describe('Natural Gas valuation model view contract', () => {
  it('resolves v2 active status and contributions', () => {
    const doc = {
      summary: {
        active_model: NG_MODEL_V2,
        fallback_to_v1: false,
        headline: NG_HEADLINE_V2,
        production_transformation: 'production_yoy_pct',
        raw_level_used_in_fair_value: false,
      },
      instrument: {
        active_model: NG_MODEL_V2,
        fallback_to_v1: false,
        headline: NG_HEADLINE_V2,
        spot_price: 2.8,
        fair_value: 2.7,
        deviation_pct: 3.7,
        v1_fair_value: 2.82,
        v2_fair_value: 2.7,
        v1_v2_fair_value_diff: -0.12,
        validated_drivers: ['storage_surplus_bcf', 'production_yoy_pct'],
        as_of_week: '2026-07-30',
        production_observation_date: '2026-05-15',
        production_source_cadence: 'monthly',
        production_transformation: 'production_yoy_pct',
        raw_level_used_in_fair_value: false,
        confidence: 'Medium',
        confidence_reasons: ['active_model=ng_storage_production_v2'],
        deviation_pct_trusted: true,
        price_freshness: {
          provider: 'oanda',
          symbol: 'NATGAS_USD',
          overall_status: 'Current',
          live_quote: { price: 2.76, as_of: '2026-08-03T07:00:00Z', status: 'Current' },
          latest_completed_daily: { date: '2026-07-30', close: 2.799, status: 'Stale' },
          latest_completed_weekly: { date: '2026-07-24', close: 2.799, status: 'Stale' },
          market_comparison: { price: 2.76, status: 'Current', trusted: true, kind: 'live_snapshot' },
        },
        driver_contributions: {
          storage_surplus_bcf: {
            value: 172.6,
            coefficient: -0.0008,
            log_contribution: -0.14,
            direction: 'lowers fair value',
            label: 'Storage surplus/deficit',
          },
          production_yoy_pct: {
            value: 2.1,
            coefficient: -0.023,
            log_contribution: -0.048,
            direction: 'lowers fair value',
            label: 'Production YoY %',
          },
        },
      },
    }

    const view = resolveNgValuationView(doc)
    assert.equal(view.activeModel, NG_MODEL_V2)
    assert.equal(view.headline, NG_HEADLINE_V2)
    assert.equal(view.fallback, false)
    assert.equal(view.rawLevelUsed, false)
    assert.equal(view.deviationTrusted, true)
    assert.equal(view.priceStatus, 'Current')
    assert.equal(view.livePrice, 2.76)
    assert.deepEqual(view.validatedDrivers, ['storage_surplus_bcf', 'production_yoy_pct'])
    assert.equal(assertNgV2Contract(view).length, 0)

    const rows = contributionRows(view)
    assert.equal(rows.length, 2)
    assert.ok(rows.some((r) => r.feature === 'production_yoy_pct'))
  })

  it('marks v1 fallback status', () => {
    const view = resolveNgValuationView({
      instrument: {
        active_model: NG_MODEL_V1,
        fallback_to_v1: true,
        fallback_reason: 'production_yoy_stale_beyond_monthly_cadence',
        headline: `${NG_HEADLINE_V2} (v1 fallback)`,
        validated_drivers: ['storage_surplus_bcf'],
        production_transformation: 'production_yoy_pct',
        raw_level_used_in_fair_value: false,
        v1_fair_value: 2.82,
        fair_value: 2.82,
      },
    })
    assert.equal(view.activeModel, NG_MODEL_V1)
    assert.equal(view.fallback, true)
    assert.equal(assertNgV2Contract(view).length, 0)
  })
})
