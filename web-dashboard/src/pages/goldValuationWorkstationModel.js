/**
 * Adapt Gold market-clearing export into the NG Valuation Workstation week shape.
 * Display-only — no valuation maths.
 */

import {
  decisiveInterpretation,
  valuationBucket,
} from './naturalGasValuationWorkstationModel.js'

function normalizeBucket(bucket, deviationPct) {
  if (bucket === 'near_fair_value') return 'near_fair'
  if (bucket) return bucket
  return valuationBucket(deviationPct)
}

/** Build NG-compatible workstation history document from gold_valuation_latest.json */
export function buildGoldWorkstationHistory(goldDoc) {
  const inst = goldDoc?.instrument || {}
  const chart = Array.isArray(inst.display_chart) ? inst.display_chart : []
  const weeks = chart.map((r) => {
    const fair =
      r.fair_value != null && Number.isFinite(Number(r.fair_value))
        ? Number(r.fair_value)
        : null
    const dev =
      r.deviation_pct != null && Number.isFinite(Number(r.deviation_pct))
        ? Number(r.deviation_pct)
        : null
    const bucket = fair != null ? normalizeBucket(r.valuation_bucket, dev) : null
    const block = {
      fair_value: fair,
      deviation_pct: fair != null ? dev : null,
      valuation_bucket: bucket,
      model_type: 'Gold market-clearing',
      solver_status: r.solver_status || null,
      fair_value_quarter: r.fair_value_quarter || null,
      fair_value_publication_date: r.fair_value_publication_date || null,
      is_carried_forward: !!r.is_carried_forward,
    }
    return {
      model_week: r.date,
      market_price: r.market_price != null ? Number(r.market_price) : null,
      walk_forward: { ...block },
      frozen_v2: { ...block },
      fair_value_quarter: r.fair_value_quarter || null,
      fair_value_publication_date: r.fair_value_publication_date || null,
      solver_status: r.solver_status || null,
      is_carried_forward: !!r.is_carried_forward,
      quality_status: fair != null ? 'OK' : 'UNAVAILABLE',
    }
  })

  const withFv = weeks.filter((w) => w.walk_forward?.fair_value != null)
  return {
    market: 'Gold',
    weeks,
    coverage: {
      first_week: weeks[0]?.model_week || null,
      last_week: weeks[weeks.length - 1]?.model_week || null,
      n_weeks: weeks.length,
      n_walkforward_fair_values: withFv.length,
    },
    event_study_walkforward: { cooldown_weeks: 4 },
    verdict: {
      verdict: inst.latest_quarter_valid
        ? 'Useful confluence'
        : 'Latest tip invalid — history retained',
    },
    tip: {
      fair_value: inst.latest_valid_fair_value ?? inst.fair_value ?? null,
      latest_valid_quarter: inst.latest_valid_quarter || null,
      latest_valid_publication_date: inst.latest_valid_publication_date || null,
      market_quarter: inst.market_quarter || null,
      solver_status: inst.solver_status || null,
      latest_quarter_valid: inst.latest_quarter_valid === true,
      model_id: inst.model_id || inst.active_model || null,
      model_anchor_price: inst.model_anchor_price ?? inst.spot_price ?? null,
      publication_date: inst.publication_date || null,
      total_demand: inst.total_demand ?? null,
      total_supply: inst.total_supply ?? null,
      net_imbalance_tonnes: inst.net_imbalance_tonnes ?? null,
      market_contributions: inst.market_contributions || null,
    },
  }
}

export function goldLiveState({
  marketPrice,
  fairValue,
  priceStatus,
  priceLabel,
  priceSource,
  asOf,
  tip,
}) {
  const p =
    marketPrice != null && Number.isFinite(Number(marketPrice)) ? Number(marketPrice) : null
  const f =
    fairValue != null && Number.isFinite(Number(fairValue)) ? Number(fairValue) : null
  const trusted = p != null && f != null && f > 0
  const liveDev = trusted ? (100 * (p - f)) / f : null
  const decisive = decisiveInterpretation(liveDev)
  const st = String(priceStatus || '').toUpperCase()
  const updateMode =
    st === 'LIVE'
      ? 'LIVE'
      : st === 'STALE'
        ? 'STALE'
        : st === 'FALLBACK'
          ? 'SNAPSHOT'
          : st || 'STALE'

  return {
    market_price: p,
    physical_fair_value: f,
    live_deviation_pct: liveDev,
    live_deviation_pct_display: liveDev == null ? null : Math.round(liveDev * 100) / 100,
    deviation_trusted:
      trusted &&
      (st === 'LIVE' || st === 'STALE' || st === 'FALLBACK' || st === 'SNAPSHOT' || !st),
    state_headline: decisive.headline,
    interpretation: decisive.detail,
    strength: decisive.strength,
    price_source: priceSource || priceLabel || '—',
    update_mode: updateMode,
    price_status: st || '—',
    price_updated: asOf || null,
    comparison_status: trusted
      ? st === 'STALE'
        ? 'Stale'
        : 'Current'
      : 'Unavailable',
    model_verdict: tip?.latest_quarter_valid
      ? 'Useful confluence'
      : 'Latest tip invalid — history retained',
    model_as_of: tip?.latest_valid_publication_date || tip?.publication_date || null,
    storage_as_of: tip?.latest_valid_quarter || null,
    production_as_of: tip?.market_quarter || null,
    published_model_id: tip?.model_id || null,
    fair_value_stable: true,
  }
}
