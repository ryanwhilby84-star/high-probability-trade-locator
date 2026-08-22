/** View helpers for Gold market-clearing valuation — NG page slot mapping. */

export const GOLD_HEADLINE = 'Gold Market-Clearing Fair Value'

export function classifyBucket(dev) {
  if (dev == null || !Number.isFinite(Number(dev))) return null
  const d = Number(dev)
  if (d <= -15) return 'materially_undervalued'
  if (d < -5) return 'undervalued'
  if (d <= 5) return 'near_fair_value'
  if (d < 15) return 'overvalued'
  return 'materially_overvalued'
}

export function bucketLabel(bucket) {
  const map = {
    materially_undervalued: 'Materially undervalued',
    undervalued: 'Undervalued',
    near_fair_value: 'Near fair',
    near_fair: 'Near fair',
    overvalued: 'Overvalued',
    materially_overvalued: 'Materially overvalued',
  }
  return map[bucket] || (bucket ? String(bucket).replace(/_/g, ' ') : '—')
}

export function scaleFromDeviation(dev) {
  if (dev == null || !Number.isFinite(Number(dev))) {
    return { pct: 50, band: '—' }
  }
  const d = Number(dev)
  // Map ±30% into 0–100 for the NG valuation scale pin
  const pct = Math.max(0, Math.min(100, 50 + (d / 30) * 50))
  return {
    pct,
    band: bucketLabel(classifyBucket(d)),
  }
}

export function resolveGoldValuationView(doc, canonical) {
  const inst = doc?.instrument || {}
  const freshness = inst.data_freshness || {}
  const hasValidFv = inst.latest_valid_fair_value != null || inst.fair_value != null
  const fairValue = hasValidFv
    ? Number(inst.latest_valid_fair_value ?? inst.fair_value)
    : null
  const livePrice =
    canonical?.price != null && Number.isFinite(Number(canonical.price))
      ? Number(canonical.price)
      : null
  const deviationTrusted = hasValidFv && livePrice != null && fairValue != null && fairValue !== 0
  const deviationPct = deviationTrusted
    ? (100 * (livePrice - fairValue)) / fairValue
    : null
  const latestQuarterValid = inst.latest_quarter_valid === true
  const contrib = inst.market_contributions || {}
  const demand = contrib.demand || {}
  const supply = contrib.supply || {}

  const driverCards = [
    {
      id: 'jewellery',
      label: 'Jewellery',
      source: 'WGC GDT',
      current: demand.jewellery,
      unit: 't',
      available: demand.jewellery != null,
      institutional_effect: 'Demand',
      tone: 'neutral',
      interpretation: 'Fabrication / jewellery demand (tonnes).',
    },
    {
      id: 'technology',
      label: 'Technology',
      source: 'WGC GDT',
      current: demand.technology,
      unit: 't',
      available: demand.technology != null,
      institutional_effect: 'Demand',
      tone: 'neutral',
      interpretation: 'Technology / industrial demand (tonnes).',
    },
    {
      id: 'investment',
      label: 'Bar, Coin & ETF',
      source: 'WGC GDT',
      current:
        demand.bar_coin != null || demand.etf != null
          ? (Number(demand.bar_coin || 0) || 0) + (Number(demand.etf || 0) || 0)
          : demand.investment,
      unit: 't',
      available:
        demand.bar_coin != null || demand.etf != null || demand.investment != null,
      institutional_effect: 'Demand',
      tone: 'neutral',
      interpretation: 'Investment demand: bar & coin plus ETF flows.',
    },
    {
      id: 'central_bank',
      label: 'Central Banks',
      source: 'WGC GDT',
      current: demand.central_bank,
      unit: 't',
      available: demand.central_bank != null,
      institutional_effect: 'Demand',
      tone: 'neutral',
      interpretation: 'Official-sector net purchases (tonnes).',
    },
    {
      id: 'mine',
      label: 'Mine Supply',
      source: 'WGC GDT',
      current: supply.mine,
      unit: 't',
      available: supply.mine != null,
      institutional_effect: 'Supply',
      tone: 'neutral',
      interpretation: 'Mine production (tonnes).',
    },
    {
      id: 'recycling',
      label: 'Recycling',
      source: 'WGC GDT',
      current: supply.recycling,
      unit: 't',
      available: supply.recycling != null,
      institutional_effect: 'Supply',
      tone: 'neutral',
      interpretation: 'Recycled gold supply (tonnes).',
    },
  ]

  const history = Array.isArray(inst.display_chart)
    ? inst.display_chart.map((r) => ({
        date: r.date,
        spot_price: r.market_price,
        fair_value: r.fair_value,
        deviation_pct: r.deviation_pct,
        fair_value_quarter: r.fair_value_quarter,
        fair_value_publication_date: r.fair_value_publication_date,
        solver_status: r.solver_status,
        is_live_price: r.is_live_price,
        is_carried_forward: r.is_carried_forward,
      }))
    : []

  // Append live tip on the chart series (same role as NG live overlay via cards)
  if (livePrice != null && history.length) {
    const last = history[history.length - 1]
    const today = new Date().toISOString().slice(0, 10)
    const liveDate = today > last.date ? today : last.date
    const fv = last.fair_value
    const liveDev =
      fv != null && Number(fv) !== 0 ? (100 * (livePrice - Number(fv))) / Number(fv) : null
    const liveRow = {
      date: liveDate,
      spot_price: livePrice,
      fair_value: fv,
      deviation_pct: liveDev,
      fair_value_quarter: last.fair_value_quarter,
      fair_value_publication_date: last.fair_value_publication_date,
      solver_status: last.solver_status,
      is_live_price: true,
      is_carried_forward: true,
    }
    if (last.date === liveDate) {
      history[history.length - 1] = { ...last, ...liveRow }
    } else {
      history.push(liveRow)
    }
  }

  return {
    headline: GOLD_HEADLINE,
    activeModel: inst.active_model || inst.model_id || '—',
    livePrice,
    marketPrice: inst.model_anchor_price ?? inst.spot_price,
    fairValue,
    latestValidQuarter: inst.latest_valid_quarter || null,
    latestValidPublicationDate: inst.latest_valid_publication_date || null,
    marketQuarter: inst.market_quarter || null,
    publicationDate: inst.publication_date || freshness.latest_publication_date || null,
    latestQuarterValid,
    solverStatus: inst.solver_status || inst.latest_model_quarter_status || '—',
    deviationPct,
    deviationTrusted,
    priceStatus: canonical?.status || (livePrice != null ? 'Current' : 'Unavailable'),
    priceSource: canonical?.source || 'OANDA / canonical',
    livePriceStatus: canonical?.label || null,
    livePriceAsOf: canonical?.asOf || null,
    modelAnchorPrice: inst.model_anchor_price ?? inst.spot_price,
    asOfWeek: inst.as_of_week || inst.latest_valid_publication_date || null,
    equation: inst.equation || null,
    scale: scaleFromDeviation(deviationPct),
    contributionBreakdown: {
      drivers: [
        {
          feature: 'jewellery',
          label: 'Jewellery',
          raw_observation: demand.jewellery,
          coefficient: null,
          log_contribution: null,
          price_impact_pct: null,
          direction: 'Demand',
        },
        {
          feature: 'technology',
          label: 'Technology',
          raw_observation: demand.technology,
          coefficient: null,
          log_contribution: null,
          price_impact_pct: null,
          direction: 'Demand',
        },
        {
          feature: 'bar_coin',
          label: 'Bar & coin',
          raw_observation: demand.bar_coin,
          coefficient: null,
          log_contribution: null,
          price_impact_pct: null,
          direction: 'Demand',
        },
        {
          feature: 'etf',
          label: 'ETF',
          raw_observation: demand.etf,
          coefficient: null,
          log_contribution: null,
          price_impact_pct: null,
          direction: 'Demand',
        },
        {
          feature: 'central_bank',
          label: 'Central banks',
          raw_observation: demand.central_bank,
          coefficient: null,
          log_contribution: null,
          price_impact_pct: null,
          direction: 'Demand',
        },
        {
          feature: 'mine',
          label: 'Mine supply',
          raw_observation: supply.mine,
          coefficient: null,
          log_contribution: null,
          price_impact_pct: null,
          direction: 'Supply',
        },
        {
          feature: 'recycling',
          label: 'Recycling',
          raw_observation: supply.recycling,
          coefficient: null,
          log_contribution: null,
          price_impact_pct: null,
          direction: 'Supply',
        },
        {
          feature: 'producer_hedging',
          label: 'Producer hedging',
          raw_observation: supply.producer_hedging,
          coefficient: null,
          log_contribution: null,
          price_impact_pct: null,
          direction: 'Supply',
        },
      ].filter((r) => r.raw_observation != null),
      intercept_log_contribution: null,
      sum_log_contributions: null,
      reconstructed_fair_value: fairValue,
      market_price: livePrice ?? inst.model_anchor_price,
      deviation_pct: deviationPct,
      identity: 'FV from market-clearing solve; sectors in tonnes (WGC GDT).',
      reconciliation_ok: latestQuarterValid || hasValidFv,
      note: inst.model_note || '',
    },
    contributions: contrib,
    validatedDrivers: ['market_clearing', 'wgc_gdt_sectors'],
    driverCards,
    history,
    totalDemand: inst.total_demand ?? contrib.total_demand,
    totalSupply: inst.total_supply ?? contrib.total_supply,
    netImbalance: inst.net_imbalance_tonnes ?? contrib.net_imbalance_tonnes,
    gdtQuarters: inst.gdt_quarters_loaded ?? freshness.gdt_quarters,
    panelQuarters: inst.panel_quarters,
    nValidHistorical: inst.n_valid_historical_quarters,
    summaryText: inst.summary_text || inst.driver_summary || '—',
    modelNote: inst.model_note || null,
    freshnessWarnings: !latestQuarterValid
      ? [
          `Latest model quarter invalid (${inst.solver_status || 'SOLVER_INVALID'}). Showing most recent valid fair value${
            inst.latest_valid_quarter ? ` from ${inst.latest_valid_quarter}` : ''
          }${
            inst.latest_valid_publication_date
              ? ` (published ${inst.latest_valid_publication_date})`
              : ''
          }.`,
        ]
      : [],
  }
}
