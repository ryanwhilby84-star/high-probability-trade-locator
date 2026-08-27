/**
 * Natural Gas valuation payload presentation helpers (ng_storage_production_v2).
 */

export const NG_MODEL_V1 = 'ng_storage_v1'
export const NG_MODEL_V2 = 'ng_storage_production_v2'
export const NG_HEADLINE_V2 = 'Validated Two-Driver Fair Value'

export function resolveNgValuationView(doc) {
  const inst = doc?.instrument || {}
  const summary = doc?.summary || {}
  const activeModel = inst.active_model || summary.active_model || inst.model_id || null
  const fallback = Boolean(inst.fallback_to_v1 ?? summary.fallback_to_v1)
  const headline =
    inst.headline ||
    summary.headline ||
    (activeModel === NG_MODEL_V2 && !fallback ? NG_HEADLINE_V2 : NG_HEADLINE_V2)
  const contributions =
    inst.driver_contributions ||
    inst.contribution_breakdown?.driver_contributions ||
    null
  const validatedDrivers =
    inst.validated_drivers ||
    summary.validated_drivers ||
    inst.validated_features ||
    summary.validated_features ||
    []

  const priceFreshness = inst.price_freshness || null
  const live = priceFreshness?.live_quote || {}
  const completedDaily = priceFreshness?.latest_completed_daily || {}
  const completedWeekly = priceFreshness?.latest_completed_weekly || {}
  const comparison = priceFreshness?.market_comparison || {}
  const deviationTrusted = inst.deviation_pct_trusted !== false && comparison.trusted !== false

  return {
    activeModel,
    fallback,
    fallbackReason: inst.fallback_reason || summary.fallback_reason || null,
    headline,
    marketPrice: comparison.price ?? live.price ?? inst.spot_price ?? null,
    modelAnchorPrice: inst.model_anchor_price ?? inst.spot_price ?? null,
    fairValue: inst.fair_value ?? null,
    deviationPct: deviationTrusted ? inst.deviation_pct ?? null : null,
    deviationTrusted,
    deviationUntrusted: inst.deviation_pct_stale_untrusted ?? null,
    v1FairValue: inst.v1_fair_value ?? summary.v1_fair_value ?? inst.v1_benchmark?.fair_value ?? null,
    v2FairValue: inst.v2_fair_value ?? summary.v2_fair_value ?? inst.v2_model?.fair_value ?? null,
    v1V2Diff: inst.v1_v2_fair_value_diff ?? null,
    validatedDrivers,
    asOfWeek: inst.as_of_week ?? null,
    productionObservationDate: inst.production_observation_date ?? null,
    productionSourceCadence: inst.production_source_cadence || 'monthly',
    productionTransformation: inst.production_transformation || summary.production_transformation || 'production_yoy_pct',
    rawLevelUsed: Boolean(inst.raw_level_used_in_fair_value ?? summary.raw_level_used_in_fair_value),
    confidence: inst.confidence ?? summary.confidence ?? null,
    confidenceReasons: inst.confidence_reasons || summary.confidence_reasons || [],
    freshnessWarnings: inst.freshness_warnings || summary.freshness_warnings || [],
    priceSource: priceFreshness?.provider
      ? `${priceFreshness.provider}:${priceFreshness.symbol || ''}`
      : null,
    livePrice: live.price ?? null,
    livePriceAsOf: live.as_of ?? null,
    livePriceStatus: live.status ?? null,
    latestCompletedDaily: completedDaily,
    latestCompletedWeekly: completedWeekly,
    formingDaily: priceFreshness?.forming_daily || null,
    priceStatus: priceFreshness?.overall_status || comparison.status || null,
    dataAgeHours: comparison.age_hours ?? live.age_hours ?? null,
    contributions,
    contributionBreakdown: inst.contribution_breakdown || null,
    equation: inst.equation || null,
  }
}

export function contributionRows(view) {
  const map = view?.contributions || {}
  return Object.entries(map).map(([feature, row]) => ({
    feature,
    value: row?.value,
    coefficient: row?.coefficient,
    logContribution: row?.log_contribution,
    priceImpactPct: row?.price_impact_pct,
    direction: row?.direction,
    label: row?.label || feature,
  }))
}

export function assertNgV2Contract(view) {
  const errors = []
  if (!view.activeModel) errors.push('missing activeModel')
  if (view.productionTransformation !== 'production_yoy_pct') {
    errors.push('production_transformation must be production_yoy_pct')
  }
  if (view.rawLevelUsed) errors.push('raw production level must not be used')
  if (view.activeModel === NG_MODEL_V2) {
    const drivers = view.validatedDrivers || []
    if (!(drivers.includes('storage_surplus_bcf') && drivers.includes('production_yoy_pct'))) {
      errors.push('v2 must validate storage_surplus_bcf and production_yoy_pct')
    }
    if (view.fallback) errors.push('v2 active must not set fallback_to_v1')
  }
  if (view.activeModel === NG_MODEL_V1 && !view.fallback) {
    errors.push('storage-only active publish must mark fallback_to_v1')
  }
  return errors
}
