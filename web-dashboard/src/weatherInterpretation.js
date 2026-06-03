/**
 * Client-side weather interpretation (matches ``weather_interpretation.py`` when JSON is stale).
 */

function interp(fields) {
  return { ...fields }
}

function interpretWheat(region, s, precipMm, baseConf) {
  const cold = s.cold_snap
  const heat = s.heatwave
  const storm = s.storm
  const heavy = s.heavy_precip
  const dry = s.dry_spell
  const coldAnom = s.temp_anomaly_low
  const warmAnom = s.temp_anomaly_high

  if (cold && dry) {
    return interp({
      crop_impact: 'bad',
      crop_impact_label: 'Stress risk',
      price_impact: 'bullish',
      price_impact_label: 'Bullish wheat',
      confidence: baseConf === 'low' ? 'medium' : 'high',
      badge: 'red',
      reason: `${region}: cold/dry conditions can threaten crop quality and yield.`,
    })
  }
  if (cold || coldAnom) {
    return interp({
      crop_impact: 'bad',
      crop_impact_label: 'Stress risk',
      price_impact: 'bullish',
      price_impact_label: 'Bullish wheat',
      confidence: baseConf,
      badge: 'red',
      reason: `${region}: cold snap risk to emerging or filling crops.`,
    })
  }
  if (dry) {
    return interp({
      crop_impact: 'bad',
      crop_impact_label: 'Dryness risk',
      price_impact: 'bullish',
      price_impact_label: 'Bullish wheat',
      confidence: baseConf,
      badge: 'red',
      reason: `${region}: limited rainfall raises moisture-stress concern.`,
    })
  }
  if (heat || warmAnom) {
    return interp({
      crop_impact: 'bad',
      crop_impact_label: 'Heat stress',
      price_impact: 'bullish',
      price_impact_label: 'Bullish wheat',
      confidence: baseConf,
      badge: 'red',
      reason: `${region}: heat can stress yields during sensitive growth windows.`,
    })
  }
  if (storm || (heavy && precipMm >= 25)) {
    return interp({
      crop_impact: 'mixed',
      crop_impact_label: 'Mixed (flood risk)',
      price_impact: 'mixed',
      price_impact_label: 'Mixed wheat',
      confidence: 'medium',
      badge: 'amber',
      reason: `${region}: heavy rain/storms can help soil moisture but raise flooding/damage risk.`,
    })
  }
  if (heavy || precipMm >= 10) {
    return interp({
      crop_impact: 'good',
      crop_impact_label: 'Moisture supportive',
      price_impact: 'bearish',
      price_impact_label: 'Neutral/Bearish wheat',
      confidence: baseConf,
      badge: 'green',
      reason: `${region}: rainfall can improve crop conditions unless totals become excessive.`,
    })
  }
  if (precipMm >= 0.1) {
    return interp({
      crop_impact: 'good',
      crop_impact_label: 'Moisture adequate',
      price_impact: 'neutral',
      price_impact_label: 'Neutral wheat',
      confidence: 'low',
      badge: 'green',
      reason: `${region}: light precipitation is generally crop-supportive.`,
    })
  }
  return interp({
    crop_impact: 'neutral',
    crop_impact_label: 'Neutral',
    price_impact: 'neutral',
    price_impact_label: 'Neutral wheat',
    confidence: 'low',
    badge: 'amber',
    reason: `${region}: no dominant stress signal in the current forecast window.`,
  })
}

function interpretNatGas(region, s, precipMm, baseConf) {
  const cold = s.cold_snap
  const heat = s.heatwave
  const storm = s.storm
  const heavy = s.heavy_precip
  const coldAnom = s.temp_anomaly_low
  const warmAnom = s.temp_anomaly_high

  if (cold || coldAnom) {
    return interp({
      crop_impact: 'neutral',
      crop_impact_label: 'Heating demand firm',
      price_impact: 'bullish',
      price_impact_label: 'Bullish nat gas',
      confidence: baseConf === 'low' ? 'medium' : 'high',
      badge: 'red',
      reason: `${region}: colder outlook lifts residential/commercial heating demand.`,
    })
  }
  if (heat || warmAnom) {
    return interp({
      crop_impact: 'neutral',
      crop_impact_label: 'Cooling demand elevated',
      price_impact: 'bullish',
      price_impact_label: 'Bullish nat gas (power burn)',
      confidence: baseConf,
      badge: 'red',
      reason: `${region}: heat can lift power-sector gas burn for cooling.`,
    })
  }
  if (storm || (heavy && precipMm >= 15)) {
    return interp({
      crop_impact: 'neutral',
      crop_impact_label: 'Mixed (supply/logistics)',
      price_impact: 'mixed',
      price_impact_label: 'Mixed nat gas',
      confidence: 'medium',
      badge: 'amber',
      reason: `${region}: storms can disrupt Gulf production/flows — direction depends on damage vs demand loss.`,
    })
  }
  if (!cold && !heat && precipMm < 5) {
    return interp({
      crop_impact: 'neutral',
      crop_impact_label: 'Mild demand backdrop',
      price_impact: 'bearish',
      price_impact_label: 'Bearish nat gas',
      confidence: 'low',
      badge: 'green',
      reason: `${region}: mild conditions reduce heating/cooling demand versus extremes.`,
    })
  }
  return interp({
    crop_impact: 'neutral',
    crop_impact_label: 'Neutral demand',
    price_impact: 'neutral',
    price_impact_label: 'Neutral nat gas',
    confidence: 'low',
    badge: 'amber',
    reason: `${region}: weather not extreme enough for a clear demand bias.`,
  })
}

export function interpretWeatherRegion(record, market) {
  if (record?.interpretation && typeof record.interpretation === 'object') {
    return record.interpretation
  }
  const region = String(record?.region || 'Region')
  const imp = String(record?.importance || 'low').toLowerCase()
  const baseConf = imp === 'high' ? 'high' : imp === 'medium' ? 'medium' : 'low'
  const s = record?.signals || {}
  const precipMm = Number(record?.precipitation_mm_24h) || 0
  if (market === 'Wheat') return interpretWheat(region, s, precipMm, baseConf)
  if (market === 'Natural Gas / NG') return interpretNatGas(region, s, precipMm, baseConf)
  return interp({
    crop_impact: 'neutral',
    crop_impact_label: 'Neutral',
    price_impact: 'neutral',
    price_impact_label: 'Neutral',
    confidence: 'low',
    badge: 'amber',
    reason: 'Weather context not mapped for this market.',
  })
}

export function weeklyBiasLine(weatherContext, market) {
  const block = weatherContext?.markets?.[market]
  if (block?.weekly_bias_line) return block.weekly_bias_line
  return `Weather bias this week: Mixed for ${market}.`
}
