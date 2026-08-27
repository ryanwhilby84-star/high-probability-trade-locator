/**
 * Chart Workstation analytics — percentiles, forward returns, state labels.
 * Computed client-side from the COT + price series (no new data sources).
 */

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

export const EXTREME_PERCENTILE = 10

export const PCT_CLASS = {
  EXTREME_LOW: 'Extreme Low',
  LOW: 'Low',
  NEUTRAL: 'Neutral',
  HIGH: 'High',
  EXTREME_HIGH: 'Extreme High',
  NA: 'N/A',
}

/** Empirical percentile rank 0–100 (share of window strictly below value). */
export function percentileRank(windowValues, value) {
  if (!isNum(value)) return null
  const vals = (windowValues || []).filter(isNum)
  if (!vals.length) return null
  let below = 0
  for (const v of vals) {
    if (v < value) below += 1
  }
  return (below / vals.length) * 100
}

/** Linear-interpolation quantile (q in 0–1). */
export function quantile(values, q) {
  const sorted = (values || []).filter(isNum).sort((a, b) => a - b)
  if (!sorted.length) return null
  if (q <= 0) return sorted[0]
  if (q >= 1) return sorted[sorted.length - 1]
  const idx = (sorted.length - 1) * q
  const lo = Math.floor(idx)
  const hi = Math.ceil(idx)
  if (lo === hi) return sorted[lo]
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo)
}

export function classifyPercentile(pct) {
  if (!isNum(pct)) return PCT_CLASS.NA
  const p = Math.max(0, Math.min(100, pct))
  if (p < 10) return PCT_CLASS.EXTREME_LOW
  if (p < 30) return PCT_CLASS.LOW
  if (p < 70) return PCT_CLASS.NEUTRAL
  if (p < 90) return PCT_CLASS.HIGH
  return PCT_CLASS.EXTREME_HIGH
}

export function isExtremePercentile(pct) {
  if (!isNum(pct)) return false
  return pct <= EXTREME_PERCENTILE || pct >= 100 - EXTREME_PERCENTILE
}

export function extremeZoneLabel(pct) {
  if (!isNum(pct)) return null
  if (pct >= 100 - EXTREME_PERCENTILE) return 'Top 10%'
  if (pct <= EXTREME_PERCENTILE) return 'Bottom 10%'
  return null
}

/** p10 / p90 thresholds for subtle chart shading. */
export function percentileExtremeThresholds(series, key, endIndex = null) {
  const slice = endIndex == null ? series : series.slice(0, endIndex + 1)
  const vals = slice.map((r) => r[key]).filter(isNum)
  if (vals.length < 8) return { low: null, high: null, p10: null, p90: null, n: vals.length }
  const p10 = quantile(vals, 0.1)
  const p90 = quantile(vals, 0.9)
  return { low: p10, high: p90, p10, p90, n: vals.length }
}

function forwardPriceReturn(series, index, weeks) {
  const start = series[index]?.price
  const end = series[index + weeks]?.price
  if (!isNum(start) || !isNum(end) || start === 0) return null
  return ((end - start) / start) * 100
}

function valuationState(pct) {
  return classifyPercentile(pct)
}

/**
 * Enrich each row with expanding-window percentiles, forward returns, and flags.
 * @param {object[]} series
 * @param {number|null} historyEndIndex — cap history for replay (inclusive index)
 */
export function enrichChartAnalytics(series, historyEndIndex = null) {
  if (!Array.isArray(series) || !series.length) return []

  const histEnd = historyEndIndex == null ? series.length - 1 : Math.min(historyEndIndex, series.length - 1)
  const histSlice = series.slice(0, histEnd + 1)

  const instThresholds = percentileExtremeThresholds(histSlice, 'institutional_net')
  const retailThresholds = percentileExtremeThresholds(histSlice, 'retail_net')

  return series.map((row, i) => {
    const history = series.slice(0, Math.min(i, histEnd) + 1)
    const instWindow = history.map((r) => r.institutional_net)
    const retailWindow = history.map((r) => r.retail_net)
    const valWindow = history.map((r) => r.location_percentile_52w ?? r.location).filter(isNum)

    const institutional_pct = percentileRank(instWindow, row.institutional_net)
    const retail_pct = percentileRank(retailWindow, row.retail_net)
    const location_pct = isNum(row.location_percentile_52w)
      ? row.location_percentile_52w
      : isNum(row.location)
        ? percentileRank(valWindow, row.location)
        : null

    const institutional_class = classifyPercentile(institutional_pct)
    const retail_class = classifyPercentile(retail_pct)

    return {
      ...row,
      index: i,
      institutional_pct,
      retail_pct,
      location_pct,
      institutional_class,
      retail_class,
      institutional_extreme: isExtremePercentile(institutional_pct),
      retail_extreme: isExtremePercentile(retail_pct),
      institutional_extreme_label: extremeZoneLabel(institutional_pct),
      retail_extreme_label: extremeZoneLabel(retail_pct),
      seasonality_state: PCT_CLASS.NA,
      location_state: valuationState(location_pct),
      forward_return_4w: forwardPriceReturn(series, i, 4),
      forward_return_8w: forwardPriceReturn(series, i, 8),
      forward_return_12w: forwardPriceReturn(series, i, 12),
    }
  })
}

export function sliceSeriesForReplay(series, replayCutoffIndex) {
  if (replayCutoffIndex == null || replayCutoffIndex < 0) return series
  return series.slice(0, replayCutoffIndex + 1)
}

export function findSeriesIndexByDate(series, dateStr) {
  if (!dateStr || !series?.length) return -1
  const d = String(dateStr).slice(0, 10)
  return series.findIndex((r) => r.date === d || r.label === d)
}

export function pointExplainSnapshot(point) {
  if (!point) return null
  return {
    date: point.date || point.label,
    price: point.price,
    institutional_net: point.institutional_net,
    institutional_pct: point.institutional_pct,
    institutional_class: point.institutional_class,
    institutional_extreme_label: point.institutional_extreme_label,
    retail_net: point.retail_net,
    retail_pct: point.retail_pct,
    retail_class: point.retail_class,
    retail_extreme_label: point.retail_extreme_label,
    institutional_wow: point.institutional_wow,
    retail_wow: point.retail_wow,
    location: point.location,
    location_state: point.location_state,
    valuation_fair: point.valuation_fair,
    forward_return_4w: point.forward_return_4w,
    forward_return_8w: point.forward_return_8w,
    forward_return_12w: point.forward_return_12w,
    index: point.index,
  }
}
