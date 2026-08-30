/** Seasonal Roadmap view helpers (UI only). */
export const ROADMAP_METHOD_LABEL = 'Seasonal Roadmap'
export const ROADMAP_METHOD_DESCRIPTION =
  'Robust daily close-to-close historical returns, compounded trading-day by trading-day and rebased to the current price. No synthetic interpolation or default smoothing.'

export const ROADMAP_HORIZON_WEEKS = [4, 8, 12]

export function classifyRoadmapHorizon(row) {
  if (!row || row.n == null || row.n <= 0) return 'Mixed'
  const { mean, median, bullish_frequency: bull, bearish_frequency: bear } = row
  if (mean != null && median != null && bull != null && mean > 0 && median > 0 && bull > 0.5) return 'Bullish'
  if (mean != null && median != null && bear != null && mean < 0 && median < 0 && bear > 0.5) return 'Bearish'
  return 'Mixed'
}

export function resolveRoadmapSeriesSource(roadmap, useSmoothed) {
  if (!roadmap?.available) return { sourcePath: null, datasetName: null }
  if (useSmoothed && roadmap.smoothed?.full_year?.length) {
    return {
      sourcePath: 'payload.seasonal_roadmap.smoothed.full_year',
      datasetName: roadmap.method?.version || 'robust_daily_returns_v3',
      valueKey: 'price',
    }
  }
  if (roadmap.unsmoothed?.full_year?.length) {
    return {
      sourcePath: 'payload.seasonal_roadmap.unsmoothed.full_year',
      datasetName: roadmap.method?.version || 'robust_daily_returns_v3',
      valueKey: 'price',
    }
  }
  return { sourcePath: null, datasetName: roadmap.method?.version || 'robust_daily_returns_v3' }
}

export function defaultSeasonalView(displayDefaults) {
  return displayDefaults?.seasonal_view || 'roadmap'
}
