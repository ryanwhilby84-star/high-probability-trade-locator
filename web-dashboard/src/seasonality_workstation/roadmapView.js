/** Seasonal Roadmap view helpers (UI only). */
export const ROADMAP_METHOD_LABEL = 'Seasonal Roadmap'
export const ROADMAP_METHOD_DESCRIPTION =
  'Historical daily moves build the plotted roadmap. Forward lookback statistics are a separate same-ISO-week study: one observation per year, genuine completed weekly closes only, with incomplete or gapped horizons rejected.'

export const ROADMAP_HORIZON_WEEKS = [1, 2, 4, 8, 12]

export function classifyRoadmapHorizon(row) {
  if (!row || row.n == null || row.n <= 0) return 'Mixed'
  if (row.direction === 'Bullish' || row.direction === 'Bearish' || row.direction === 'Mixed') {
    return row.direction
  }
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
      datasetName: roadmap.method?.version || 'volatility_normalised_daily_texture_v4',
      valueKey: 'price',
    }
  }
  if (roadmap.unsmoothed?.full_year?.length) {
    return {
      sourcePath: 'payload.seasonal_roadmap.unsmoothed.full_year',
      datasetName: roadmap.method?.version || 'volatility_normalised_daily_texture_v4',
      valueKey: 'price',
    }
  }
  return { sourcePath: null, datasetName: roadmap.method?.version || 'volatility_normalised_daily_texture_v4' }
}

export function defaultSeasonalView(displayDefaults) {
  return displayDefaults?.seasonal_view || 'roadmap'
}
