/**
 * Seasonal Roadmap view helpers (UI only — no methodology changes).
 */

export const ROADMAP_METHOD_LABEL = 'Seasonal Roadmap'

export const ROADMAP_METHOD_DESCRIPTION =
  'Average normalised historical yearly price path, rebased to the current price.'

export const ROADMAP_HORIZON_WEEKS = [4, 8, 12, 26, 48]

/**
 * Directional class from existing forecast_stats fields only.
 * Bullish: mean > 0, median > 0, bullish_frequency > 0.5
 * Bearish: mean < 0, median < 0, bearish_frequency > 0.5
 * Mixed otherwise.
 */
export function classifyRoadmapHorizon(row) {
  if (!row || row.n == null || row.n <= 0) return 'Mixed'
  const mean = row.mean
  const median = row.median
  const bull = row.bullish_frequency
  const bear = row.bearish_frequency
  if (
    mean != null &&
    median != null &&
    bull != null &&
    mean > 0 &&
    median > 0 &&
    bull > 0.5
  ) {
    return 'Bullish'
  }
  if (
    mean != null &&
    median != null &&
    bear != null &&
    mean < 0 &&
    median < 0 &&
    bear > 0.5
  ) {
    return 'Bearish'
  }
  return 'Mixed'
}

/**
 * Which Roadmap series the chart binds when the smooth toggle changes.
 * Does not touch Mean-return or Freeze packs.
 */
export function resolveRoadmapSeriesSource(roadmap, useSmoothed) {
  if (!roadmap?.available) {
    return { sourcePath: null, datasetName: null }
  }
  if (useSmoothed && roadmap.smoothed?.full_year?.length) {
    return {
      sourcePath: 'payload.seasonal_roadmap.smoothed.full_year',
      datasetName: roadmap.method?.version || 'seasonal_roadmap_v1',
      valueKey: 'price',
    }
  }
  if (roadmap.unsmoothed?.full_year?.length) {
    return {
      sourcePath: 'payload.seasonal_roadmap.unsmoothed.full_year',
      datasetName: roadmap.method?.version || 'seasonal_roadmap_v1',
      valueKey: 'price',
    }
  }
  return { sourcePath: null, datasetName: roadmap.method?.version || 'seasonal_roadmap_v1' }
}

export function defaultSeasonalView(displayDefaults) {
  return displayDefaults?.seasonal_view || 'roadmap'
}
