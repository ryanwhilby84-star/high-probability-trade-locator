/**
 * Canonical Weekly Roadmap frontend contract.
 *
 * Backend emits snake_case key: payload.weekly_roadmap
 * (also nested at payload.seasonality.weekly_roadmap for research packs).
 * Do not invent camelCase aliases in the API.
 */

export const WEEKLY_ROADMAP_CANONICAL_KEY = 'weekly_roadmap'
export const SWS_EQUAL_CHART_HEIGHT = 280

/** Resolve weekly roadmap from a workstation API / research payload. */
export function resolveWeeklyRoadmap(payload) {
  if (!payload || typeof payload !== 'object') return null
  return payload.weekly_roadmap || payload.seasonality?.weekly_roadmap || null
}

/**
 * Chart gate:
 * - missing payload → missing
 * - quality_status === 'unavailable' OR available === false → unavailable
 * - warning/valid with points → render (warnings must not hide the chart)
 */
export function weeklyRoadmapRenderState(weeklyRoadmap) {
  if (weeklyRoadmap == null) {
    return {
      mode: 'missing',
      message: 'Weekly Roadmap missing from API payload (expected payload.weekly_roadmap).',
      reasons: [],
      points: [],
    }
  }
  const points = Array.isArray(weeklyRoadmap.weekly_points)
    ? weeklyRoadmap.weekly_points
    : []
  const quality = weeklyRoadmap.quality_status || null
  const explicitlyUnavailable =
    weeklyRoadmap.available === false || quality === 'unavailable'
  if (explicitlyUnavailable) {
    return {
      mode: 'unavailable',
      message: `Weekly Roadmap unavailable${quality ? ` (${quality})` : ''}.`,
      reasons: weeklyRoadmap.quality_reasons || [],
      points,
    }
  }
  if (!points.length) {
    return {
      mode: 'empty',
      message: 'Weekly Roadmap has no weekly_points to plot.',
      reasons: weeklyRoadmap.quality_reasons || [],
      points,
    }
  }
  return {
    mode: 'ready',
    message: null,
    reasons: weeklyRoadmap.quality_reasons || [],
    points,
  }
}

export function assertEqualChartPanelCount(panelHeights) {
  if (!Array.isArray(panelHeights) || panelHeights.length !== 3) return false
  const [a, b, c] = panelHeights
  return a === b && b === c && a === SWS_EQUAL_CHART_HEIGHT
}
