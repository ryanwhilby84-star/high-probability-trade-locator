/** Master camera — continuous horizontal stretch (barSpacing + rightOffset). */

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

export const CAMERA_DEFAULTS = {
  rightOffset: 8,
  minBarSpacing: 0.35,
  zoomFactor: 1.12,
  defaultBarSpacing: 6,
}

export function maxBarSpacingForPlot(plotWidth) {
  const plot = Math.max(plotWidth, 80)
  return plot * 2
}

export function timeToIndex(rows, time) {
  if (!rows?.length || !isNum(time)) return 0
  let best = 0
  let bestDist = Infinity
  for (let i = 0; i < rows.length; i += 1) {
    const t = rows[i]?.time
    if (!isNum(t)) continue
    const d = Math.abs(t - time)
    if (d < bestDist) {
      bestDist = d
      best = i
    }
  }
  return best
}

export function indexToTime(rows, index) {
  if (!rows?.length) return null
  const i = Math.max(0, Math.min(rows.length - 1, Math.round(index)))
  return rows[i]?.time ?? null
}

export function barSpacingForWeekCount(plotWidth, weekCount) {
  const count = Math.max(weekCount, 1)
  const plot = Math.max(plotWidth, 80)
  return Math.max(CAMERA_DEFAULTS.minBarSpacing, plot / count)
}

export function cameraForWeekWindow(rows, weeks, plotWidth) {
  if (!rows?.length) return null
  const span = weeks == null || weeks >= rows.length ? rows.length : weeks
  return {
    barSpacing: barSpacingForWeekCount(plotWidth, span),
    rightOffset: CAMERA_DEFAULTS.rightOffset,
  }
}

export function cameraShowAll(rows, plotWidth) {
  if (!rows?.length) return null
  return {
    barSpacing: barSpacingForWeekCount(plotWidth, rows.length),
    rightOffset: CAMERA_DEFAULTS.rightOffset,
  }
}

export function clampStretchCamera(camera, plotWidth = null) {
  if (!camera) return camera
  const plot = Math.max(plotWidth ?? 800, 80)
  const maxSpacing = maxBarSpacingForPlot(plot)
  return {
    barSpacing: Math.max(
      CAMERA_DEFAULTS.minBarSpacing,
      Math.min(maxSpacing, camera.barSpacing),
    ),
    rightOffset: camera.rightOffset ?? CAMERA_DEFAULTS.rightOffset,
  }
}

/** Drag surface right → pan earlier through history at fixed magnification. */
export function panCameraByPixels(camera, deltaXPixels) {
  if (!camera || !deltaXPixels) return camera
  const offsetDelta = -deltaXPixels / camera.barSpacing
  return {
    ...camera,
    rightOffset: camera.rightOffset + offsetDelta,
  }
}

/**
 * Wheel zoom — increase/decrease barSpacing only; adjust rightOffset so the
 * point under the cursor stays fixed (TradingView-style side stretch).
 */
export function zoomCameraAtPixel(
  camera,
  rows,
  plotWidth,
  chartWidth,
  plotX,
  zoomIn,
  { intensity = 1, baseIndex = null } = {},
) {
  if (!camera || !rows?.length) return camera
  const plot = Math.max(plotWidth, 80)
  const width = Math.max(chartWidth, plot)
  const lastIdx = baseIndex ?? rows.length - 1

  const ratio = Math.max(0, Math.min(1, plotX / plot))
  const visibleBars = width / camera.barSpacing
  const anchorIdx = lastIdx + camera.rightOffset - (1 - ratio) * visibleBars

  const steps = Math.max(1, intensity)
  const factor = Math.pow(CAMERA_DEFAULTS.zoomFactor, steps)
  const nextSpacing = zoomIn
    ? Math.min(maxBarSpacingForPlot(plot), camera.barSpacing * factor)
    : Math.max(CAMERA_DEFAULTS.minBarSpacing, camera.barSpacing / factor)

  const deltaFromRight = (width - plotX - 1) / nextSpacing
  const nextRightOffset = deltaFromRight - lastIdx + anchorIdx - 0.5

  return clampStretchCamera(
    { barSpacing: nextSpacing, rightOffset: nextRightOffset },
    plot,
  )
}

export function camerasEqual(a, b, epsilon = 0.02) {
  if (!a || !b) return false
  return (
    Math.abs(a.barSpacing - b.barSpacing) < epsilon &&
    Math.abs(a.rightOffset - b.rightOffset) < epsilon
  )
}

/** @deprecated use stretch camera fields directly */
export function cameraLogicalRange(camera, rows) {
  if (!camera || !rows?.length) return { from: 0, to: rows.length - 1 }
  const lastIdx = rows.length - 1
  const plotBars = 800 / camera.barSpacing
  const to = lastIdx + camera.rightOffset
  const from = to - plotBars
  return { from, to }
}

/** @deprecated */
export function clampCamera(camera, rows, plotWidth = null) {
  return clampStretchCamera(camera, plotWidth)
}
