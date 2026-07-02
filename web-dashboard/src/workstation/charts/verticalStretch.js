/** Master vertical stretch — independent from horizontal barSpacing camera. */

export const VERTICAL_STRETCH_DEFAULTS = {
  factor: 1,
  minFactor: 0.25,
  maxFactor: 16,
  zoomFactor: 1.12,
}

export function clampVerticalStretch(factor) {
  if (!Number.isFinite(factor)) return VERTICAL_STRETCH_DEFAULTS.factor
  return Math.max(
    VERTICAL_STRETCH_DEFAULTS.minFactor,
    Math.min(VERTICAL_STRETCH_DEFAULTS.maxFactor, factor),
  )
}

export function zoomVerticalStretch(current, zoomIn, { intensity = 1 } = {}) {
  const base = clampVerticalStretch(current ?? VERTICAL_STRETCH_DEFAULTS.factor)
  const steps = Math.max(1, intensity)
  const mult = Math.pow(VERTICAL_STRETCH_DEFAULTS.zoomFactor, steps)
  const next = zoomIn ? base * mult : base / mult
  return clampVerticalStretch(next)
}

export function verticalStretchEqual(a, b, epsilon = 0.02) {
  return Math.abs((a ?? 1) - (b ?? 1)) < epsilon
}

/** Drag price axis up (negative deltaY) → taller drawings; down → flatter. */
export function magnifyByAxisDrag(current, deltaYPixels) {
  if (!deltaYPixels) return clampVerticalStretch(current ?? VERTICAL_STRETCH_DEFAULTS.factor)
  const base = clampVerticalStretch(current ?? VERTICAL_STRETCH_DEFAULTS.factor)
  const sensitivity = 0.008
  const next = base * Math.exp(-deltaYPixels * sensitivity)
  return clampVerticalStretch(next)
}
