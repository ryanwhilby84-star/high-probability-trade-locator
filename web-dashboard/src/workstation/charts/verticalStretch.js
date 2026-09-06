/** Master vertical stretch — independent from horizontal barSpacing camera. */

export const VERTICAL_STRETCH_DEFAULTS = {
  factor: 1,
  // Do not allow indicator panes to be compressed into a visually flat line.
  // 0.7 still gives useful breathing room while preserving readable structure.
  minFactor: 0.7,
  // Enough magnification for close inspection without making axis drag unstable.
  maxFactor: 6,
  zoomFactor: 1.1,
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
  // Deliberately damped: a normal mouse drag should refine the scale rather than
  // jump from readable to extreme compression/magnification in one movement.
  const sensitivity = 0.0045
  const next = base * Math.exp(-deltaYPixels * sensitivity)
  return clampVerticalStretch(next)
}
