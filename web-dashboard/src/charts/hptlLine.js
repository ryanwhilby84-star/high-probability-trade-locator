/**
 * HPTL global chart line rule — weekly/discrete data, no curve smoothing.
 * Recharts: type="linear" only (straight segments between observations).
 */

/** @type {'linear'} */
export const HPTL_LINE_TYPE = 'linear'

/** Default props for Recharts <Line /> — no monotone/bezier/spline. */
export const HPTL_LINE_PROPS = {
  type: HPTL_LINE_TYPE,
  isAnimationActive: false,
}

/** Merge HPTL line defaults with component-specific props. */
export function hptlLineProps(overrides = {}) {
  return { ...HPTL_LINE_PROPS, ...overrides }
}
