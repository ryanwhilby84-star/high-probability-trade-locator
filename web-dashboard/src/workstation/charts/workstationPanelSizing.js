/** Default and bounds for synchronized workstation panel plot areas. */

export const WS_PRICE_PLOT_HEIGHT = 520
export const WS_PRICE_PLOT_MIN = 360

export const WS_COT_PLOT_HEIGHT = 400
export const WS_COT_PLOT_MIN = 280
export const WS_COT_PLOT_MAX = 960

export function clampPanelHeight(value, min, max) {
  if (!Number.isFinite(value)) return min
  return Math.min(max, Math.max(min, Math.round(value)))
}

export function defaultCotPanelHeights() {
  return {
    commercial: WS_COT_PLOT_HEIGHT,
    institutional: WS_COT_PLOT_HEIGHT,
    retail: WS_COT_PLOT_HEIGHT,
  }
}
