/** Default and bounds for synchronized workstation panel plot areas. */

export const WS_PRICE_PLOT_HEIGHT = 600
export const WS_PRICE_PLOT_MIN = 400

export const WS_COT_PLOT_HEIGHT = 420
export const WS_COT_RETAIL_PLOT_HEIGHT = 448
export const WS_COT_PLOT_MIN = 280
export const WS_COT_PLOT_MAX = 960

/**
 * Single-surface proportions for the fullscreen fitted layout.
 *
 * Institutional hierarchy: the COT positioning panes are the primary product and
 * own the majority of the surface; price is a compact market-context viewport at
 * the top. Price ≈ 29%; the COT group ≈ 71%, split equally across the 3 COT panes
 * (each ≈ 23–24%). When OHLC is unavailable the price pane collapses further so
 * the COT panes own the whole surface.
 *
 * These unitless flex ratios are the single sizing lever. `CotWorkstation` holds
 * the live price ratio in state and publishes it to the root element as CSS custom
 * properties (`--ws-price-flex`, `--ws-cot-group-flex = 100 - price`); the fitted
 * CSS consumes them and the three COT panes (`flex: 1 1 0`) always redistribute the
 * remaining height evenly. The draggable splitter only mutates the price ratio
 * (clamped to WS_PANE_FLEX_BOUNDS) — no layout rewrite, shared camera untouched,
 * and a future "reset layout" simply restores `WS_PANE_FLEX.price`.
 */
export const WS_PANE_FLEX = {
  price: 29,
  cotGroup: 71,
  priceNoOhlc: 10,
}

/** Clamp bounds for the draggable price/COT divider. */
export const WS_PANE_FLEX_BOUNDS = {
  priceMin: 15,
  priceMax: 60,
}

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
