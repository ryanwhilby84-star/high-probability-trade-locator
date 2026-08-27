import { clampVerticalStretch, VERTICAL_STRETCH_DEFAULTS } from './verticalStretch.js'
import { PANEL_IDS } from '../../charts/chartTheme.js'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function breathMarginsPx(panelId, stretchFactor) {
  const factor = clampVerticalStretch(stretchFactor)
  // Price sits in a compact top pane. Keep its breathing room small and constant
  // so a manual vertical stretch actually magnifies the candles instead of being
  // swallowed by fixed pixel padding. COT panes are tall, so they keep the larger,
  // factor-scaled margins that give the "best version so far" look.
  if (panelId === PANEL_IDS.price) {
    return { above: 8, below: 8 }
  }
  const scaled = 22 + Math.max(0, factor - 1) * 4
  const px = Math.round(Math.min(48, scaled))
  return { above: px, below: px }
}

/**
 * Independent per-pane vertical camera.
 *
 * A pane's vertical camera is `{ factor, panOffset }`:
 *   - `factor`   — Y zoom/stretch around the visible center (1 = native fit).
 *   - `panOffset`— additive Y shift in price units (moves the line up/down).
 *
 * At `factor === 1` and `panOffset === 0` the provider returns the chart's native
 * autoscale, which Lightweight Charts computes from the currently visible time
 * range only — i.e. this is exactly "Fit Y for the visible date range". Because
 * each pane owns its own provider instance, adjusting one pane never touches the
 * Y range of any other pane.
 */
export function createVerticalCameraAutoscaleProvider({
  stretchFactor,
  panOffset = 0,
  panelId = null,
}) {
  const factor = clampVerticalStretch(stretchFactor)
  const stretched = Math.abs(factor - VERTICAL_STRETCH_DEFAULTS.factor) >= 0.005
  const shifted = Boolean(panOffset)

  return (baseImplementation) => {
    const info = baseImplementation()
    if (!info?.priceRange) return info

    // Default camera → hand back the native visible-range autoscale untouched.
    if (!stretched && !shifted) return info

    let { minValue, maxValue } = info.priceRange
    if (!isNum(minValue) || !isNum(maxValue)) return info
    let span = maxValue - minValue
    if (span <= 0) return info

    if (stretched) {
      const center = (minValue + maxValue) / 2
      const half = span / (2 * factor)
      minValue = center - half
      maxValue = center + half
      span = maxValue - minValue
    }

    if (shifted) {
      minValue += panOffset
      maxValue += panOffset
    }

    return {
      ...info,
      priceRange: { minValue, maxValue },
      margins: breathMarginsPx(panelId, factor),
    }
  }
}

export function applyVerticalCameraToSeries(
  series,
  { stretchFactor, panOffset = 0, panelId = null },
) {
  if (!series) return
  try {
    series.applyOptions({
      autoscaleInfoProvider: createVerticalCameraAutoscaleProvider({
        stretchFactor,
        panOffset,
        panelId,
      }),
    })
  } catch {
    /* ignore stale series */
  }
}

/** Apply one pane's independent vertical camera to its primary series. */
export function applyVerticalCameraToPane(pane, camera) {
  if (!pane || pane.syncOnly || !camera) return
  applyVerticalCameraToSeries(pane.primarySeries, {
    stretchFactor: camera.factor,
    panOffset: camera.panOffset ?? 0,
    panelId: pane.panelId,
  })
}

/** Pixel height of the visible price span — used for stretch verification/diagnostics. */
export function readPaneVerticalSpanPx(pane) {
  const chart = pane?.chart
  const series = pane?.primarySeries
  if (!chart || !series) return null
  try {
    const chartEl = chart.chartElement?.()
    const height = chartEl?.clientHeight ?? 0
    if (height < 20) return null

    const yPad = Math.round(height * 0.12)
    const yTop = yPad
    const yBottom = height - yPad
    const priceTop = series.coordinateToPrice(yTop)
    const priceBottom = series.coordinateToPrice(yBottom)
    if (priceTop == null || priceBottom == null) return null

    const min = Math.min(priceTop, priceBottom)
    const max = Math.max(priceTop, priceBottom)
    const visiblePriceSpan = max - min
    if (visiblePriceSpan <= 0) return null

    return {
      spanPx: yBottom - yTop,
      visiblePriceSpan,
      min,
      max,
    }
  } catch {
    return null
  }
}
