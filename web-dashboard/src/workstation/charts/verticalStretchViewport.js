import { clampVerticalStretch, VERTICAL_STRETCH_DEFAULTS } from './verticalStretch.js'
import { PANEL_IDS } from '../../charts/chartTheme.js'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

/** Shrink visible price range around its center — drawings render taller. */
export function createVerticalStretchAutoscaleProvider(stretchFactor) {
  const factor = clampVerticalStretch(stretchFactor)
  return (baseImplementation) => {
    const info = baseImplementation()
    if (!info?.priceRange) return info
    if (Math.abs(factor - VERTICAL_STRETCH_DEFAULTS.factor) < 0.005) return info

    const { minValue, maxValue } = info.priceRange
    if (!isNum(minValue) || !isNum(maxValue)) return info
    const span = maxValue - minValue
    if (span <= 0) return info

    const center = (minValue + maxValue) / 2
    const half = span / (2 * factor)
    return {
      ...info,
      priceRange: {
        minValue: center - half,
        maxValue: center + half,
      },
    }
  }
}

export function applyVerticalStretchToSeries(series, stretchFactor) {
  if (!series) return
  try {
    series.applyOptions({
      autoscaleInfoProvider: createVerticalStretchAutoscaleProvider(stretchFactor),
    })
  } catch {
    /* ignore stale series */
  }
}

export function applyVerticalStretchToPane(pane, stretchFactor) {
  if (!pane || pane.syncOnly) return
  applyVerticalStretchToSeries(pane.primarySeries, stretchFactor)
}

export function magnificationFactorForPane(panelId, { priceFactor, cotFactor }) {
  if (panelId === PANEL_IDS.price) return priceFactor
  return cotFactor
}

export function applyVerticalMagnificationToPane(pane, { priceFactor, cotFactor }) {
  if (!pane || pane.syncOnly) return
  const factor = magnificationFactorForPane(pane.panelId, { priceFactor, cotFactor })
  applyVerticalStretchToSeries(pane.primarySeries, factor)
}

export function applyVerticalMagnificationToPanes(panes, { priceFactor, cotFactor }) {
  if (!panes?.size) return
  for (const pane of panes.values()) {
    applyVerticalMagnificationToPane(pane, { priceFactor, cotFactor })
  }
}

/** @deprecated use applyVerticalMagnificationToPanes */
export function applyVerticalStretchToPanes(panes, stretchFactor) {
  applyVerticalMagnificationToPanes(panes, {
    priceFactor: stretchFactor,
    cotFactor: stretchFactor,
  })
}

/** Pixel height of the visible price span — used for stretch verification. */
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
