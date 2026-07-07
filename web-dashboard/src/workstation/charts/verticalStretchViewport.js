import { clampVerticalStretch, VERTICAL_STRETCH_DEFAULTS } from './verticalStretch.js'
import { PANEL_IDS } from '../../charts/chartTheme.js'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function breathMarginsPx(panelId, stretchFactor) {
  const factor = clampVerticalStretch(stretchFactor)
  const isPrice = panelId === PANEL_IDS.price
  const base = isPrice ? 36 : 22
  const scaled = base + Math.max(0, factor - 1) * (isPrice ? 6 : 4)
  const px = Math.round(Math.min(isPrice ? 72 : 48, scaled))
  return { above: px, below: px }
}

/** Shrink visible price range around its center — drawings render taller. */
export function createVerticalStretchAutoscaleProvider(stretchFactor, panelId = null) {
  const factor = clampVerticalStretch(stretchFactor)
  return (baseImplementation) => {
    const info = baseImplementation()
    if (!info?.priceRange) return info

    const { minValue, maxValue } = info.priceRange
    if (!isNum(minValue) || !isNum(maxValue)) return info
    const span = maxValue - minValue
    if (span <= 0) return info

    if (Math.abs(factor - VERTICAL_STRETCH_DEFAULTS.factor) < 0.005) {
      return info
    }

    const center = (minValue + maxValue) / 2
    const half = span / (2 * factor)
    const margins = breathMarginsPx(panelId, factor)
    return {
      ...info,
      priceRange: {
        minValue: center - half,
        maxValue: center + half,
      },
      margins,
    }
  }
}

/** Price-only composition — vertical stretch + in-panel vertical pan offset. */
export function createPriceCompositionAutoscaleProvider({ stretchFactor, panOffset = 0 }) {
  const factor = clampVerticalStretch(stretchFactor)
  return (baseImplementation) => {
    const info = baseImplementation()
    if (!info?.priceRange) return info

    let { minValue, maxValue } = info.priceRange
    if (!isNum(minValue) || !isNum(maxValue)) return info
    let span = maxValue - minValue
    if (span <= 0) return info

    if (Math.abs(factor - VERTICAL_STRETCH_DEFAULTS.factor) >= 0.005) {
      const center = (minValue + maxValue) / 2
      const half = span / (2 * factor)
      minValue = center - half
      maxValue = center + half
      span = maxValue - minValue
    }

    if (panOffset !== 0) {
      minValue += panOffset
      maxValue += panOffset
    }

    const margins = breathMarginsPx(PANEL_IDS.price, factor)
    return {
      ...info,
      priceRange: { minValue, maxValue },
      margins,
    }
  }
}

export function applyVerticalStretchToSeries(series, stretchFactor, panelId = null) {
  if (!series) return
  try {
    series.applyOptions({
      autoscaleInfoProvider: createVerticalStretchAutoscaleProvider(stretchFactor, panelId),
    })
  } catch {
    /* ignore stale series */
  }
}

export function applyPriceCompositionToSeries(series, { stretchFactor, panOffset = 0 }) {
  if (!series) return
  try {
    series.applyOptions({
      autoscaleInfoProvider: createPriceCompositionAutoscaleProvider({
        stretchFactor,
        panOffset,
      }),
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

export function applyVerticalMagnificationToPane(
  pane,
  { priceFactor, cotFactor, pricePanOffset = 0 },
) {
  if (!pane || pane.syncOnly) return
  if (pane.panelId === PANEL_IDS.price) {
    applyPriceCompositionToSeries(pane.primarySeries, {
      stretchFactor: priceFactor,
      panOffset: pricePanOffset,
    })
    return
  }
  const factor = magnificationFactorForPane(pane.panelId, { priceFactor, cotFactor })
  applyVerticalStretchToSeries(pane.primarySeries, factor, pane.panelId)
}

export function applyVerticalMagnificationToPanes(
  panes,
  { priceFactor, cotFactor, pricePanOffset = 0 },
) {
  if (!panes?.size) return
  for (const pane of panes.values()) {
    applyVerticalMagnificationToPane(pane, { priceFactor, cotFactor, pricePanOffset })
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
