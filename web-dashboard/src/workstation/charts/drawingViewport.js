/** Imperative chart viewport — data coords ↔ screen coords. No React state. */

export function createRafCoalescer(fn) {
  let frameId = null
  return () => {
    if (frameId != null) return
    frameId = requestAnimationFrame(() => {
      frameId = null
      fn()
    })
  }
}

/**
 * Bind to a lightweight-charts pane. Repaints on visible-range change + resize only.
 * Does NOT subscribe to crosshair (avoids 60+ React renders/sec while hovering).
 */
export function bindChartViewport(chart, primarySeries, onViewportChange) {
  if (!chart) return () => {}

  const schedule = createRafCoalescer(onViewportChange)
  const timeScale = chart.timeScale()
  timeScale.subscribeVisibleLogicalRangeChange(schedule)

  const el = chart.chartElement?.()
  const ro = el ? new ResizeObserver(schedule) : null
  if (el) ro.observe(el)

  schedule()

  return () => {
    timeScale.unsubscribeVisibleLogicalRangeChange(schedule)
    ro?.disconnect()
  }
}

export function chartTimeToX(chart, time) {
  if (!chart || time == null) return null
  const x = chart.timeScale().timeToCoordinate(time)
  return x == null || Number.isNaN(x) ? null : x
}

export function chartXToTime(chart, clientX) {
  const el = chart?.chartElement?.()
  if (!el) return null
  const rect = el.getBoundingClientRect()
  const x = clientX - rect.left
  return chart.timeScale().coordinateToTime(x)
}

export function seriesValueToY(primarySeries, value) {
  if (!primarySeries || value == null || !Number.isFinite(value)) return null
  const y = primarySeries.priceToCoordinate(value)
  return y == null || Number.isNaN(y) ? null : y
}

export function seriesYToValue(chart, primarySeries, clientY) {
  const el = chart?.chartElement?.()
  if (!el || !primarySeries) return null
  const rect = el.getBoundingClientRect()
  const y = clientY - rect.top
  return primarySeries.coordinateToPrice(y)
}

export function pointerToPanelData(chart, primarySeries, clientX, clientY) {
  const time = chartXToTime(chart, clientX)
  const value = seriesYToValue(chart, primarySeries, clientY)
  return { time, value }
}

/** Stack-relative layout for the global timeline overlay column. */
export function measureGlobalOverlayLayout(referenceChart, stackEl) {
  if (!referenceChart || !stackEl) return null
  const chartEl = referenceChart.chartElement?.()
  if (!chartEl) return null
  const stackRect = stackEl.getBoundingClientRect()
  const chartRect = chartEl.getBoundingClientRect()
  const width = chartRect.width
  const height = stackRect.height
  if (width <= 0 || height <= 0) return null
  return {
    left: chartRect.left - stackRect.left,
    width,
    height,
  }
}
