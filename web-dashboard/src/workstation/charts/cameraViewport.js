import { WS_PRICE_SCALE_WIDTH } from './workstationChartOptions.js'

/** Push stretch camera — barSpacing + rightOffset only (no range refit). */
export function applyCameraToChart(chart, camera) {
  if (!chart || !camera) return
  try {
    chart.timeScale().applyOptions({
      barSpacing: camera.barSpacing,
      rightOffset: camera.rightOffset ?? 8,
    })
  } catch {
    /* ignore stale chart */
  }
}

export function applyCameraToPanes(panes, camera) {
  if (!camera) return
  for (const pane of panes.values()) {
    applyCameraToChart(pane.chart, camera)
  }
}

export function readPlotWidthFromChart(chart) {
  try {
    const el = chart?.chartElement?.()
    const width = el?.clientWidth ?? 0
    return Math.max(width - WS_PRICE_SCALE_WIDTH - 20, 80)
  } catch {
    return 800
  }
}

export function readChartWidthFromChart(chart) {
  try {
    const ts = chart?.timeScale?.()
    return ts?.width?.() ?? readPlotWidthFromChart(chart)
  } catch {
    return readPlotWidthFromChart(chart)
  }
}

export function plotXFromClientX(clientX, chart) {
  try {
    const el = chart?.chartElement?.()
    if (!el) return null
    const rect = el.getBoundingClientRect()
    return clientX - rect.left
  } catch {
    return null
  }
}
