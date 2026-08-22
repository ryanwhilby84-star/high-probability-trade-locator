from pathlib import Path

path = Path("web-dashboard/src/workstation/charts/WorkstationChartPane.jsx")
text = path.read_text(encoding="utf-8")
if "isPriceCameraPane" in text:
    print("already patched")
    raise SystemExit(0)

old_imports = """import React from 'react'
import { createChart } from 'lightweight-charts'

import { cotDateToBarTime } from '../../charts/positioningTimelineAlign.js'
import {
  prepareLightweightCandles,
  prepareLightweightLinePoints,
} from '../data/prepareLightweightCandles.js'
import { createWorkstationChartOptions } from './workstationChartOptions.js'
import { WorkstationLwcDrawingOverlay } from './WorkstationLwcDrawingOverlay.jsx'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)
"""

new_imports = """import React from 'react'
import { CrosshairMode, createChart } from 'lightweight-charts'

import { PANEL_IDS } from '../../charts/chartTheme.js'
import { cotDateToBarTime } from '../../charts/positioningTimelineAlign.js'
import {
  prepareLightweightCandles,
  prepareLightweightLinePoints,
} from '../data/prepareLightweightCandles.js'
import { createWorkstationChartOptions } from './workstationChartOptions.js'
import { WorkstationLwcDrawingOverlay } from './WorkstationLwcDrawingOverlay.jsx'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

/** TradingView-style camera on valuation price panes only (not deviation / sync followers). */
function isPriceCameraPane(panelId, syncFollower) {
  return panelId === PANEL_IDS.price && !syncFollower
}

const CAMERA_BTN_STYLE = {
  appearance: 'none',
  border: '1px solid rgba(148, 163, 184, 0.35)',
  background: 'rgba(15, 23, 42, 0.82)',
  color: '#e2e8f0',
  fontSize: 11,
  fontWeight: 600,
  letterSpacing: '0.02em',
  lineHeight: 1.2,
  padding: '4px 8px',
  borderRadius: 4,
  cursor: 'pointer',
}

const CAMERA_BAR_STYLE = {
  position: 'absolute',
  top: 8,
  right: 64,
  zIndex: 5,
  display: 'flex',
  gap: 6,
  pointerEvents: 'auto',
}
"""

old_start = """  className = '',
}) {
  const containerRef = React.useRef(null)
  const chartRef = React.useRef(null)
  const [chartInstance, setChartInstance] = React.useState(null)
  const [primarySeriesInstance, setPrimarySeriesInstance] = React.useState(null)
  const primaryRef = React.useRef(null)
  const livePriceLineRef = React.useRef(null)
  const liveDeviationLineRef = React.useRef(null)
  const anchorRef = React.useRef(null)
  const zeroRef = React.useRef(null)
  const overlayRef = React.useRef(null)
  const guideLinesRef = React.useRef([])
  const candlesRef = React.useRef([])
  const lineRef = React.useRef([])
  const skipEmitRef = React.useRef(false)
  const onChartClickRef = React.useRef(onChartClick)
  onChartClickRef.current = onChartClick

  const onCrosshairMoveRef = React.useRef(onCrosshairMove)
  const onCrosshairClearRef = React.useRef(onCrosshairClear)
  onCrosshairMoveRef.current = onCrosshairMove
  onCrosshairClearRef.current = onCrosshairClear

  React.useEffect(() => {
    const el = containerRef.current
    if (!el) return undefined

    let chart
    try {
      const initialWidth = Math.max(el.clientWidth, 1)
      const initialHeight = Math.max(el.clientHeight, 1)
      chart = createChart(
        el,
        createWorkstationChartOptions({
          width: initialWidth,
          height: initialHeight,
          showTimeAxis,
          interactionEnabled: !syncFollower,
        }),
      )
      if (hideFloatingLabels) {
        chart.applyOptions({
          crosshair: {
            horzLine: { labelVisible: false },
            vertLine: { labelVisible: showTimeAxis },
          },
        })
      }
      if (transparentBackground) {
        chart.applyOptions({
          layout: { background: { color: 'transparent' } },
          grid: {
            vertLines: { color: 'rgba(148, 163, 184, 0.08)' },
            horzLines: { color: 'rgba(148, 163, 184, 0.1)' },
          },
        })
      }
    } catch (err) {
      console.error('[workstation] pane createChart failed', panelId, err)
      return undefined
    }
"""

new_start = """  className = '',
}) {
  const priceCamera = isPriceCameraPane(panelId, syncFollower)
  const containerRef = React.useRef(null)
  const chartRef = React.useRef(null)
  const [chartInstance, setChartInstance] = React.useState(null)
  const [primarySeriesInstance, setPrimarySeriesInstance] = React.useState(null)
  const primaryRef = React.useRef(null)
  const livePriceLineRef = React.useRef(null)
  const liveDeviationLineRef = React.useRef(null)
  const anchorRef = React.useRef(null)
  const zeroRef = React.useRef(null)
  const overlayRef = React.useRef(null)
  const guideLinesRef = React.useRef([])
  const candlesRef = React.useRef([])
  const lineRef = React.useRef([])
  const skipEmitRef = React.useRef(false)
  const homeLogicalRangeRef = React.useRef(null)
  const homeCaptureTimerRef = React.useRef(null)
  const lastSizeRef = React.useRef({ width: 0, height: 0 })
  const onChartClickRef = React.useRef(onChartClick)
  onChartClickRef.current = onChartClick

  const onCrosshairMoveRef = React.useRef(onCrosshairMove)
  const onCrosshairClearRef = React.useRef(onCrosshairClear)
  onCrosshairMoveRef.current = onCrosshairMove
  onCrosshairClearRef.current = onCrosshairClear

  const resetPriceCamera = React.useCallback(() => {
    const chart = chartRef.current
    if (!chart) return
    const home = homeLogicalRangeRef.current
    try {
      if (home && Number.isFinite(home.from) && Number.isFinite(home.to)) {
        chart.timeScale().setVisibleLogicalRange({ from: home.from, to: home.to })
      } else {
        chart.timeScale().fitContent()
      }
      chart.priceScale('right').applyOptions({ autoScale: true })
    } catch {
      /* ignore */
    }
  }, [])

  const returnPriceCameraToLive = React.useCallback(() => {
    const chart = chartRef.current
    if (!chart) return
    try {
      chart.timeScale().scrollToRealTime()
      chart.priceScale('right').applyOptions({ autoScale: true })
    } catch {
      /* ignore */
    }
  }, [])

  React.useEffect(() => {
    const el = containerRef.current
    if (!el) return undefined

    let chart
    try {
      const initialWidth = Math.max(el.clientWidth, 1)
      const initialHeight = Math.max(el.clientHeight, 1)
      lastSizeRef.current = { width: initialWidth, height: initialHeight }
      chart = createChart(
        el,
        createWorkstationChartOptions({
          width: initialWidth,
          height: initialHeight,
          showTimeAxis,
          interactionEnabled: !syncFollower,
        }),
      )
      if (hideFloatingLabels && !priceCamera) {
        chart.applyOptions({
          crosshair: {
            horzLine: { labelVisible: false },
            vertLine: { labelVisible: showTimeAxis },
          },
        })
      }
      if (transparentBackground) {
        chart.applyOptions({
          layout: { background: { color: 'transparent' } },
          grid: {
            vertLines: { color: 'rgba(148, 163, 184, 0.08)' },
            horzLines: { color: 'rgba(148, 163, 184, 0.1)' },
          },
        })
      }
      if (priceCamera) {
        chart.applyOptions({
          handleScroll: {
            mouseWheel: false,
            pressedMouseMove: true,
            horzTouchDrag: true,
            vertTouchDrag: false,
          },
          handleScale: {
            mouseWheel: true,
            pinch: true,
            axisPressedMouseMove: { time: true, price: true },
            axisDoubleClickReset: { time: true, price: true },
          },
          kineticScroll: {
            touch: true,
            mouse: true,
          },
          crosshair: {
            mode: CrosshairMode.Normal,
            vertLine: { visible: true, labelVisible: true },
            horzLine: { visible: true, labelVisible: true },
          },
          rightPriceScale: { autoScale: true },
        })
      }
    } catch (err) {
      console.error('[workstation] pane createChart failed', panelId, err)
      return undefined
    }
"""

old_ro = """    const ro = new ResizeObserver(() => {
      if (!containerRef.current || !chartRef.current) return
      const { clientWidth, clientHeight } = containerRef.current
      chartRef.current.applyOptions({
        width: Math.max(clientWidth, 1),
        height: Math.max(clientHeight, 1),
      })
    })
    ro.observe(el)

    const unregister =
"""

new_ro = """    const ro = new ResizeObserver(() => {
      if (!containerRef.current || !chartRef.current) return
      const { clientWidth, clientHeight } = containerRef.current
      const width = Math.max(clientWidth, 1)
      const height = Math.max(clientHeight, 1)
      const prev = lastSizeRef.current
      if (prev.width === width && prev.height === height) return
      lastSizeRef.current = { width, height }
      chartRef.current.applyOptions({ width, height })
    })
    ro.observe(el)

    if (priceCamera) {
      homeCaptureTimerRef.current = window.setTimeout(() => {
        try {
          const range = chart.timeScale().getVisibleLogicalRange()
          if (range && Number.isFinite(range.from) && Number.isFinite(range.to)) {
            homeLogicalRangeRef.current = { from: range.from, to: range.to }
          }
        } catch {
          /* ignore */
        }
      }, 900)
    }

    const unregister =
"""

old_cleanup = """    return () => {
      ro.disconnect()
      try {
        chart.unsubscribeClick(onClick)
      } catch {
        /* ignore */
      }
      unregister()
      chart.remove()
      chartRef.current = null
      primaryRef.current = null
      anchorRef.current = null
      zeroRef.current = null
      overlayRef.current = null
      guideLinesRef.current = []
      setChartInstance(null)
      setPrimarySeriesInstance(null)
    }
  }, [
    panelId,
    mode,
    showTimeAxis,
    lineColor,
    lineWidth,
    zeroLine,
    registerPane,
    symmetricZero,
    overlayLineColor,
    hideFloatingLabels,
    transparentBackground,
    fixedPriceRange?.min,
    fixedPriceRange?.max,
    syncFollower,
  ])
"""

new_cleanup = """    return () => {
      ro.disconnect()
      if (homeCaptureTimerRef.current != null) {
        window.clearTimeout(homeCaptureTimerRef.current)
        homeCaptureTimerRef.current = null
      }
      try {
        chart.unsubscribeClick(onClick)
      } catch {
        /* ignore */
      }
      unregister()
      chart.remove()
      chartRef.current = null
      primaryRef.current = null
      anchorRef.current = null
      zeroRef.current = null
      overlayRef.current = null
      guideLinesRef.current = []
      homeLogicalRangeRef.current = null
      setChartInstance(null)
      setPrimarySeriesInstance(null)
    }
  }, [
    panelId,
    mode,
    showTimeAxis,
    lineColor,
    lineWidth,
    zeroLine,
    registerPane,
    symmetricZero,
    overlayLineColor,
    hideFloatingLabels,
    transparentBackground,
    fixedPriceRange?.min,
    fixedPriceRange?.max,
    syncFollower,
    priceCamera,
  ])
"""

old_return = """  return (
    <div
      className={`ws-chart-pane ${drawingMode ? 'ws-chart-pane--drawing' : ''} ${className}`.trim()}
      data-panel={panelId}
    >
      <div className="ws-chart-pane-plot ws-chart-pane-plot--overlay-host">
        <div className="ws-chart-pane-canvas" ref={containerRef} />
        <WorkstationLwcDrawingOverlay
"""

new_return = """  return (
    <div
      className={`ws-chart-pane ${drawingMode ? 'ws-chart-pane--drawing' : ''} ${className}`.trim()}
      data-panel={panelId}
      data-price-camera={priceCamera ? 'on' : undefined}
    >
      <div
        className="ws-chart-pane-plot ws-chart-pane-plot--overlay-host"
        style={priceCamera ? { position: 'relative' } : undefined}
      >
        <div className="ws-chart-pane-canvas" ref={containerRef} />
        {priceCamera ? (
          <div
            className="ws-price-camera-controls"
            style={CAMERA_BAR_STYLE}
            data-testid="ws-price-camera-controls"
          >
            <button
              type="button"
              style={CAMERA_BTN_STYLE}
              data-testid="ws-price-camera-reset"
              title="Reset camera to the default window and auto Y scale"
              onClick={resetPriceCamera}
            >
              Reset Camera
            </button>
            <button
              type="button"
              style={CAMERA_BTN_STYLE}
              data-testid="ws-price-camera-live"
              title="Scroll to the live / latest bars"
              onClick={returnPriceCameraToLive}
            >
              Return to Live
            </button>
          </div>
        ) : null}
        <WorkstationLwcDrawingOverlay
"""

for name, old, new in [
    ("imports", old_imports, new_imports),
    ("start", old_start, new_start),
    ("ro", old_ro, new_ro),
    ("cleanup", old_cleanup, new_cleanup),
    ("return", old_return, new_return),
]:
    if old not in text:
        raise SystemExit(f"block not found: {name}")
    text = text.replace(old, new, 1)

path.write_text(text, encoding="utf-8")
print("patched ok")
