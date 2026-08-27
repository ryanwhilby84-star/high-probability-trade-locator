import React from 'react'
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

function buildAnchorPoints(timelineRows) {
  return (timelineRows || [])
    .filter((r) => isNum(r.time))
    .map((r) => ({ time: r.time, value: 0 }))
}

function findValueAtTime(points, time) {
  if (!isNum(time) || !points?.length) return null
  const hit = points.find((p) => p.time === time)
  return hit ? hit.value : null
}

function findCandleAtTime(candles, time) {
  if (!isNum(time) || !candles?.length) return null
  return candles.find((c) => c.time === time) || null
}

/**
 * Single synchronized workstation pane (price candlestick or COT line).
 * Registers with the linked timeline controller for shared X zoom + crosshair.
 */
export function WorkstationChartPane({
  panelId,
  mode = 'line',
  showTimeAxis = false,
  lineColor = '#38bdf8',
  lineWidth = 1.75,
  linePoints = [],
  overlayLinePoints = null,
  overlayLineColor = '#f59e0b',
  candleBars = [],
  timelineRows = [],
  zeroLine = false,
  /** Optional horizontal guides: [{ price, color, title?, lineWidth?, lineStyle? }] */
  priceLines = null,
  /** When true, Y-scale stays symmetric about zero (deviation panes). */
  symmetricZero = false,
  /** Fixed Y range (e.g. focus scale −40…+40). */
  fixedPriceRange = null,
  registerPane,
  onCrosshairMove,
  onCrosshairClear,
  externalCrosshairTime = null,
  drawings = [],
  drawingTool = 'select',
  selectedDrawingId = null,
  onSelectDrawing,
  onDrawingCommit,
  drawingMode = false,
  livePrice = null,
  /** Distinct horizontal marker for live valuation deviation (not a historical point). */
  liveDeviationMarker = null,
  /** Hide right-edge last-value / price-line tabs that obscure the oscillator. */
  hideFloatingLabels = false,
  /** Transparent chart canvas so HTML zone layers can show through. */
  transparentBackground = false,
  /** Locked selection time (unix seconds) — draws a shared marker. */
  selectedTime = null,
  /** Overflow markers for clipped focus-scale extremes: [{time, direction}] */
  overflowMarkers = null,
  onChartClick = null,
  /** When true, pane follows shared range/crosshair but does not accept pan/zoom. */
  syncFollower = false,
  className = '',
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

    const anchorSeries = chart.addLineSeries({
      color: 'transparent',
      lineWidth: 0,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: false,
    })

    let primarySeries
    if (mode === 'candle') {
      primarySeries = chart.addCandlestickSeries({
        upColor: '#22c55e',
        downColor: '#ef4444',
        borderUpColor: '#22c55e',
        borderDownColor: '#ef4444',
        wickUpColor: '#22c55e',
        wickDownColor: '#ef4444',
        priceLineVisible: false,
        lastValueVisible: false,
      })
    } else {
      const fixed =
        fixedPriceRange &&
        Number.isFinite(fixedPriceRange.min) &&
        Number.isFinite(fixedPriceRange.max)
          ? fixedPriceRange
          : null
      primarySeries = chart.addLineSeries({
        color: lineColor,
        lineWidth: lineWidth || 1.75,
        priceLineVisible: false,
        lastValueVisible: !hideFloatingLabels,
        crosshairMarkerVisible: true,
        crosshairMarkerRadius: 5,
        autoscaleInfoProvider: fixed
          ? () => ({
              priceRange: {
                minValue: fixed.min,
                maxValue: fixed.max,
              },
            })
          : symmetricZero
            ? (original) => {
                const res = original()
                if (!res?.priceRange) return res
                const lo = Number(res.priceRange.minValue)
                const hi = Number(res.priceRange.maxValue)
                if (!Number.isFinite(lo) || !Number.isFinite(hi)) return res
                const ext = Math.max(Math.abs(lo), Math.abs(hi), 5)
                return {
                  ...res,
                  priceRange: { minValue: -ext, maxValue: ext },
                }
              }
            : undefined,
      })
    }

    let zeroSeries = null
    if (zeroLine && mode === 'line') {
      zeroSeries = chart.addLineSeries({
        color: 'rgba(248, 250, 252, 0.85)',
        lineWidth: 2,
        lineStyle: 0,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
        autoscaleInfoProvider: () => null,
      })
    }

    let overlaySeries = null
    if (mode === 'line') {
      overlaySeries = chart.addLineSeries({
        color: overlayLineColor,
        lineWidth: 1.5,
        lineStyle: 2,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
        visible: false,
        autoscaleInfoProvider: () => null,
      })
    }

    chartRef.current = chart
    primaryRef.current = primarySeries
    anchorRef.current = anchorSeries
    zeroRef.current = zeroSeries
    overlayRef.current = overlaySeries
    setChartInstance(chart)
    setPrimarySeriesInstance(primarySeries)

    const ro = new ResizeObserver(() => {
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
      registerPane?.(panelId, {
        chart,
        primarySeries,
        valueAtTime: (time) => {
          if (mode === 'candle') {
            const candle = findCandleAtTime(candlesRef.current, time)
            return candle?.close ?? 0
          }
          return findValueAtTime(lineRef.current, time) ?? 0
        },
        onCrosshairMove: (param) => {
          if (skipEmitRef.current) return
          if (!param?.time) {
            onCrosshairClearRef.current?.()
            return
          }
          const payload = {
            time: param.time,
            panelId,
            candle: mode === 'candle' ? findCandleAtTime(candlesRef.current, param.time) : null,
            value:
              mode === 'candle'
                ? findCandleAtTime(candlesRef.current, param.time)?.close ?? null
                : findValueAtTime(lineRef.current, param.time),
          }
          onCrosshairMoveRef.current?.(payload)
        },
        onCrosshairClear: () => onCrosshairClearRef.current?.(),
      }) || (() => {})

    const onClick = (param) => {
      if (!param?.time) return
      onChartClickRef.current?.({ time: param.time, panelId })
    }
    chart.subscribeClick(onClick)

    return () => {
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

  React.useEffect(() => {
    if (!anchorRef.current) return
    try {
      anchorRef.current.setData(buildAnchorPoints(timelineRows))
    } catch (err) {
      console.error('[workstation] anchor setData failed', panelId, err)
    }
  }, [timelineRows, panelId])

  React.useEffect(() => {
    if (!primaryRef.current || mode !== 'candle') return
    if (livePriceLineRef.current) {
      try {
        primaryRef.current.removePriceLine(livePriceLineRef.current)
      } catch {
        /* ignore */
      }
      livePriceLineRef.current = null
    }
    if (isNum(livePrice)) {
      try {
        livePriceLineRef.current = primaryRef.current.createPriceLine({
          price: livePrice,
          color: '#f87171',
          lineWidth: 1,
          lineStyle: 2,
          axisLabelVisible: !hideFloatingLabels,
          title: hideFloatingLabels ? '' : 'Live',
        })
      } catch (err) {
        console.error('[workstation] live price line failed', panelId, err)
      }
    }
  }, [livePrice, mode, panelId, hideFloatingLabels])

  React.useEffect(() => {
    if (!primaryRef.current || mode === 'candle') return
    if (liveDeviationLineRef.current) {
      try {
        primaryRef.current.removePriceLine(liveDeviationLineRef.current)
      } catch {
        /* ignore */
      }
      liveDeviationLineRef.current = null
    }
    if (!isNum(liveDeviationMarker)) return
    try {
      liveDeviationLineRef.current = primaryRef.current.createPriceLine({
        price: liveDeviationMarker,
        color: '#f472b6',
        lineWidth: 2,
        lineStyle: 2,
        axisLabelVisible: false,
        title: '',
      })
    } catch {
      /* ignore */
    }
  }, [liveDeviationMarker, mode, panelId])

  React.useEffect(() => {
    if (!primaryRef.current) return
    if (mode === 'candle') {
      const data = prepareLightweightCandles(candleBars)
      candlesRef.current = data
      try {
        primaryRef.current.setData(data)
      } catch (err) {
        console.error('[workstation] candle setData failed', panelId, err)
      }
      return
    }

    const data = prepareLightweightLinePoints(linePoints)
    lineRef.current = data
    try {
      primaryRef.current.setData(data)
    } catch (err) {
      console.error('[workstation] line setData failed', panelId, err)
    }
  }, [candleBars, linePoints, mode, panelId])

  React.useEffect(() => {
    if (!zeroRef.current || !timelineRows.length) return
    const first = timelineRows.find((r) => isNum(r.time))
    const last = timelineRows[timelineRows.length - 1]
    if (!first?.time || !last?.time) return
    try {
      zeroRef.current.setData([
        { time: first.time, value: 0 },
        { time: last.time, value: 0 },
      ])
    } catch {
      /* ignore */
    }
  }, [timelineRows])

  React.useEffect(() => {
    if (!overlayRef.current || mode !== 'line') return
    const pts = Array.isArray(overlayLinePoints) ? overlayLinePoints : []
    const data = prepareLightweightLinePoints(pts)
    try {
      overlayRef.current.applyOptions({ visible: data.length > 0 })
      overlayRef.current.setData(data)
    } catch (err) {
      console.error('[workstation] overlay setData failed', panelId, err)
    }
  }, [overlayLinePoints, mode, panelId])

  React.useEffect(() => {
    if (!primaryRef.current || mode !== 'line') return
    for (const line of guideLinesRef.current) {
      try {
        primaryRef.current.removePriceLine(line)
      } catch {
        /* ignore */
      }
    }
    guideLinesRef.current = []
    if (!Array.isArray(priceLines) || !priceLines.length) return
    for (const spec of priceLines) {
      if (!isNum(spec?.price)) continue
      try {
        const line = primaryRef.current.createPriceLine({
          price: spec.price,
          color: spec.color || 'rgba(148, 163, 184, 0.35)',
          lineWidth: spec.lineWidth ?? 1,
          lineStyle: spec.lineStyle ?? 2,
          axisLabelVisible:
            spec.axisLabelVisible != null
              ? Boolean(spec.axisLabelVisible)
              : Boolean(spec.title) && !hideFloatingLabels,
          title: spec.title || '',
        })
        guideLinesRef.current.push(line)
      } catch {
        /* ignore */
      }
    }
  }, [priceLines, mode, panelId, linePoints, hideFloatingLabels])

  React.useEffect(() => {
    if (!primaryRef.current || mode === 'candle') return
    try {
      const markers = []
      for (const ov of overflowMarkers || []) {
        if (!isNum(ov?.time)) continue
        markers.push({
          time: ov.time,
          position: ov.direction === 'down' ? 'belowBar' : 'aboveBar',
          color: '#fbbf24',
          shape: ov.direction === 'down' ? 'arrowDown' : 'arrowUp',
          size: 1,
        })
      }
      if (selectedTime != null) {
        markers.push({
          time: selectedTime,
          position: 'inBar',
          color: '#f8fafc',
          shape: 'circle',
          size: 2.5,
        })
      }
      primaryRef.current.setMarkers(markers)
    } catch {
      /* ignore */
    }
  }, [selectedTime, overflowMarkers, linePoints, mode, panelId])

  React.useEffect(() => {
    if (!chartRef.current || !primaryRef.current) return
    if (externalCrosshairTime == null) return
    const value =
      mode === 'candle'
        ? findCandleAtTime(candlesRef.current, externalCrosshairTime)?.close
        : findValueAtTime(lineRef.current, externalCrosshairTime)
    if (value == null) {
      const anchor = timelineRows.find((r) => r.time === externalCrosshairTime)
      if (!anchor) return
      skipEmitRef.current = true
      try {
        chartRef.current.setCrosshairPosition(0, externalCrosshairTime, anchorRef.current)
      } catch {
        /* ignore */
      }
      skipEmitRef.current = false
      return
    }
    skipEmitRef.current = true
    try {
      chartRef.current.setCrosshairPosition(value, externalCrosshairTime, primaryRef.current)
    } catch {
      /* ignore */
    }
    skipEmitRef.current = false
  }, [externalCrosshairTime, mode, timelineRows])

  const dateToTime = React.useCallback(
    (date) => cotDateToBarTime(timelineRows, date) ?? cotDateToBarTime(candlesRef.current, date),
    [timelineRows],
  )

  return (
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
          chart={chartInstance}
          primarySeries={primarySeriesInstance}
          timelineRows={timelineRows}
          panelId={panelId}
          drawings={drawings}
          selectedId={selectedDrawingId}
          activeTool={drawingTool}
          dateToTime={dateToTime}
          onSelectDrawing={onSelectDrawing}
          onDrawingCommit={onDrawingCommit}
        />
      </div>
    </div>
  )
}
