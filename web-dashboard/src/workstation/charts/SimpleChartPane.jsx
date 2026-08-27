import React from 'react'
import { createChart } from 'lightweight-charts'

import { createRafCoalescer } from './drawingViewport.js'
import {
  prepareLightweightCandles,
  prepareLightweightLinePoints,
} from '../data/prepareLightweightCandles.js'
import { formatAgeMs } from '../../hooks/liveQuoteFreshness.js'
import {
  WS_CHART_COLORS,
  createCotWorkstationChartOptions,
  EXCLUDE_FROM_AUTOSCALE,
  WS_PRICE_SCALE_WIDTH,
  formatWorkstationAxisPrice,
  formatExactLivePrice,
  scaleMarginsForPanel,
} from './workstationChartOptions.js'
import {
  recordChartMount,
  recordChartUnmount,
} from './cotWsRenderDiagnostics.js'
import { ResearchPinsOverlay } from './ResearchPinsOverlay.jsx'

const LIVE_PRICE_LINE_COLOR = '#38bdf8'
const LIVE_PRICE_STALE_LINE_COLOR = '#fbbf24'
const LIVE_PRICE_FALLBACK_LINE_COLOR = '#94a3b8'

const isFiniteNumber = (value) =>
  typeof value === 'number' && Number.isFinite(value)

const isDev =
  typeof import.meta !== 'undefined' && Boolean(import.meta.env?.DEV)

function liveBadgeClass(status) {
  const s = String(status || '').toUpperCase()
  if (s === 'LIVE') return ''
  if (s === 'STALE' || s === 'RECONNECTING') return ' cot-ws-live-price-badge--stale'
  if (s === 'FALLBACK') return ' cot-ws-live-price-badge--fallback'
  return ' cot-ws-live-price-badge--offline'
}

function liveLineColor(status) {
  const s = String(status || '').toUpperCase()
  if (s === 'LIVE') return LIVE_PRICE_LINE_COLOR
  if (s === 'STALE' || s === 'RECONNECTING') return LIVE_PRICE_STALE_LINE_COLOR
  return LIVE_PRICE_FALLBACK_LINE_COLOR
}

function buildAnchorPoints(timelineRows) {
  return (timelineRows || [])
    .filter((row) => isFiniteNumber(row.time))
    .map((row) => ({ time: row.time, value: 0 }))
}

function findValueAtTime(points, time) {
  if (!isFiniteNumber(time) || !points?.length) return null
  const row = points.find((point) => point.time === time)
  return row?.value ?? null
}

function findCandleAtTime(candles, time) {
  if (!isFiniteNumber(time) || !candles?.length) return null
  return candles.find((candle) => candle.time === time) ?? null
}

function hasRenderableData(mode, candleBars, linePoints) {
  if (mode === 'candle') {
    return prepareLightweightCandles(candleBars).length > 0
  }
  return prepareLightweightLinePoints(linePoints).length > 0
}

function percentile(sortedValues, q) {
  if (!sortedValues.length) return 0
  const idx = Math.max(
    0,
    Math.min(sortedValues.length - 1, Math.round((sortedValues.length - 1) * q)),
  )
  return sortedValues[idx]
}

/**
 * Weekly net-position delta rendered as a subtle histogram on its own hidden
 * scale. Positive = net positioning increased; negative = decreased.
 * Colour opacity is normalised to the 90th percentile so one historic shock
 * cannot make the rest of the stream disappear.
 */
function buildDeltaFlowPoints(points) {
  if (!Array.isArray(points) || points.length < 2) return []

  const deltas = []
  const magnitudes = []

  for (let i = 1; i < points.length; i += 1) {
    const current = points[i]
    const previous = points[i - 1]
    if (
      !isFiniteNumber(current?.time) ||
      !isFiniteNumber(current?.value) ||
      !isFiniteNumber(previous?.value)
    ) {
      continue
    }

    const delta = current.value - previous.value
    if (!Number.isFinite(delta)) continue
    deltas.push({ time: current.time, value: delta })
    magnitudes.push(Math.abs(delta))
  }

  const sorted = magnitudes.slice().sort((a, b) => a - b)
  const normaliser = percentile(sorted, 0.9) || sorted[sorted.length - 1] || 1

  return deltas.map((row) => {
    const strength = Math.min(1, Math.abs(row.value) / normaliser)
    const alpha = 0.07 + strength * 0.22
    const color =
      row.value >= 0
        ? `rgba(34, 139, 101, ${alpha.toFixed(3)})`
        : `rgba(201, 84, 84, ${alpha.toFixed(3)})`

    return { ...row, color }
  })
}

function waitForContainerSize(element, onReady) {
  if (element.clientWidth > 0 && element.clientHeight > 0) {
    onReady()
    return () => {}
  }

  const observer = new ResizeObserver(() => {
    if (element.clientWidth > 0 && element.clientHeight > 0) {
      observer.disconnect()
      onReady()
    }
  })

  observer.observe(element)
  return () => observer.disconnect()
}

/**
 * One Lightweight Charts pane.
 *
 * Horizontal navigation is owned by the master camera. Each pane keeps its own
 * vertical scale while sharing time, crosshair and viewport geometry.
 */
export function SimpleChartPane({
  panelId,
  mode = 'line',
  showTimeAxis = false,
  lineColor = '#38bdf8',
  linePoints = [],
  candleBars = [],
  timelineRows = [],
  zeroLine = false,
  syncOnly = false,
  emptyMessage = null,
  registerPane,
  chartsReady = true,
  passiveCamera = false,
  nativeWheelZoom = false,
  livePrice = null,
  livePriceAsOf = null,
  livePriceSource = null,
  livePriceStale = false,
  livePriceAgeMs = null,
  livePriceStatus = null,
  livePricePrecision = null,
  livePriceBid = null,
  livePriceAsk = null,
  livePriceProvider = null,
  livePriceSymbol = null,
  activeWeeklyCandle = null,
  latestMarkerTime = null,
  latestMarkerLabel = null,
  showLatestLabel = false,
  /** @type {Array<{ time: number, position?: string, color?: string, shape?: string, text?: string }>|null} */
  eventMarkers = null,
  /** Selected research event week — draws a synced vertical highlight line. */
  eventHighlightTime = null,
  /** Compact HTML research pins (primary visibility). */
  researchPins = null,
  researchPinVariant = 'cot',
  /** Optional secondary line on a separate right scale (e.g. positioning spread). */
  overlayLinePoints = null,
  overlayLineColor = 'rgba(15, 118, 110, 0.9)',
  onTimeClick = null,
  /** Pin click — receives pin descriptor; must not mutate camera. */
  onPinClick = null,
  valueBadge = null,
  legendLabel = null,
  onFitY = null,
  className = '',
}) {
  const containerRef = React.useRef(null)
  const chartRef = React.useRef(null)
  const primarySeriesRef = React.useRef(null)
  const overlaySeriesRef = React.useRef(null)
  const deltaSeriesRef = React.useRef(null)
  const anchorSeriesRef = React.useRef(null)
  const zeroSeriesRef = React.useRef(null)
  const livePriceLineRef = React.useRef(null)
  const latestLineRef = React.useRef(null)
  const eventHighlightRef = React.useRef(null)
  const onTimeClickRef = React.useRef(onTimeClick)
  onTimeClickRef.current = onTimeClick

  const candlesRef = React.useRef([])
  const linePointsRef = React.useRef([])
  const registerPaneRef = React.useRef(registerPane)
  const syncOnlyRef = React.useRef(syncOnly)
  const modeRef = React.useRef(mode)

  const [mounted, setMounted] = React.useState(false)

  registerPaneRef.current = registerPane
  syncOnlyRef.current = syncOnly
  modeRef.current = mode

  const showPlaceholder =
    Boolean(emptyMessage) &&
    !hasRenderableData(mode, candleBars, linePoints)

  React.useEffect(() => {
    if (!chartsReady) return undefined

    const element = containerRef.current
    if (!element) return undefined

    let cancelled = false
    let chart = null
    let resizeObserver = null
    let unregisterPane = () => {}
    let clickHandler = null

    const mount = () => {
      if (cancelled || chartRef.current) return

      try {
        chart = createChart(
          element,
          createCotWorkstationChartOptions({
            width: Math.max(element.clientWidth, 1),
            height: Math.max(element.clientHeight, 1),
            showTimeAxis,
            panelId,
            interactionEnabled: true,
            passiveCamera,
            hidePriceScale: syncOnlyRef.current,
            reservePriceScaleGutter: syncOnlyRef.current,
          }),
        )
      } catch (error) {
        console.error('[cot-workstation] createChart failed', panelId, error)
        return
      }

      recordChartMount(panelId)

      const anchorSeries = chart.addLineSeries({
        color: 'transparent',
        lineWidth: 0,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
        autoscaleInfoProvider: EXCLUDE_FROM_AUTOSCALE,
      })

      let primarySeries
      let zeroSeries = null
      let deltaSeries = null

      if (modeRef.current === 'candle') {
        primarySeries = chart.addCandlestickSeries({
          upColor: WS_CHART_COLORS.up,
          downColor: WS_CHART_COLORS.down,
          borderUpColor: WS_CHART_COLORS.up,
          borderDownColor: WS_CHART_COLORS.down,
          wickUpColor: WS_CHART_COLORS.upWick,
          wickDownColor: WS_CHART_COLORS.downWick,
          priceLineVisible: false,
          lastValueVisible: false,
        })
      } else {
        // Delta is created first so the positioning line always paints above it.
        if (zeroLine) {
          deltaSeries = chart.addHistogramSeries({
            priceScaleId: 'delta-flow',
            base: 0,
            priceLineVisible: false,
            lastValueVisible: false,
          })
          chart.priceScale('delta-flow').applyOptions({
            visible: false,
            borderVisible: false,
            scaleMargins: { top: 0.7, bottom: 0.03 },
          })
        }

        primarySeries = chart.addLineSeries({
          color: lineColor,
          lineWidth: 1.5,
          lineType: 0,
          priceLineVisible: false,
          lastValueVisible: false,
          crosshairMarkerVisible: true,
          crosshairMarkerRadius: 2,
          crosshairMarkerBorderColor: lineColor,
          crosshairMarkerBackgroundColor: WS_CHART_COLORS.background,
        })

        if (zeroLine) {
          zeroSeries = chart.addLineSeries({
            color: 'rgba(111, 124, 119, 0.22)',
            lineWidth: 1,
            lineStyle: 2,
            priceLineVisible: false,
            lastValueVisible: false,
            crosshairMarkerVisible: false,
            autoscaleInfoProvider: EXCLUDE_FROM_AUTOSCALE,
          })
        }
      }

      chartRef.current = chart
      primarySeriesRef.current = primarySeries
      overlaySeriesRef.current = null
      deltaSeriesRef.current = deltaSeries
      anchorSeriesRef.current = anchorSeries
      zeroSeriesRef.current = zeroSeries

      clickHandler = (param) => {
        if (param?.time == null) return
        onTimeClickRef.current?.(param.time)
      }
      chart.subscribeClick(clickHandler)

      const resizeChart = createRafCoalescer(() => {
        const target = containerRef.current
        const currentChart = chartRef.current
        if (!target || !currentChart) return

        currentChart.applyOptions({
          width: Math.max(target.clientWidth, 1),
          height: Math.max(target.clientHeight, 1),
        })
      })

      resizeObserver = new ResizeObserver(resizeChart)
      resizeObserver.observe(element)

      unregisterPane =
        registerPaneRef.current?.(panelId, {
          chart,
          primarySeries,
          anchorSeries,
          panelId,
          mode: modeRef.current,
          syncOnly: syncOnlyRef.current,
          valueAtTime: (time) => {
            if (syncOnlyRef.current) return null

            if (modeRef.current === 'candle') {
              return findCandleAtTime(candlesRef.current, time)?.close ?? null
            }

            return findValueAtTime(linePointsRef.current, time)
          },
        }) || (() => {})

      requestAnimationFrame(() => {
        if (!cancelled) setMounted(true)
      })
    }

    const stopWaiting = waitForContainerSize(element, mount)

    return () => {
      cancelled = true
      stopWaiting()
      resizeObserver?.disconnect()
      unregisterPane()

      if (chart) {
        if (clickHandler) {
          try {
            chart.unsubscribeClick(clickHandler)
          } catch {
            // chart already torn down
          }
        }
        recordChartUnmount(panelId)
        chart.remove()
      }

      chartRef.current = null
      primarySeriesRef.current = null
      overlaySeriesRef.current = null
      deltaSeriesRef.current = null
      anchorSeriesRef.current = null
      zeroSeriesRef.current = null
      livePriceLineRef.current = null
      setMounted(false)
    }
  }, [
    chartsReady,
    lineColor,
    mode,
    panelId,
    passiveCamera,
    showTimeAxis,
    zeroLine,
  ])

  React.useEffect(() => {
    const chart = chartRef.current
    if (!chart) return

    const gutterOnly = syncOnly
    const horizontalNavigation = nativeWheelZoom || !passiveCamera

    chart.applyOptions({
      handleScroll: {
        mouseWheel: false,
        pressedMouseMove: !passiveCamera,
        horzTouchDrag: !passiveCamera,
        vertTouchDrag: false,
      },
      handleScale: {
        mouseWheel: horizontalNavigation,
        pinch: horizontalNavigation,
        axisPressedMouseMove: {
          time: horizontalNavigation,
          price: false,
        },
        axisDoubleClickReset: {
          time: false,
          price: false,
        },
      },
      grid: {
        horzLines: { visible: !gutterOnly },
      },
      rightPriceScale: {
        visible: !syncOnly || gutterOnly,
        borderVisible: false,
        minimumWidth: WS_PRICE_SCALE_WIDTH,
        entireTextOnly: true,
        ticksVisible: !gutterOnly,
        autoScale: true,
        scaleMargins: scaleMarginsForPanel(panelId, showTimeAxis),
      },
      crosshair: {
        horzLine: {
          visible: !syncOnly && !gutterOnly,
          labelVisible: !syncOnly && !gutterOnly,
        },
        vertLine: {
          visible: true,
          labelVisible: showTimeAxis,
        },
      },
      localization: gutterOnly
        ? { priceFormatter: () => '' }
        : { priceFormatter: formatWorkstationAxisPrice },
    })

    if (syncOnly && primarySeriesRef.current) {
      try {
        primarySeriesRef.current.setData([])
      } catch {
        // Ignore a chart that is being remounted.
      }
    }
  }, [
    nativeWheelZoom,
    panelId,
    passiveCamera,
    showTimeAxis,
    syncOnly,
  ])

  React.useEffect(() => {
    const anchorSeries = anchorSeriesRef.current
    if (!anchorSeries) return

    try {
      anchorSeries.setData(buildAnchorPoints(timelineRows))
    } catch (error) {
      console.error('[cot-workstation] anchor setData failed', panelId, error)
    }
  }, [panelId, timelineRows])

  React.useEffect(() => {
    const primarySeries = primarySeriesRef.current
    if (syncOnly || !primarySeries) return

    if (mode === 'candle') {
      const candles = prepareLightweightCandles(candleBars)
      candlesRef.current = candles

      try {
        primarySeries.setData(candles)
      } catch (error) {
        console.error('[cot-workstation] candle setData failed', panelId, error)
      }

      return
    }

    const points = prepareLightweightLinePoints(linePoints)
    linePointsRef.current = points

    try {
      primarySeries.setData(points)
      primarySeries.setMarkers([])
      deltaSeriesRef.current?.setData(buildDeltaFlowPoints(points))
    } catch (error) {
      console.error('[cot-workstation] line/delta setData failed', panelId, error)
    }
  }, [
    candleBars,
    lineColor,
    linePoints,
    mode,
    panelId,
    syncOnly,
  ])

  // Intelligence V2 event markers — paint-only; never mutates camera.
  React.useEffect(() => {
    const primarySeries = primarySeriesRef.current
    if (syncOnly || !primarySeries) return
    const markers = Array.isArray(eventMarkers)
      ? eventMarkers.filter((m) => isFiniteNumber(m?.time))
      : []
    try {
      primarySeries.setMarkers(markers)
    } catch (error) {
      console.error('[cot-workstation] setMarkers failed', panelId, error)
    }
  }, [eventMarkers, syncOnly, panelId, mounted, linePoints, candleBars])

  // Optional overlay line (separate price scale) — used for Comm↔NR spread.
  React.useEffect(() => {
    const chart = chartRef.current
    if (!chart || syncOnly || mode === 'candle') return

    const points = Array.isArray(overlayLinePoints)
      ? prepareLightweightLinePoints(overlayLinePoints)
      : []

    if (!points.length) {
      if (overlaySeriesRef.current) {
        try {
          chart.removeSeries(overlaySeriesRef.current)
        } catch {
          // ignore
        }
        overlaySeriesRef.current = null
      }
      return
    }

    if (!overlaySeriesRef.current) {
      try {
        overlaySeriesRef.current = chart.addLineSeries({
          color: overlayLineColor,
          lineWidth: 1,
          lineStyle: 2,
          priceScaleId: 'overlay',
          priceLineVisible: false,
          lastValueVisible: false,
          crosshairMarkerVisible: false,
        })
        chart.priceScale('overlay').applyOptions({
          scaleMargins: { top: 0.12, bottom: 0.12 },
          borderVisible: false,
        })
      } catch (error) {
        console.error('[cot-workstation] overlay series create failed', panelId, error)
        return
      }
    }

    try {
      overlaySeriesRef.current.applyOptions({ color: overlayLineColor })
      overlaySeriesRef.current.setData(points)
    } catch (error) {
      console.error('[cot-workstation] overlay setData failed', panelId, error)
    }
  }, [overlayLinePoints, overlayLineColor, syncOnly, mode, panelId, mounted])

  // Incremental live weekly-candle update — series.update only. Never setData,
  // fitContent, or camera resets. Preserves pan/zoom/drawings.
  React.useEffect(() => {
    const primarySeries = primarySeriesRef.current
    if (syncOnly || mode !== 'candle' || !primarySeries) return
    if (!activeWeeklyCandle) return

    const open = Number(activeWeeklyCandle.open)
    const high = Number(activeWeeklyCandle.high)
    const low = Number(activeWeeklyCandle.low)
    const close = Number(activeWeeklyCandle.close)
    const time = Number(activeWeeklyCandle.time)

    if (![open, high, low, close, time].every(isFiniteNumber)) return

    const candle = {
      time,
      open,
      high: Math.max(open, high, low, close),
      low: Math.min(open, high, low, close),
      close,
    }

    try {
      primarySeries.update(candle)
      const existing = candlesRef.current || []
      const idx = existing.findIndex((row) => row.time === time)
      if (idx >= 0) {
        const next = existing.slice()
        next[idx] = candle
        candlesRef.current = next
      } else {
        candlesRef.current = [...existing, candle]
      }
    } catch (error) {
      console.error('[cot-workstation] active weekly candle update failed', panelId, error)
    }
  }, [activeWeeklyCandle, mode, panelId, syncOnly])

  React.useEffect(() => {
    const primarySeries = primarySeriesRef.current
    if (!primarySeries || mode !== 'candle') return

    if (livePriceLineRef.current) {
      try {
        primarySeries.removePriceLine(livePriceLineRef.current)
      } catch {
        // Ignore stale price-line handles.
      }
      livePriceLineRef.current = null
    }

    if (!isFiniteNumber(livePrice)) return

    const status = livePriceStatus || (livePriceStale ? 'STALE' : 'LIVE')
    const exact = formatExactLivePrice(livePrice, livePricePrecision)

    try {
      livePriceLineRef.current = primarySeries.createPriceLine({
        price: livePrice,
        color: liveLineColor(status),
        lineWidth: 1,
        lineStyle: 2,
        axisLabelVisible: true,
        title: exact ? `${status === 'LIVE' ? 'Live' : status} ${exact}` : status,
      })
    } catch (error) {
      console.error('[cot-workstation] live price line failed', panelId, error)
    }
  }, [
    livePrice,
    livePriceStale,
    livePriceStatus,
    livePricePrecision,
    mode,
    panelId,
  ])

  React.useEffect(() => {
    const zeroSeries = zeroSeriesRef.current
    if (!zeroSeries || timelineRows.length === 0) return

    const first = timelineRows.find((row) => isFiniteNumber(row.time))
    const last = timelineRows[timelineRows.length - 1]

    if (!isFiniteNumber(first?.time) || !isFiniteNumber(last?.time)) return

    try {
      zeroSeries.setData([
        { time: first.time, value: 0 },
        { time: last.time, value: 0 },
      ])
    } catch {
      // Ignore a chart that is being remounted.
    }
  }, [timelineRows])

  React.useEffect(() => {
    const chart = chartRef.current
    const marker = latestLineRef.current

    if (!chart || !marker || !isFiniteNumber(latestMarkerTime)) {
      if (marker) marker.style.display = 'none'
      return undefined
    }

    const timeScale = chart.timeScale()

    const positionMarker = () => {
      const currentMarker = latestLineRef.current
      if (!currentMarker) return

      let x = null

      try {
        x = timeScale.timeToCoordinate(latestMarkerTime)
      } catch {
        x = null
      }

      if (x == null) {
        currentMarker.style.display = 'none'
        return
      }

      currentMarker.style.display = 'block'
      currentMarker.style.left = `${Math.round(x)}px`
    }

    positionMarker()

    try {
      timeScale.subscribeVisibleLogicalRangeChange(positionMarker)
    } catch {
      // Older chart API versions may not expose this subscription.
    }

    const resizeObserver = containerRef.current
      ? new ResizeObserver(positionMarker)
      : null

    if (resizeObserver && containerRef.current) {
      resizeObserver.observe(containerRef.current)
    }

    return () => {
      try {
        timeScale.unsubscribeVisibleLogicalRangeChange(positionMarker)
      } catch {
        // Ignore API/version differences during teardown.
      }
      resizeObserver?.disconnect()
    }
  }, [latestMarkerTime, mounted])

  // Selected research event — vertical highlight synced across panes.
  React.useEffect(() => {
    const chart = chartRef.current
    const lineEl = eventHighlightRef.current
    if (!chart || !lineEl) return undefined

    const timeScale = chart.timeScale()

    const positionLine = () => {
      const current = eventHighlightRef.current
      if (!current) return

      if (!isFiniteNumber(eventHighlightTime)) {
        current.style.display = 'none'
        return
      }

      const x = timeScale.timeToCoordinate(eventHighlightTime)
      if (x == null || !Number.isFinite(x)) {
        current.style.display = 'none'
        return
      }

      current.style.display = 'block'
      current.style.left = `${Math.round(x)}px`
    }

    positionLine()

    try {
      timeScale.subscribeVisibleLogicalRangeChange(positionLine)
    } catch {
      // API differences
    }

    const resizeObserver = containerRef.current
      ? new ResizeObserver(positionLine)
      : null
    if (resizeObserver && containerRef.current) {
      resizeObserver.observe(containerRef.current)
    }

    return () => {
      try {
        timeScale.unsubscribeVisibleLogicalRangeChange(positionLine)
      } catch {
        // teardown
      }
      resizeObserver?.disconnect()
    }
  }, [eventHighlightTime, mounted, eventMarkers])

  return (
    <div
      className={`ws-chart-pane cot-ws-chart-pane ${className}`.trim()}
      data-panel={panelId}
    >
      <div className="ws-chart-pane-plot cot-ws-chart-plot">
        <div
          ref={containerRef}
          className={`ws-chart-pane-canvas cot-ws-chart-canvas${
            mounted ? ' cot-ws-chart-canvas--ready' : ''
          }`}
        />

        {!syncOnly && Array.isArray(researchPins) && researchPins.length ? (
          <ResearchPinsOverlay
            chartRef={chartRef}
            containerRef={containerRef}
            pins={researchPins}
            mounted={mounted}
            variant={researchPinVariant}
            onPinClick={onPinClick || onTimeClick}
          />
        ) : null}

        <div
          className="cot-ws-drawing-host"
          data-drawing-panel={panelId}
          aria-hidden="true"
        />

        {onFitY && !syncOnly ? (
          <button
            type="button"
            className="cot-ws-fit-y-btn"
            title="Fit this panel to the visible date range"
            onPointerDown={(event) => event.stopPropagation()}
            onMouseDown={(event) => event.stopPropagation()}
            onClick={(event) => {
              event.stopPropagation()
              onFitY(panelId)
            }}
          >
            Fit Y
          </button>
        ) : null}

        <div
          ref={latestLineRef}
          className="cot-ws-latest-vline"
          style={{ display: 'none' }}
          aria-hidden="true"
        >
          {showLatestLabel && latestMarkerLabel ? (
            <span className="cot-ws-latest-vline-label">
              LATEST — {latestMarkerLabel}
            </span>
          ) : null}
        </div>

        <div
          ref={eventHighlightRef}
          className="cot-ws-event-vline"
          style={{ display: 'none' }}
          aria-hidden="true"
        >
          <span className="cot-ws-event-vline-label">EVENT</span>
        </div>

        {legendLabel ||
        (mode === 'line' && valueBadge) ||
        (mode === 'candle' && isFiniteNumber(livePrice)) ? (
          <div className="cot-ws-pane-legend" aria-live="polite">
            {legendLabel ? (
              <span className="cot-ws-pane-legend-label">{legendLabel}</span>
            ) : null}

            {mode === 'line' && valueBadge ? (
              <>
                <span className="cot-ws-pane-legend-value">
                  {valueBadge.valueText}
                </span>
                {valueBadge.changes?.map((chg) => (
                  <span key={chg.key} className="cot-ws-pane-legend-chg">
                    <span className="cot-ws-pane-legend-chg-key">
                      {chg.key}
                    </span>
                    <span
                      className={`cot-ws-pane-legend-delta cot-ws-pane-legend-delta--${chg.dir}`}
                    >
                      {chg.text}
                    </span>
                  </span>
                ))}
              </>
            ) : null}

            {mode === 'candle' && isFiniteNumber(livePrice) ? (
              <>
                <span
                  className={`cot-ws-live-price-badge${liveBadgeClass(
                    livePriceStatus || (livePriceStale ? 'STALE' : 'LIVE'),
                  )}`}
                >
                  {livePriceStatus || (livePriceStale ? 'STALE' : 'LIVE')}
                </span>

                <span
                  className="cot-ws-pane-legend-value cot-ws-live-price-value"
                  data-live-mid={String(livePrice)}
                  data-testid="live-mid"
                >
                  {formatExactLivePrice(livePrice, livePricePrecision)}
                </span>

                {livePriceAsOf ? (
                  <span
                    className="cot-ws-live-price-ts"
                    title={
                      livePriceSource
                        ? `Source: ${livePriceSource}`
                        : undefined
                    }
                  >
                    {String(livePriceAsOf).slice(0, 19).replace('T', ' ')}
                    {livePriceAgeMs != null
                      ? ` · ${formatAgeMs(livePriceAgeMs)}`
                      : ''}
                  </span>
                ) : null}

                {isDev ? (
                  <span className="cot-ws-live-price-diag" aria-hidden="true">
                    Source: {livePriceProvider || 'OANDA'}
                    {livePriceSymbol ? ` · Symbol: ${livePriceSymbol}` : ''}
                    {livePriceAsOf
                      ? ` · Updated: ${String(livePriceAsOf).slice(11, 19)}`
                      : ''}
                    {livePriceAgeMs != null
                      ? ` · Age: ${(livePriceAgeMs / 1000).toFixed(1)}s`
                      : ''}
                    {isFiniteNumber(livePriceBid)
                      ? ` · Bid: ${formatExactLivePrice(livePriceBid, livePricePrecision)}`
                      : ''}
                    {isFiniteNumber(livePriceAsk)
                      ? ` · Ask: ${formatExactLivePrice(livePriceAsk, livePricePrecision)}`
                      : ''}
                    {isFiniteNumber(livePrice)
                      ? ` · Mid: ${formatExactLivePrice(livePrice, livePricePrecision)}`
                      : ''}
                  </span>
                ) : null}
              </>
            ) : null}
          </div>
        ) : null}

        {!chartsReady ? (
          <div className="cot-ws-chart-skeleton" aria-hidden="true" />
        ) : null}

        {showPlaceholder ? (
          <div className="cot-ws-chart-placeholder" aria-live="polite">
            {emptyMessage}
          </div>
        ) : null}
      </div>
    </div>
  )
}
