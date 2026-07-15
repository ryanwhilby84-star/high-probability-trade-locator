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
  scaleMarginsForPanel,
} from './workstationChartOptions.js'
import {
  recordChartMount,
  recordChartUnmount,
} from './cotWsRenderDiagnostics.js'

const LIVE_PRICE_LINE_COLOR = '#38bdf8'
const LIVE_PRICE_STALE_LINE_COLOR = '#fbbf24'

const isFiniteNumber = (value) =>
  typeof value === 'number' && Number.isFinite(value)

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
  latestMarkerTime = null,
  latestMarkerLabel = null,
  showLatestLabel = false,
  valueBadge = null,
  legendLabel = null,
  onFitY = null,
  className = '',
}) {
  const containerRef = React.useRef(null)
  const chartRef = React.useRef(null)
  const primarySeriesRef = React.useRef(null)
  const anchorSeriesRef = React.useRef(null)
  const zeroSeriesRef = React.useRef(null)
  const livePriceLineRef = React.useRef(null)
  const latestLineRef = React.useRef(null)

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
            color: 'rgba(148, 163, 184, 0.24)',
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
      anchorSeriesRef.current = anchorSeries
      zeroSeriesRef.current = zeroSeries

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
        recordChartUnmount(panelId)
        chart.remove()
      }

      chartRef.current = null
      primarySeriesRef.current = null
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
        // Vertical scaling + reset are owned by the workstation's per-pane
        // vertical camera (useGlobalVerticalMagnification). Disabling LWC's
        // native price-axis drag / double-click reset prevents two systems from
        // competing over the same gesture and keeps interaction stable.
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
    } catch (error) {
      console.error('[cot-workstation] line setData failed', panelId, error)
    }
  }, [
    candleBars,
    lineColor,
    linePoints,
    mode,
    panelId,
    syncOnly,
  ])

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

    try {
      livePriceLineRef.current = primarySeries.createPriceLine({
        price: livePrice,
        color: livePriceStale
          ? LIVE_PRICE_STALE_LINE_COLOR
          : LIVE_PRICE_LINE_COLOR,
        lineWidth: 1,
        lineStyle: 2,
        axisLabelVisible: true,
        title: livePriceStale ? 'Last' : 'Live',
      })
    } catch (error) {
      console.error('[cot-workstation] live price line failed', panelId, error)
    }
  }, [livePrice, livePriceStale, mode, panelId])

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
                  className={`cot-ws-live-price-badge${
                    livePriceStale ? ' cot-ws-live-price-badge--stale' : ''
                  }`}
                >
                  {livePriceStale ? 'STALE' : 'LIVE'}
                </span>

                <span className="cot-ws-pane-legend-value">
                  {formatWorkstationAxisPrice(livePrice)}
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
