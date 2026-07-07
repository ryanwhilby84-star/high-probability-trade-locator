import React from 'react'
import { createChart } from 'lightweight-charts'

import { createRafCoalescer } from './drawingViewport.js'
import {
  prepareLightweightCandles,
  prepareLightweightLinePoints,
} from '../data/prepareLightweightCandles.js'
import { formatAgeMs } from '../../hooks/liveQuoteFreshness.js'
import { WS_CHART_COLORS, createCotWorkstationChartOptions, EXCLUDE_FROM_AUTOSCALE, WS_PRICE_SCALE_WIDTH, formatWorkstationAxisPrice } from './workstationChartOptions.js'
import { recordChartMount, recordChartUnmount } from './cotWsRenderDiagnostics.js'

const LIVE_PRICE_LINE_COLOR = '#38bdf8'
const LIVE_PRICE_STALE_LINE_COLOR = '#fbbf24'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

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

function hasChartData(mode, candleBars, linePoints) {
  if (mode === 'candle') return prepareLightweightCandles(candleBars).length > 0
  return prepareLightweightLinePoints(linePoints).length > 0
}

function waitForContainerSize(el, cb) {
  if (el.clientWidth > 0 && el.clientHeight > 0) {
    cb()
    return () => {}
  }
  const ro = new ResizeObserver(() => {
    if (el.clientWidth > 0 && el.clientHeight > 0) {
      ro.disconnect()
      cb()
    }
  })
  ro.observe(el)
  return () => ro.disconnect()
}

/** Lightweight chart pane — candles or line, linked timeline, no drawings. */
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
  className = '',
}) {
  const containerRef = React.useRef(null)
  const chartRef = React.useRef(null)
  const primaryRef = React.useRef(null)
  const livePriceLineRef = React.useRef(null)
  const anchorRef = React.useRef(null)
  const zeroRef = React.useRef(null)
  const candlesRef = React.useRef([])
  const lineRef = React.useRef([])
  const syncOnlyRef = React.useRef(syncOnly)
  const modeRef = React.useRef(mode)
  const registerPaneRef = React.useRef(registerPane)
  const [viewportPainted, setViewportPainted] = React.useState(false)

  registerPaneRef.current = registerPane
  syncOnlyRef.current = syncOnly
  modeRef.current = mode

  const showPlaceholder = Boolean(emptyMessage) && !hasChartData(mode, candleBars, linePoints)

  React.useEffect(() => {
    if (!chartsReady) return undefined
    const el = containerRef.current
    if (!el) return undefined

    let chart
    let unregister = () => {}
    let resizeRo = null
    let cancelled = false

    const mountChart = () => {
      if (cancelled || chartRef.current) return

      try {
        chart = createChart(
          el,
          createCotWorkstationChartOptions({
            width: Math.max(el.clientWidth, 1),
            height: Math.max(el.clientHeight, 1),
            showTimeAxis,
            panelId,
            interactionEnabled: true,
            passiveCamera,
            hidePriceScale: syncOnlyRef.current,
            reservePriceScaleGutter: syncOnlyRef.current,
          }),
        )
      } catch (err) {
        console.error('[cot-workstation] createChart failed', panelId, err)
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

      let primarySeries = null
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
          lineWidth: 2,
          lineType: 0,
          priceLineVisible: false,
          lastValueVisible: false,
          crosshairMarkerVisible: true,
          crosshairMarkerRadius: 3,
          crosshairMarkerBorderColor: lineColor,
          crosshairMarkerBackgroundColor: WS_CHART_COLORS.background,
        })

        if (zeroLine) {
          zeroSeries = chart.addLineSeries({
            color: 'rgba(148, 163, 184, 0.28)',
            lineWidth: 1,
            lineStyle: 2,
            priceLineVisible: false,
            lastValueVisible: false,
            crosshairMarkerVisible: false,
          })
        }
      }

      chartRef.current = chart
      primaryRef.current = primarySeries
      anchorRef.current = anchorSeries
      zeroRef.current = zeroSeries

      const scheduleResize = createRafCoalescer(() => {
        if (!containerRef.current || !chartRef.current) return
        const { clientWidth, clientHeight } = containerRef.current
        chartRef.current.applyOptions({
          width: Math.max(clientWidth, 1),
          height: Math.max(clientHeight, 1),
        })
      })

      resizeRo = new ResizeObserver(() => scheduleResize())
      resizeRo.observe(el)

      unregister =
        registerPaneRef.current?.(panelId, {
          chart,
          primarySeries: primarySeries ?? anchorSeries,
          anchorSeries,
          panelId,
          mode: modeRef.current,
          syncOnly: syncOnlyRef.current,
          valueAtTime: (time) => {
            if (syncOnlyRef.current) return null
            if (modeRef.current === 'candle') {
              const close = findCandleAtTime(candlesRef.current, time)?.close
              return close != null ? close : null
            }
            return findValueAtTime(lineRef.current, time)
          },
        }) || (() => {})

      requestAnimationFrame(() => {
        if (!cancelled) setViewportPainted((prev) => (prev ? prev : true))
      })
    }

    const cancelWait = waitForContainerSize(el, mountChart)

    return () => {
      cancelled = true
      cancelWait()
      resizeRo?.disconnect()
      unregister()
      if (chart) {
        recordChartUnmount(panelId)
        chart.remove()
      }
      chartRef.current = null
      primaryRef.current = null
      anchorRef.current = null
      zeroRef.current = null
      setViewportPainted((prev) => (prev ? false : prev))
    }
  }, [panelId, mode, showTimeAxis, zeroLine, lineColor, chartsReady, passiveCamera])

  React.useEffect(() => {
    if (!chartRef.current) return
    const gutterOnly = syncOnly
    chartRef.current.applyOptions({
      handleScroll: {
        mouseWheel: false,
        pressedMouseMove: !passiveCamera,
        horzTouchDrag: !passiveCamera,
        vertTouchDrag: false,
      },
      handleScale: {
        mouseWheel: nativeWheelZoom || !passiveCamera,
        pinch: nativeWheelZoom || !passiveCamera,
        axisPressedMouseMove: { time: nativeWheelZoom || !passiveCamera, price: false },
        axisDoubleClickReset: { time: false, price: false },
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
        scaleMargins: { top: 0.12, bottom: showTimeAxis ? 0.14 : 0.1 },
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
    if (syncOnly && primaryRef.current) {
      try {
        primaryRef.current.setData([])
      } catch {
        /* ignore */
      }
    }
  }, [syncOnly, passiveCamera, nativeWheelZoom, panelId, showTimeAxis])

  React.useEffect(() => {
    if (!anchorRef.current) return
    try {
      anchorRef.current.setData(buildAnchorPoints(timelineRows))
    } catch (err) {
      console.error('[cot-workstation] anchor setData failed', panelId, err)
    }
  }, [timelineRows, panelId])

  React.useEffect(() => {
    if (syncOnly || !primaryRef.current) return
    if (mode === 'candle') {
      const data = prepareLightweightCandles(candleBars)
      candlesRef.current = data
      try {
        primaryRef.current.setData(data)
      } catch (err) {
        console.error('[cot-workstation] candle setData failed', panelId, err)
      }
      return
    }

    const data = prepareLightweightLinePoints(linePoints)
    lineRef.current = data
    try {
      primaryRef.current.setData(data)
    } catch (err) {
      console.error('[cot-workstation] line setData failed', panelId, err)
    }
  }, [candleBars, linePoints, mode, panelId, syncOnly])

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
    if (!isNum(livePrice)) return
    const stale = Boolean(livePriceStale)
    try {
      livePriceLineRef.current = primaryRef.current.createPriceLine({
        price: livePrice,
        color: stale ? LIVE_PRICE_STALE_LINE_COLOR : LIVE_PRICE_LINE_COLOR,
        lineWidth: 1,
        lineStyle: 2,
        axisLabelVisible: true,
        title: stale ? 'Last' : 'Live',
      })
    } catch (err) {
      console.error('[cot-workstation] live price line failed', panelId, err)
    }
  }, [livePrice, livePriceStale, mode, panelId])

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

  return (
    <div className={`ws-chart-pane cot-ws-chart-pane ${className}`.trim()} data-panel={panelId}>
      <div className="ws-chart-pane-plot cot-ws-chart-plot">
        <div
          className={`ws-chart-pane-canvas cot-ws-chart-canvas${
            viewportPainted ? ' cot-ws-chart-canvas--ready' : ''
          }`}
          ref={containerRef}
        />
        <div className="cot-ws-drawing-host" data-drawing-panel={panelId} aria-hidden="true" />
        {!chartsReady ? (
          <div className="cot-ws-chart-skeleton" aria-hidden="true" />
        ) : null}
        {showPlaceholder ? (
          <div className="cot-ws-chart-placeholder" aria-live="polite">
            {emptyMessage}
          </div>
        ) : null}
        {mode === 'candle' && isNum(livePrice) ? (
          <div className="cot-ws-live-price-hud" aria-live="polite">
            <span
              className={`cot-ws-live-price-badge${
                livePriceStale ? ' cot-ws-live-price-badge--stale' : ''
              }`}
            >
              {livePriceStale ? 'STALE' : 'LIVE'}
            </span>
            <span className="cot-ws-live-price-value">{formatWorkstationAxisPrice(livePrice)}</span>
            {livePriceAsOf ? (
              <span
                className="cot-ws-live-price-ts"
                title={livePriceSource ? `Source: ${livePriceSource}` : undefined}
              >
                {String(livePriceAsOf).slice(0, 19).replace('T', ' ')}
                {livePriceAgeMs != null ? ` · ${formatAgeMs(livePriceAgeMs)}` : ''}
              </span>
            ) : null}
          </div>
        ) : null}
      </div>
    </div>
  )
}
