import React from 'react'
import { createChart } from 'lightweight-charts'

import { createRafCoalescer } from './drawingViewport.js'
import {
  prepareLightweightCandles,
  prepareLightweightLinePoints,
} from '../data/prepareLightweightCandles.js'
import { WS_CHART_COLORS, createCotWorkstationChartOptions, WS_PRICE_SCALE_WIDTH } from './workstationChartOptions.js'
import { recordChartMount, recordChartUnmount } from './cotWsRenderDiagnostics.js'

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
  className = '',
}) {
  const containerRef = React.useRef(null)
  const chartRef = React.useRef(null)
  const primaryRef = React.useRef(null)
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
        minimumWidth: WS_PRICE_SCALE_WIDTH,
        ticksVisible: !gutterOnly,
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
      localization: gutterOnly ? { priceFormatter: () => '' } : undefined,
    })
    if (syncOnly && primaryRef.current) {
      try {
        primaryRef.current.setData([])
      } catch {
        /* ignore */
      }
    }
  }, [syncOnly, passiveCamera, nativeWheelZoom])

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
      </div>
    </div>
  )
}
