import React from 'react'
import { createChart, CrosshairMode } from 'lightweight-charts'
import { CHART_WS } from '../../charts/chartTheme.js'
import {
  prepareLightweightCandles,
  prepareLightweightLinePoints,
} from '../data/prepareLightweightCandles.js'

const WS_CHART = {
  background: CHART_WS.canvas,
  text: CHART_WS.axis,
  grid: CHART_WS.grid,
  border: CHART_WS.border,
  up: '#22c55e',
  down: '#ef4444',
  wickUp: '#22c55e',
  wickDown: '#ef4444',
  fairValue: CHART_WS.valuation,
  crosshair: CHART_WS.crosshair,
}

function findBarByTime(bars, time) {
  if (time == null || !bars?.length) return null
  return bars.find((b) => b.time === time) || null
}

/**
 * Professional weekly candlestick chart with optional fair-value overlay.
 * Chart instance is created once; callbacks use refs to avoid teardown loops.
 */
export function WeeklyCandlestickChart({
  weeklyBars,
  fairValuePoints,
  height = 440,
  onCrosshairMove,
  onVisibleRangeChange,
  onVisibleTimeRangeChange,
  externalCrosshairTime = null,
  controlledVisibleRange = null,
  autoFit = true,
  chartRef: externalChartRef,
  className = '',
}) {
  const containerRef = React.useRef(null)
  const chartRef = React.useRef(null)
  const candleRef = React.useRef(null)
  const fairRef = React.useRef(null)
  const fittedRef = React.useRef(false)
  const barsKeyRef = React.useRef('')
  const skipCrosshairEmitRef = React.useRef(false)
  const userZoomedRef = React.useRef(false)
  const barsRef = React.useRef([])

  const onCrosshairMoveRef = React.useRef(onCrosshairMove)
  const onVisibleRangeChangeRef = React.useRef(onVisibleRangeChange)
  const onVisibleTimeRangeChangeRef = React.useRef(onVisibleTimeRangeChange)
  onCrosshairMoveRef.current = onCrosshairMove
  onVisibleRangeChangeRef.current = onVisibleRangeChange
  onVisibleTimeRangeChangeRef.current = onVisibleTimeRangeChange

  const imperativeRef = externalChartRef ?? React.useRef(null)

  React.useImperativeHandle(
    imperativeRef,
    () => ({
      fitContent: () => {
        userZoomedRef.current = false
        chartRef.current?.timeScale()?.fitContent()
      },
      resetZoom: () => {
        userZoomedRef.current = false
        chartRef.current?.timeScale()?.fitContent()
      },
      setVisibleTimeRange: (range) => {
        if (!range || !chartRef.current) return
        chartRef.current.timeScale().setVisibleRange(range)
      },
      clearCrosshair: () => chartRef.current?.clearCrosshairPosition(),
    }),
    [],
  )

  React.useEffect(() => {
    const el = containerRef.current
    if (!el) return undefined

    let chart
    try {
      chart = createChart(el, {
        width: Math.max(el.clientWidth, 1),
        height,
        layout: {
          background: { color: WS_CHART.background },
          textColor: WS_CHART.text,
          fontFamily: CHART_WS.fontFamily,
          fontSize: CHART_WS.axisFontSize,
        },
        grid: {
          vertLines: { color: WS_CHART.grid },
          horzLines: { color: WS_CHART.grid },
        },
        rightPriceScale: {
          borderColor: WS_CHART.border,
          scaleMargins: { top: 0.08, bottom: 0.06 },
        },
        timeScale: {
          borderColor: WS_CHART.border,
          timeVisible: true,
          secondsVisible: false,
          rightOffset: 8,
          barSpacing: 7,
          minBarSpacing: 1.5,
          fixLeftEdge: false,
          fixRightEdge: false,
        },
        crosshair: {
          mode: CrosshairMode.Normal,
          vertLine: {
            color: WS_CHART.crosshair,
            width: 1,
            style: 2,
            labelBackgroundColor: '#1e293b',
          },
          horzLine: {
            color: WS_CHART.crosshair,
            width: 1,
            style: 2,
            labelBackgroundColor: '#1e293b',
          },
        },
        handleScroll: {
          mouseWheel: true,
          pressedMouseMove: true,
          horzTouchDrag: true,
          vertTouchDrag: false,
        },
        handleScale: {
          mouseWheel: true,
          pinch: true,
          axisPressedMouseMove: { time: true, price: true },
        },
        kineticScroll: { touch: true, mouse: true },
      })
    } catch (err) {
      console.error('[workstation] createChart failed', err)
      return undefined
    }

    const candleSeries = chart.addCandlestickSeries({
      upColor: WS_CHART.up,
      downColor: WS_CHART.down,
      borderUpColor: WS_CHART.up,
      borderDownColor: WS_CHART.down,
      wickUpColor: WS_CHART.wickUp,
      wickDownColor: WS_CHART.wickDown,
    })

    const fairSeries = chart.addLineSeries({
      color: WS_CHART.fairValue,
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: true,
      title: 'Fair value',
      crosshairMarkerVisible: true,
      crosshairMarkerRadius: 4,
    })

    chartRef.current = chart
    candleRef.current = candleSeries
    fairRef.current = fairSeries
    fittedRef.current = false
    userZoomedRef.current = false

    const ro = new ResizeObserver(() => {
      if (containerRef.current && chartRef.current) {
        chartRef.current.applyOptions({ width: Math.max(containerRef.current.clientWidth, 1) })
      }
    })
    ro.observe(el)

    chart.subscribeCrosshairMove((param) => {
      if (skipCrosshairEmitRef.current) {
        skipCrosshairEmitRef.current = false
        return
      }
      const handler = onCrosshairMoveRef.current
      if (!handler) return
      if (!param?.time) {
        handler(null)
        return
      }
      const candle = param.seriesData.get(candleSeries)
      const fair = param.seriesData.get(fairSeries)
      handler({
        time: param.time,
        candle: candle || null,
        fairValue: fair?.value ?? null,
      })
    })

    const emitVisible = () => {
      const ts = chart.timeScale()
      const timeRange = ts.getVisibleRange()
      if (timeRange && onVisibleTimeRangeChangeRef.current) {
        onVisibleTimeRangeChangeRef.current(timeRange)
      }
      if (onVisibleRangeChangeRef.current) {
        onVisibleRangeChangeRef.current(ts.getVisibleLogicalRange())
      }
    }

    chart.timeScale().subscribeVisibleLogicalRangeChange(emitVisible)

    return () => {
      ro.disconnect()
      chart.remove()
      chartRef.current = null
      candleRef.current = null
      fairRef.current = null
    }
  }, [height])

  React.useEffect(() => {
    if (!candleRef.current) return

    const data = prepareLightweightCandles(weeklyBars)
    barsRef.current = data

    try {
      candleRef.current.setData(data)
    } catch (err) {
      console.error('[workstation] candlestick setData failed', err, { bars: data.length })
      return
    }

    const el = containerRef.current
    if (el && chartRef.current && el.clientWidth > 0) {
      chartRef.current.applyOptions({ width: el.clientWidth, height })
    }

    const key = data.length ? `${data[0].time}-${data.length}` : ''
    if (data.length && autoFit && !userZoomedRef.current && (!fittedRef.current || barsKeyRef.current !== key)) {
      barsKeyRef.current = key
      fittedRef.current = true
      try {
        chartRef.current?.timeScale().fitContent()
      } catch (err) {
        console.error('[workstation] fitContent failed', err)
      }
    }
  }, [weeklyBars, height, autoFit])

  React.useEffect(() => {
    if (!fairRef.current) return
    const data = prepareLightweightLinePoints(fairValuePoints)
    try {
      fairRef.current.setData(data)
    } catch (err) {
      console.error('[workstation] fair-value setData failed', err, { points: data.length })
    }
  }, [fairValuePoints])

  React.useEffect(() => {
    if (!chartRef.current || !controlledVisibleRange) return
    skipCrosshairEmitRef.current = true
    try {
      chartRef.current.timeScale().setVisibleRange(controlledVisibleRange)
    } catch (err) {
      console.error('[workstation] setVisibleRange failed', err)
    }
  }, [controlledVisibleRange])

  React.useEffect(() => {
    if (!chartRef.current || !candleRef.current) return
    if (externalCrosshairTime == null) {
      chartRef.current.clearCrosshairPosition()
      return
    }
    const bar = findBarByTime(barsRef.current, externalCrosshairTime)
    if (!bar) return
    skipCrosshairEmitRef.current = true
    try {
      chartRef.current.setCrosshairPosition(bar.close, bar.time, candleRef.current)
    } catch (err) {
      console.error('[workstation] setCrosshairPosition failed', err)
    }
  }, [externalCrosshairTime])

  return <div ref={containerRef} className={`irw-candle-chart ${className}`.trim()} />
}
