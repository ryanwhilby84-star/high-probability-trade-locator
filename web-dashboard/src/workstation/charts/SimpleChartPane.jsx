import React from 'react'
import { createChart } from 'lightweight-charts'

import { cotDateToBarTime } from '../../charts/positioningTimelineAlign.js'
import {
  prepareLightweightCandles,
  prepareLightweightLinePoints,
} from '../data/prepareLightweightCandles.js'
import { createWorkstationChartOptions } from './workstationChartOptions.js'

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
  registerPane,
  onCrosshairMove,
  onCrosshairClear,
  externalCrosshairTime = null,
  className = '',
}) {
  const containerRef = React.useRef(null)
  const chartRef = React.useRef(null)
  const primaryRef = React.useRef(null)
  const anchorRef = React.useRef(null)
  const zeroRef = React.useRef(null)
  const candlesRef = React.useRef([])
  const lineRef = React.useRef([])
  const skipEmitRef = React.useRef(false)

  const onCrosshairMoveRef = React.useRef(onCrosshairMove)
  const onCrosshairClearRef = React.useRef(onCrosshairClear)
  onCrosshairMoveRef.current = onCrosshairMove
  onCrosshairClearRef.current = onCrosshairClear

  React.useEffect(() => {
    const el = containerRef.current
    if (!el) return undefined

    let chart
    try {
      chart = createChart(
        el,
        createWorkstationChartOptions({
          width: Math.max(el.clientWidth, 1),
          height: Math.max(el.clientHeight, 1),
          showTimeAxis,
          interactionEnabled: true,
        }),
      )
    } catch (err) {
      console.error('[cot-workstation] createChart failed', panelId, err)
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
      primarySeries = chart.addLineSeries({
        color: lineColor,
        lineWidth: 1.75,
        priceLineVisible: false,
        lastValueVisible: true,
        crosshairMarkerVisible: true,
        crosshairMarkerRadius: 4,
      })
    }

    let zeroSeries = null
    if (zeroLine && mode === 'line') {
      zeroSeries = chart.addLineSeries({
        color: 'rgba(203, 213, 225, 0.5)',
        lineWidth: 1,
        lineStyle: 2,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      })
    }

    chartRef.current = chart
    primaryRef.current = primarySeries
    anchorRef.current = anchorSeries
    zeroRef.current = zeroSeries

    const ro = new ResizeObserver(() => {
      if (!containerRef.current || !chartRef.current) return
      const { clientWidth, clientHeight } = containerRef.current
      chartRef.current.applyOptions({
        width: Math.max(clientWidth, 1),
        height: Math.max(clientHeight, 1),
      })
    })
    ro.observe(el)

    const unregister =
      registerPane?.(panelId, {
        chart,
        primarySeries,
        valueAtTime: (time) => {
          if (mode === 'candle') {
            return findCandleAtTime(candlesRef.current, time)?.close ?? 0
          }
          return findValueAtTime(lineRef.current, time) ?? 0
        },
        onCrosshairMove: (param) => {
          if (skipEmitRef.current) return
          if (!param?.time) {
            onCrosshairClearRef.current?.()
            return
          }
          onCrosshairMoveRef.current?.({
            time: param.time,
            panelId,
            candle: mode === 'candle' ? findCandleAtTime(candlesRef.current, param.time) : null,
            value:
              mode === 'candle'
                ? findCandleAtTime(candlesRef.current, param.time)?.close ?? null
                : findValueAtTime(lineRef.current, param.time),
          })
        },
        onCrosshairClear: () => onCrosshairClearRef.current?.(),
      }) || (() => {})

    return () => {
      ro.disconnect()
      unregister()
      chart.remove()
      chartRef.current = null
      primaryRef.current = null
      anchorRef.current = null
      zeroRef.current = null
    }
  }, [panelId, mode, showTimeAxis, lineColor, zeroLine, registerPane])

  React.useEffect(() => {
    if (!anchorRef.current) return
    try {
      anchorRef.current.setData(buildAnchorPoints(timelineRows))
    } catch (err) {
      console.error('[cot-workstation] anchor setData failed', panelId, err)
    }
  }, [timelineRows, panelId])

  React.useEffect(() => {
    if (!primaryRef.current) return
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
    if (!chartRef.current || !primaryRef.current) return
    if (externalCrosshairTime == null) return
    const value =
      mode === 'candle'
        ? findCandleAtTime(candlesRef.current, externalCrosshairTime)?.close
        : findValueAtTime(lineRef.current, externalCrosshairTime)
    skipEmitRef.current = true
    try {
      if (value == null) {
        chartRef.current.setCrosshairPosition(0, externalCrosshairTime, anchorRef.current)
      } else {
        chartRef.current.setCrosshairPosition(value, externalCrosshairTime, primaryRef.current)
      }
    } catch {
      /* ignore */
    }
    skipEmitRef.current = false
  }, [externalCrosshairTime, mode])

  return (
    <div className={`ws-chart-pane ${className}`.trim()} data-panel={panelId}>
      <div className="ws-chart-pane-plot">
        <div className="ws-chart-pane-canvas" ref={containerRef} />
      </div>
    </div>
  )
}

export function dateToBarTime(timelineRows, date) {
  return cotDateToBarTime(timelineRows, date)
}
