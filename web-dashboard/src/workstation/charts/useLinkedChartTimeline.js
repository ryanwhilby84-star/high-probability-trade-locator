import React from 'react'

import { createRafCoalescer } from './drawingViewport.js'

/**
 * One shared X-axis controller for all workstation panes.
 * Syncs visible logical range + crosshair without feedback loops.
 */
export function useLinkedChartTimeline() {
  const panesRef = React.useRef(new Map())
  const syncingRangeRef = React.useRef(false)
  const syncingCrosshairRef = React.useRef(false)
  const crosshairSourceRef = React.useRef(null)
  const fittedKeyRef = React.useRef('')
  const geometryListenersRef = React.useRef(new Set())

  const scheduleGeometryBump = React.useMemo(
    () =>
      createRafCoalescer(() => {
        for (const fn of geometryListenersRef.current) {
          try {
            fn()
          } catch {
            /* ignore */
          }
        }
      }),
    [],
  )

  const bumpGeometry = React.useCallback(() => {
    scheduleGeometryBump()
  }, [scheduleGeometryBump])

  const subscribeGeometry = React.useCallback((listener) => {
    geometryListenersRef.current.add(listener)
    listener()
    return () => geometryListenersRef.current.delete(listener)
  }, [])

  const getPaneChart = React.useCallback((panelId) => {
    return panesRef.current.get(panelId)?.chart ?? null
  }, [])

  const registerPane = React.useCallback((panelId, api) => {
    if (!panelId || !api?.chart) return () => {}
    panesRef.current.set(panelId, api)
    bumpGeometry()

    const onRange = (range) => {
      if (!range || syncingRangeRef.current) return
      syncingRangeRef.current = true
      for (const [id, pane] of panesRef.current) {
        if (id === panelId) continue
        try {
          pane.chart.timeScale().setVisibleLogicalRange(range)
        } catch {
          /* ignore stale chart */
        }
      }
      syncingRangeRef.current = false
      bumpGeometry()
    }

    const onCrosshair = (param) => {
      if (syncingCrosshairRef.current) return

      if (!param?.time) {
        crosshairSourceRef.current = null
        syncingCrosshairRef.current = true
        for (const pane of panesRef.current.values()) {
          try {
            pane.chart.clearCrosshairPosition()
          } catch {
            /* ignore */
          }
        }
        syncingCrosshairRef.current = false
        api.onCrosshairClear?.()
        return
      }

      crosshairSourceRef.current = panelId
      syncingCrosshairRef.current = true
      for (const [id, pane] of panesRef.current) {
        if (id === panelId) continue
        const value = pane.valueAtTime?.(param.time)
        if (value == null) {
          try {
            pane.chart.clearCrosshairPosition()
          } catch {
            /* ignore */
          }
          continue
        }
        try {
          pane.chart.setCrosshairPosition(value, param.time, pane.primarySeries)
        } catch {
          /* ignore */
        }
      }
      syncingCrosshairRef.current = false
      api.onCrosshairMove?.(param)
    }

    chartSubscribe(api.chart, onRange, onCrosshair)

    const chartEl = api.chart.chartElement?.()
    const ro = chartEl ? new ResizeObserver(bumpGeometry) : null
    if (chartEl) ro.observe(chartEl)

    return () => {
      ro?.disconnect()
      const current = panesRef.current.get(panelId)
      if (current?.chart === api.chart) {
        panesRef.current.delete(panelId)
      }
      if (crosshairSourceRef.current === panelId) crosshairSourceRef.current = null
    }
  }, [bumpGeometry])

  const fitAll = React.useCallback((key = '') => {
    if (key && fittedKeyRef.current === key) return
    fittedKeyRef.current = key
    syncingRangeRef.current = true
    for (const pane of panesRef.current.values()) {
      try {
        pane.chart.timeScale().fitContent()
      } catch {
        /* ignore */
      }
    }
    syncingRangeRef.current = false
    bumpGeometry()
  }, [bumpGeometry])

  const setExternalCrosshair = React.useCallback((time) => {
    if (!time) {
      syncingCrosshairRef.current = true
      for (const pane of panesRef.current.values()) {
        try {
          pane.chart.clearCrosshairPosition()
        } catch {
          /* ignore */
        }
      }
      syncingCrosshairRef.current = false
      crosshairSourceRef.current = null
      return
    }

    syncingCrosshairRef.current = true
    crosshairSourceRef.current = '__external__'
    for (const pane of panesRef.current.values()) {
      const value = pane.valueAtTime?.(time)
      if (value == null) continue
      try {
        pane.chart.setCrosshairPosition(value, time, pane.primarySeries)
      } catch {
        /* ignore */
      }
    }
    syncingCrosshairRef.current = false
  }, [])

  const setInteractionEnabled = React.useCallback((enabled) => {
    const scroll = {
      mouseWheel: enabled,
      pressedMouseMove: enabled,
      horzTouchDrag: enabled,
      vertTouchDrag: false,
    }
    const scale = {
      mouseWheel: enabled,
      pinch: enabled,
      axisPressedMouseMove: { time: false, price: true },
    }
    for (const pane of panesRef.current.values()) {
      try {
        pane.chart.applyOptions({
          handleScroll: scroll,
          handleScale: scale,
          kineticScroll: { touch: enabled, mouse: enabled },
        })
      } catch {
        /* ignore */
      }
    }
  }, [])

  return {
    registerPane,
    fitAll,
    setExternalCrosshair,
    setInteractionEnabled,
    getPaneChart,
    subscribeGeometry,
  }
}

function chartSubscribe(chart, onRange, onCrosshair) {
  chart.timeScale().subscribeVisibleLogicalRangeChange(onRange)
  chart.subscribeCrosshairMove(onCrosshair)
}
