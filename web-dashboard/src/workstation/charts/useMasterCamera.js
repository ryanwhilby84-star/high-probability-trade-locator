import React from 'react'

import { PANEL_IDS } from '../../charts/chartTheme.js'
import { createRafCoalescer } from './drawingViewport.js'
import {
  applyCameraToPanes,
  readPlotWidthFromChart,
  readChartWidthFromChart,
  plotXFromClientX,
} from './cameraViewport.js'
import {
  cameraForWeekWindow,
  cameraShowAll,
  camerasEqual,
  clampStretchCamera,
  panCameraByPixels,
  cameraLogicalRange,
  zoomCameraAtPixel,
} from './masterCamera.js'
import { snapToTimelineTime } from './cotViewportUtils.js'
import { POSITIONING_DEFAULT_RANGE_ID } from '../../cot/positioningChartMetrics.js'
import { WS_PRICE_SCALE_WIDTH } from './workstationChartOptions.js'
import {
  VERTICAL_STRETCH_DEFAULTS,
  verticalStretchEqual,
  magnifyByAxisDrag,
} from './verticalStretch.js'
import {
  applyVerticalMagnificationToPane,
  applyVerticalMagnificationToPanes,
  readPaneVerticalSpanPx,
} from './verticalStretchViewport.js'

const PLOT_GUTTER = WS_PRICE_SCALE_WIDTH + 20

function plotAtClientX(clientX, containerEl, leadChart) {
  const fromChart = leadChart ? plotXFromClientX(clientX, leadChart) : null
  if (fromChart != null) return { pixelX: fromChart, plotWidth: readPlotWidthFromChart(leadChart) }
  if (!containerEl) return null
  const bodies = containerEl.querySelectorAll('.cot-ws-panel-body')
  for (const body of bodies) {
    const rect = body.getBoundingClientRect()
    if (clientX >= rect.left && clientX <= rect.right) {
      const plotWidth = Math.max(rect.width - PLOT_GUTTER, 80)
      return { pixelX: clientX - rect.left, plotWidth }
    }
  }
  return null
}

function syncCrosshairToAll(panes, time, sourcePanelId = null) {
  for (const [id, pane] of panes) {
    if (sourcePanelId && id === sourcePanelId) continue
    const value = pane.valueAtTime?.(time)
    const series = value != null ? pane.primarySeries : pane.anchorSeries
    if (!series) continue
    try {
      pane.chart.setCrosshairPosition(value != null ? value : 0, time, series)
    } catch {
      /* ignore */
    }
  }
}

function leadPaneChart(panes) {
  const preferred = [
    PANEL_IDS.commercial,
    PANEL_IDS.institutional,
    PANEL_IDS.retail,
    PANEL_IDS.price,
  ]
  for (const id of preferred) {
    const pane = panes.get(id)
    if (pane?.chart) return pane.chart
  }
  return panes.values().next().value?.chart ?? null
}

/**
 * Master camera controller — single horizontal viewport, passive chart panes.
 */
export function useMasterCamera({
  timelineRowsRef,
  onCrosshairTime,
  onCrosshairClear,
  homeWeeks = 156,
} = {}) {
  const panesRef = React.useRef(new Map())
  const applyingRef = React.useRef(false)
  const syncingCrosshairRef = React.useRef(false)
  const crosshairSourceRef = React.useRef(null)
  const lastCrosshairTimeRef = React.useRef(null)
  const geometryListenersRef = React.useRef(new Set())
  const timelineRowsRefInternal = React.useRef(timelineRowsRef?.current ?? [])
  const cameraRef = React.useRef(null)
  const homeWeeksRef = React.useRef(homeWeeks)
  const draggingRef = React.useRef(false)
  const pendingPanRef = React.useRef(0)
  const pendingZoomRef = React.useRef(null)
  const priceVerticalRef = React.useRef(VERTICAL_STRETCH_DEFAULTS.factor)
  const cotVerticalRef = React.useRef(VERTICAL_STRETCH_DEFAULTS.factor)
  const pricePanOffsetRef = React.useRef(0)
  const pendingPricePanRef = React.useRef(0)

  const rows = timelineRowsRef?.current ?? timelineRowsRefInternal.current
  timelineRowsRefInternal.current = rows
  homeWeeksRef.current = homeWeeks

  const onCrosshairTimeRef = React.useRef(onCrosshairTime)
  const onCrosshairClearRef = React.useRef(onCrosshairClear)
  onCrosshairTimeRef.current = onCrosshairTime
  onCrosshairClearRef.current = onCrosshairClear

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

  const emitCrosshairTime = React.useMemo(
    () =>
      createRafCoalescer(() => {
        const time = lastCrosshairTimeRef.current
        if (time == null) return
        onCrosshairTimeRef.current?.(time)
      }),
    [],
  )

  const getPlotWidth = React.useCallback(() => {
    const chart = leadPaneChart(panesRef.current)
    return readPlotWidthFromChart(chart)
  }, [])

  const pushVerticalMagnification = React.useCallback(() => {
    if (panesRef.current.size === 0) return
    applyVerticalMagnificationToPanes(panesRef.current, {
      priceFactor: priceVerticalRef.current,
      cotFactor: cotVerticalRef.current,
      pricePanOffset: pricePanOffsetRef.current,
    })
    if (cameraRef.current) {
      applyingRef.current = true
      applyCameraToPanes(panesRef.current, cameraRef.current)
      applyingRef.current = false
    }
    bumpGeometry()
  }, [bumpGeometry])

  const pushCamera = React.useCallback(() => {
    const camera = cameraRef.current
    if (!camera || panesRef.current.size === 0) return
    applyingRef.current = true
    applyCameraToPanes(panesRef.current, camera)
    applyingRef.current = false
    bumpGeometry()
  }, [bumpGeometry])

  const setCamera = React.useCallback(
    (nextCamera, { force = false } = {}) => {
      if (!nextCamera) return
      const plotWidth = getPlotWidth()
      const clamped = clampStretchCamera(nextCamera, plotWidth)
      if (!force && camerasEqual(cameraRef.current, clamped)) return
      cameraRef.current = clamped
      pushCamera()
    },
    [getPlotWidth, pushCamera],
  )

  const commitPendingPan = React.useMemo(
    () =>
      createRafCoalescer(() => {
        const delta = pendingPanRef.current
        pendingPanRef.current = 0
        if (!delta) return
        const camera = cameraRef.current
        if (!camera) return
        const next = panCameraByPixels(camera, delta)
        setCamera(next)
      }),
    [setCamera],
  )

  const commitPendingZoom = React.useMemo(
    () =>
      createRafCoalescer(() => {
        const pending = pendingZoomRef.current
        pendingZoomRef.current = null
        if (!pending) return
        const timelineRows = timelineRowsRefInternal.current
        const camera = cameraRef.current
        if (!camera || !timelineRows.length) return
        const lead = leadPaneChart(panesRef.current)
        const plotWidth = readPlotWidthFromChart(lead)
        const chartWidth = readChartWidthFromChart(lead)
        const next = zoomCameraAtPixel(
          camera,
          timelineRows,
          plotWidth,
          chartWidth,
          pending.pixelX,
          pending.zoomIn,
          { intensity: pending.intensity },
        )
        setCamera(next)
      }),
    [setCamera],
  )

  const scheduleCrosshairSync = React.useMemo(
    () =>
      createRafCoalescer(() => {
        const time = lastCrosshairTimeRef.current
        const source = crosshairSourceRef.current
        if (time == null) return
        syncingCrosshairRef.current = true
        syncCrosshairToAll(panesRef.current, time, source)
        emitCrosshairTime()
        syncingCrosshairRef.current = false
      }),
    [emitCrosshairTime],
  )

  const goHome = React.useCallback(
    (weeks = homeWeeksRef.current) => {
      const timelineRows = timelineRowsRefInternal.current
      if (!timelineRows.length) return
      const plotWidth = getPlotWidth()
      const next = cameraForWeekWindow(timelineRows, weeks, plotWidth)
      if (next) setCamera(next, { force: true })
    },
    [getPlotWidth, setCamera],
  )

  const goAll = React.useCallback(() => {
    const timelineRows = timelineRowsRefInternal.current
    if (!timelineRows.length) return
    const plotWidth = getPlotWidth()
    const next = cameraShowAll(timelineRows, plotWidth)
    if (next) setCamera(next, { force: true })
  }, [getPlotWidth, setCamera])

  const goPreset = React.useCallback(
    (weeks) => {
      const timelineRows = timelineRowsRefInternal.current
      if (!timelineRows.length) return
      const plotWidth = getPlotWidth()
      const next =
        weeks == null
          ? cameraShowAll(timelineRows, plotWidth)
          : cameraForWeekWindow(timelineRows, weeks, plotWidth)
      if (next) setCamera(next, { force: true })
    },
    [getPlotWidth, setCamera],
  )

  const resetCamera = React.useCallback(() => {
    cameraRef.current = null
    priceVerticalRef.current = VERTICAL_STRETCH_DEFAULTS.factor
    cotVerticalRef.current = VERTICAL_STRETCH_DEFAULTS.factor
    pricePanOffsetRef.current = 0
    lastCrosshairTimeRef.current = null
    crosshairSourceRef.current = null
    for (const pane of panesRef.current.values()) {
      try {
        pane.chart.clearCrosshairPosition()
      } catch {
        /* ignore */
      }
    }
  }, [])

  const panByPixels = React.useCallback(
    (deltaXPixels) => {
      if (!deltaXPixels) return
      pendingPanRef.current += deltaXPixels
      commitPendingPan()
    },
    [commitPendingPan],
  )

  const resetVerticalMagnification = React.useCallback(
    (zone = 'all') => {
      let changed = false
      if (zone === 'all' || zone === 'price') {
        if (!verticalStretchEqual(priceVerticalRef.current, VERTICAL_STRETCH_DEFAULTS.factor)) {
          priceVerticalRef.current = VERTICAL_STRETCH_DEFAULTS.factor
          changed = true
        }
      }
      if (zone === 'all' || zone === 'cot') {
        if (!verticalStretchEqual(cotVerticalRef.current, VERTICAL_STRETCH_DEFAULTS.factor)) {
          cotVerticalRef.current = VERTICAL_STRETCH_DEFAULTS.factor
          changed = true
        }
      }
      if (changed) pushVerticalMagnification()
    },
    [pushVerticalMagnification],
  )

  const adjustVerticalMagnification = React.useCallback(
    (deltaYPixels, zone = 'cot') => {
      if (!deltaYPixels) return
      const targetRef = zone === 'price' ? priceVerticalRef : cotVerticalRef
      const next = magnifyByAxisDrag(targetRef.current, deltaYPixels)
      if (verticalStretchEqual(targetRef.current, next)) return
      targetRef.current = next
      pushVerticalMagnification()
    },
    [pushVerticalMagnification],
  )

  const commitPendingPricePan = React.useMemo(
    () =>
      createRafCoalescer(() => {
        const deltaY = pendingPricePanRef.current
        pendingPricePanRef.current = 0
        if (!deltaY) return
        const pane = panesRef.current.get(PANEL_IDS.price)
        const series = pane?.primarySeries
        const chart = pane?.chart
        if (!series || !chart) return
        try {
          const chartEl = chart.chartElement?.()
          const height = chartEl?.clientHeight ?? 0
          if (height < 40) return
          const yTop = Math.round(height * 0.2)
          const yBottom = Math.round(height * 0.8)
          const priceTop = series.coordinateToPrice(yTop)
          const priceBottom = series.coordinateToPrice(yBottom)
          if (priceTop == null || priceBottom == null) return
          const plotPx = Math.max(yBottom - yTop, 1)
          const pricePerPixel = (priceBottom - priceTop) / plotPx
          pricePanOffsetRef.current -= deltaY * pricePerPixel
          pushVerticalMagnification()
        } catch {
          /* ignore stale chart */
        }
      }),
    [pushVerticalMagnification],
  )

  const panPriceByPixels = React.useCallback(
    (deltaYPixels) => {
      if (!deltaYPixels) return
      pendingPricePanRef.current += deltaYPixels
      commitPendingPricePan()
    },
    [commitPendingPricePan],
  )

  const zoomAtClientX = React.useCallback(
    (clientX, containerEl, zoomIn, { intensity = 1 } = {}) => {
      const timelineRows = timelineRowsRefInternal.current
      const camera = cameraRef.current
      if (!camera || !timelineRows.length || !containerEl) return
      const lead = leadPaneChart(panesRef.current)
      const plot = plotAtClientX(clientX, containerEl, lead)
      if (!plot) return
      pendingZoomRef.current = {
        pixelX: plot.pixelX,
        plotWidth: plot.plotWidth,
        zoomIn,
        intensity,
      }
      commitPendingZoom()
    },
    [commitPendingZoom],
  )

  const onDragStart = React.useCallback(() => {
    draggingRef.current = true
    lastCrosshairTimeRef.current = null
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
    onCrosshairClearRef.current?.()
  }, [])

  const onDragEnd = React.useCallback(() => {
    draggingRef.current = false
  }, [])

  const subscribeGeometry = React.useCallback((listener) => {
    geometryListenersRef.current.add(listener)
    requestAnimationFrame(() => {
      if (geometryListenersRef.current.has(listener)) listener()
    })
    return () => geometryListenersRef.current.delete(listener)
  }, [])

  const getPaneChart = React.useCallback((panelId) => {
    return panesRef.current.get(panelId)?.chart ?? null
  }, [])

  const getViewportState = React.useCallback(() => {
    const timelineRows = timelineRowsRefInternal.current
    const camera = cameraRef.current
    const logicalRange =
      camera && timelineRows.length
        ? (() => {
            const { from, to } = cameraLogicalRange(camera, timelineRows)
            return { from, to }
          })()
        : null
    return {
      logicalRange,
      camera,
      verticalMagnification: {
        price: priceVerticalRef.current,
        cot: cotVerticalRef.current,
        pricePanOffset: pricePanOffsetRef.current,
      },
      crosshairTime: lastCrosshairTimeRef.current,
      panes: panesRef.current,
    }
  }, [])

  const registerPane = React.useCallback(
    (panelId, api) => {
      if (!panelId || !api?.chart) return () => {}
      panesRef.current.set(panelId, api)
      bumpGeometry()

      const timelineRows = timelineRowsRefInternal.current
      if (!cameraRef.current && timelineRows.length) {
        const plotWidth = readPlotWidthFromChart(api.chart)
        cameraRef.current = cameraForWeekWindow(
          timelineRows,
          homeWeeksRef.current,
          plotWidth,
        )
      }

      if (cameraRef.current) {
        applyingRef.current = true
        applyCameraToPanes(panesRef.current, cameraRef.current)
        applyingRef.current = false
      }

      applyVerticalMagnificationToPane(api, {
        priceFactor: priceVerticalRef.current,
        cotFactor: cotVerticalRef.current,
        pricePanOffset: pricePanOffsetRef.current,
      })

      if (lastCrosshairTimeRef.current != null) {
        syncingCrosshairRef.current = true
        syncCrosshairToAll(panesRef.current, lastCrosshairTimeRef.current, panelId)
        syncingCrosshairRef.current = false
      }

      const onCrosshair = (param) => {
        if (syncingCrosshairRef.current || draggingRef.current) return
        if (!param?.time) {
          if (lastCrosshairTimeRef.current == null) return
          lastCrosshairTimeRef.current = null
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
          onCrosshairClearRef.current?.()
          return
        }
        const rows = timelineRowsRef?.current ?? timelineRowsRefInternal.current
        const snapped = snapToTimelineTime(param.time, rows)
        if (lastCrosshairTimeRef.current === snapped && crosshairSourceRef.current === panelId) {
          return
        }
        lastCrosshairTimeRef.current = snapped
        crosshairSourceRef.current = panelId
        scheduleCrosshairSync()
      }

      api.chart.subscribeCrosshairMove(onCrosshair)

      const chartEl = api.chart.chartElement?.()
      const ro = chartEl ? new ResizeObserver(bumpGeometry) : null
      if (chartEl) ro.observe(chartEl)

      return () => {
        ro?.disconnect()
        try {
          api.chart.unsubscribeCrosshairMove(onCrosshair)
        } catch {
          /* ignore */
        }
        const current = panesRef.current.get(panelId)
        if (current?.chart === api.chart) panesRef.current.delete(panelId)
        if (crosshairSourceRef.current === panelId) crosshairSourceRef.current = null
      }
    },
    [bumpGeometry, scheduleCrosshairSync, timelineRowsRef],
  )

  React.useEffect(() => {
    if (!import.meta.env?.DEV || typeof window === 'undefined') return undefined
    window.__COT_WS_CAMERA__ = {
      getCamera: () => JSON.parse(JSON.stringify(cameraRef.current)),
      getVerticalMagnification: () => ({
        price: priceVerticalRef.current,
        cot: cotVerticalRef.current,
      }),
      getVerticalStretch: () => ({
        price: priceVerticalRef.current,
        cot: cotVerticalRef.current,
      }),
      getVerticalMetrics: () => {
        const out = {}
        for (const [id, pane] of panesRef.current) {
          out[id] = readPaneVerticalSpanPx(pane)
        }
        return out
      },
      resetVerticalMagnification,
      getTimelineBounds: () => {
        const rows = timelineRowsRefInternal.current
        if (!rows.length) return null
        return { first: rows[0].time, last: rows[rows.length - 1].time, count: rows.length }
      },
      getPaneRanges: () => {
        const out = {}
        for (const [id, pane] of panesRef.current) {
          try {
            out[id] = {
              range: pane.chart.timeScale().getVisibleRange(),
              logical: pane.chart.timeScale().getVisibleLogicalRange(),
              scrollPosition: pane.chart.timeScale().scrollPosition(),
              scale: pane.chart.timeScale().options(),
            }
          } catch {
            out[id] = null
          }
        }
        return out
      },
      goAll,
      goHome,
    }
    return () => {
      delete window.__COT_WS_CAMERA__
    }
  }, [goAll, goHome, resetVerticalMagnification])

  return {
    registerPane,
    goHome,
    goAll,
    goPreset,
    resetCamera,
    panByPixels,
    panPriceByPixels,
    zoomAtClientX,
    adjustVerticalMagnification,
    resetVerticalMagnification,
    onDragStart,
    onDragEnd,
    getPaneChart,
    getViewportState,
    subscribeGeometry,
    defaultPresetId: POSITIONING_DEFAULT_RANGE_ID,
  }
}
