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
  CAMERA_DEFAULTS,
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
  applyVerticalCameraToPane,
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

function defaultVerticalCamera() {
  return { factor: VERTICAL_STRETCH_DEFAULTS.factor, panOffset: 0 }
}

/**
 * Master camera controller.
 *
 * Horizontal model: ONE shared camera (barSpacing + rightOffset) applied to every
 * pane. Panes are passive — they never own horizontal navigation, so there is a
 * single source of truth and no chart-to-chart echo. Gestures mutate the shared
 * camera and it is broadcast to all panes.
 *
 * Vertical model: each pane owns an INDEPENDENT vertical camera `{ factor, panOffset }`
 * stored in `verticalCamerasRef` keyed by panelId. Adjusting one pane's vertical
 * camera only re-applies that pane's autoscale provider — the other panes' Y ranges
 * are never touched. `factor === 1 && panOffset === 0` == Fit Y for the visible range.
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

  // Independent per-pane vertical cameras + coalesced per-pane vertical pan deltas.
  const verticalCamerasRef = React.useRef(new Map())
  const pendingVerticalPanRef = React.useRef(new Map())

  const rows = timelineRowsRef?.current ?? timelineRowsRefInternal.current
  timelineRowsRefInternal.current = rows
  homeWeeksRef.current = homeWeeks

  const onCrosshairTimeRef = React.useRef(onCrosshairTime)
  const onCrosshairClearRef = React.useRef(onCrosshairClear)
  onCrosshairTimeRef.current = onCrosshairTime
  onCrosshairClearRef.current = onCrosshairClear

  const getVerticalCamera = React.useCallback((panelId) => {
    let cam = verticalCamerasRef.current.get(panelId)
    if (!cam) {
      cam = defaultVerticalCamera()
      verticalCamerasRef.current.set(panelId, cam)
    }
    return cam
  }, [])

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

  // Re-apply a single pane's independent vertical camera. Never touches other panes.
  const pushVerticalCamera = React.useCallback(
    (panelId) => {
      const pane = panesRef.current.get(panelId)
      if (!pane) return
      applyVerticalCameraToPane(pane, getVerticalCamera(panelId))
      bumpGeometry()
    },
    [bumpGeometry, getVerticalCamera],
  )

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
      // Ignore updates that are effectively identical — avoids float oscillation
      // and redundant broadcasts.
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

  // Imperatively scroll every pane so the latest bar sits `rightOffset` bars from
  // the right edge. Home/All/preset only mutate barSpacing+rightOffset via
  // applyOptions, which is a no-op when those values are unchanged — so after new
  // COT weeks are appended (or on instrument switch) the view stays pinned to the
  // previous right-most bar and the newest week is hidden. scrollToPosition forces
  // a re-anchor to the latest bar regardless. Panning/zoom are untouched.
  const anchorPanesToLatest = React.useCallback(() => {
    const offset = cameraRef.current?.rightOffset ?? CAMERA_DEFAULTS.rightOffset
    for (const pane of panesRef.current.values()) {
      try {
        pane.chart.timeScale().scrollToPosition(offset, false)
      } catch {
        /* ignore stale chart */
      }
    }
  }, [])

  const goHome = React.useCallback(
    (weeks = homeWeeksRef.current) => {
      const timelineRows = timelineRowsRefInternal.current
      if (!timelineRows.length) return
      const plotWidth = getPlotWidth()
      const next = cameraForWeekWindow(timelineRows, weeks, plotWidth)
      if (next) {
        setCamera(next, { force: true })
        anchorPanesToLatest()
      }
    },
    [getPlotWidth, setCamera, anchorPanesToLatest],
  )

  const goAll = React.useCallback(() => {
    const timelineRows = timelineRowsRefInternal.current
    if (!timelineRows.length) return
    const plotWidth = getPlotWidth()
    const next = cameraShowAll(timelineRows, plotWidth)
    if (next) {
      setCamera(next, { force: true })
      anchorPanesToLatest()
    }
  }, [getPlotWidth, setCamera, anchorPanesToLatest])

  const goPreset = React.useCallback(
    (weeks) => {
      const timelineRows = timelineRowsRefInternal.current
      if (!timelineRows.length) return
      const plotWidth = getPlotWidth()
      const next =
        weeks == null
          ? cameraShowAll(timelineRows, plotWidth)
          : cameraForWeekWindow(timelineRows, weeks, plotWidth)
      if (next) {
        setCamera(next, { force: true })
        anchorPanesToLatest()
      }
    },
    [getPlotWidth, setCamera, anchorPanesToLatest],
  )

  const resetCamera = React.useCallback(() => {
    cameraRef.current = null
    verticalCamerasRef.current.clear()
    pendingVerticalPanRef.current.clear()
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

  // Y-axis drag on a pane → vertically scale ONLY that pane.
  const adjustVerticalMagnification = React.useCallback(
    (deltaYPixels, panelId) => {
      if (!deltaYPixels || !panelId) return
      const cam = getVerticalCamera(panelId)
      const next = magnifyByAxisDrag(cam.factor, deltaYPixels)
      if (verticalStretchEqual(cam.factor, next)) return
      cam.factor = next
      pushVerticalCamera(panelId)
    },
    [getVerticalCamera, pushVerticalCamera],
  )

  // Fit Y for one pane: reset its vertical camera to native visible-range autoscale.
  const fitVertical = React.useCallback(
    (panelId) => {
      if (!panelId) return
      const cam = getVerticalCamera(panelId)
      cam.factor = VERTICAL_STRETCH_DEFAULTS.factor
      cam.panOffset = 0
      pushVerticalCamera(panelId)
    },
    [getVerticalCamera, pushVerticalCamera],
  )

  const resetVerticalMagnification = React.useCallback(
    (panelId = 'all') => {
      if (panelId !== 'all') {
        fitVertical(panelId)
        return
      }
      verticalCamerasRef.current.clear()
      pendingVerticalPanRef.current.clear()
      for (const [id, pane] of panesRef.current) {
        applyVerticalCameraToPane(pane, getVerticalCamera(id))
      }
      bumpGeometry()
    },
    [bumpGeometry, fitVertical, getVerticalCamera],
  )

  const commitPendingVerticalPan = React.useMemo(
    () =>
      createRafCoalescer(() => {
        const pending = pendingVerticalPanRef.current
        if (pending.size === 0) return
        for (const [panelId, deltaY] of pending) {
          if (!deltaY) continue
          const pane = panesRef.current.get(panelId)
          const series = pane?.primarySeries
          const chart = pane?.chart
          if (!series || !chart || pane.syncOnly) continue
          try {
            const chartEl = chart.chartElement?.()
            const height = chartEl?.clientHeight ?? 0
            if (height < 40) continue
            const yTop = Math.round(height * 0.2)
            const yBottom = Math.round(height * 0.8)
            const priceTop = series.coordinateToPrice(yTop)
            const priceBottom = series.coordinateToPrice(yBottom)
            if (priceTop == null || priceBottom == null) continue
            const plotPx = Math.max(yBottom - yTop, 1)
            const pricePerPixel = (priceBottom - priceTop) / plotPx
            const cam = getVerticalCamera(panelId)
            cam.panOffset -= deltaY * pricePerPixel
            applyVerticalCameraToPane(pane, cam)
          } catch {
            /* ignore stale chart */
          }
        }
        pending.clear()
        bumpGeometry()
      }),
    [bumpGeometry, getVerticalCamera],
  )

  // Vertical plot-drag inside a pane → reposition ONLY that pane's line (Y offset).
  const panPaneVerticalByPixels = React.useCallback(
    (panelId, deltaYPixels) => {
      if (!panelId || !deltaYPixels) return
      const prev = pendingVerticalPanRef.current.get(panelId) ?? 0
      pendingVerticalPanRef.current.set(panelId, prev + deltaYPixels)
      commitPendingVerticalPan()
    },
    [commitPendingVerticalPan],
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
    const verticalCameras = {}
    for (const id of panesRef.current.keys()) {
      verticalCameras[id] = { ...getVerticalCamera(id) }
    }
    return {
      logicalRange,
      camera,
      verticalCameras,
      crosshairTime: lastCrosshairTimeRef.current,
      panes: panesRef.current,
    }
  }, [getVerticalCamera])

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

      // Apply this pane's own independent vertical camera (default = Fit Y).
      applyVerticalCameraToPane(api, getVerticalCamera(panelId))

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
    [bumpGeometry, getVerticalCamera, scheduleCrosshairSync, timelineRowsRef],
  )

  React.useEffect(() => {
    if (!import.meta.env?.DEV || typeof window === 'undefined') return undefined
    const snapshotVerticalCameras = () => {
      const out = {}
      for (const [id, cam] of verticalCamerasRef.current) out[id] = { ...cam }
      return out
    }
    window.__COT_WS_CAMERA__ = {
      getCamera: () => JSON.parse(JSON.stringify(cameraRef.current)),
      getVerticalCameras: snapshotVerticalCameras,
      getVerticalMagnification: snapshotVerticalCameras,
      getVerticalMetrics: () => {
        const out = {}
        for (const [id, pane] of panesRef.current) {
          out[id] = readPaneVerticalSpanPx(pane)
        }
        return out
      },
      fitVertical,
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
  }, [goAll, goHome, fitVertical, resetVerticalMagnification])

  return {
    registerPane,
    goHome,
    goAll,
    goPreset,
    resetCamera,
    panByPixels,
    panPaneVerticalByPixels,
    zoomAtClientX,
    adjustVerticalMagnification,
    fitVertical,
    resetVerticalMagnification,
    onDragStart,
    onDragEnd,
    getPaneChart,
    getViewportState,
    subscribeGeometry,
    defaultPresetId: POSITIONING_DEFAULT_RANGE_ID,
  }
}
