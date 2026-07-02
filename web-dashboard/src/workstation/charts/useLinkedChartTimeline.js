import React from 'react'

import { PANEL_IDS } from '../../charts/chartTheme.js'
import { createRafCoalescer } from './drawingViewport.js'
import {
  applyMasterViewport,
  applyLogicalRangeToLead,
  clampLogicalRange,
  logicalRangeFitAll,
  logicalRangeForWeeks,
  rangesEqual,
  readTimeScale,
  scalesEqual,
  snapToTimelineTime,
} from './cotViewportUtils.js'
import { recordFitAll } from './cotWsRenderDiagnostics.js'

const DEFAULT_SCALE = { barSpacing: 7, rightOffset: 8 }

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

function leadChart(panes) {
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
 * Master timeline controller.
 *
 * One shared logical range + time-scale scale (barSpacing/rightOffset).
 * Panes report pan/zoom; master commits once and pushes to every pane.
 * Lightweight Charts handles wheel/drag natively — no custom handlers.
 */
export function useLinkedChartTimeline({ timelineRowsRef, onCrosshairTime, onCrosshairClear } = {}) {
  const panesRef = React.useRef(new Map())
  const applyingRef = React.useRef(false)
  const syncingCrosshairRef = React.useRef(false)
  const crosshairSourceRef = React.useRef(null)
  const lastCrosshairTimeRef = React.useRef(null)
  const geometryListenersRef = React.useRef(new Set())
  const timelineRowsRefInternal = React.useRef(timelineRowsRef?.current ?? [])
  const masterRef = React.useRef({
    logicalRange: null,
    scale: { ...DEFAULT_SCALE },
    rowCount: 0,
    windowWeeks: null,
  })
  const pendingActionRef = React.useRef(null)
  const blockCommitsUntilRef = React.useRef(0)

  const blockPaneCommits = React.useCallback((ms = 150) => {
    blockCommitsUntilRef.current = performance.now() + ms
  }, [])

  const rows = timelineRowsRef?.current ?? timelineRowsRefInternal.current
  timelineRowsRefInternal.current = rows
  if (rows.length > 0) {
    masterRef.current.rowCount = rows.length
  }

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

  const pushMaster = React.useCallback(() => {
    const master = masterRef.current
    if (!master.logicalRange || panesRef.current.size === 0) return
    applyingRef.current = true
    applyMasterViewport(panesRef.current, master)
    applyingRef.current = false
    bumpGeometry()
  }, [bumpGeometry])

  const commitFromPane = React.useCallback(
    (chart, range) => {
      if (applyingRef.current || !range) return
      const master = masterRef.current
      const rowCount = master.rowCount || timelineRowsRefInternal.current.length
      const clamped = clampLogicalRange(range, rowCount)
      const scale = readTimeScale(chart)
      if (
        rangesEqual(master.logicalRange, clamped) &&
        scalesEqual(master.scale, scale)
      ) {
        return
      }
      master.logicalRange = clamped
      master.scale = scale
      pushMaster()
    },
    [pushMaster],
  )

  const pendingCommitRef = React.useRef(null)

  const flushCommit = React.useMemo(
    () =>
      createRafCoalescer(() => {
        const pending = pendingCommitRef.current
        if (!pending) return
        pendingCommitRef.current = null
        commitFromPane(pending.chart, pending.range)
      }),
    [commitFromPane],
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

  const runPendingAction = React.useCallback(() => {
    const action = pendingActionRef.current
    if (!action || panesRef.current.size === 0) return
    pendingActionRef.current = null
    pendingCommitRef.current = null
    if (action.type === 'fitAll') {
      recordFitAll()
      const range = logicalRangeFitAll(action.rowCount)
      if (!range) return
      masterRef.current.rowCount = action.rowCount
      masterRef.current.windowWeeks = null
      applyingRef.current = true
      const lead = leadChart(panesRef.current)
      const rows = timelineRowsRefInternal.current
      if (lead) {
        const fitted = applyLogicalRangeToLead(lead, range, action.rowCount, {
          timelineRows: rows,
          forceRequestedRange: true,
        })
        masterRef.current.logicalRange = fitted.range
        masterRef.current.scale = fitted.scale
      } else {
        masterRef.current.logicalRange = range
      }
      applyMasterViewport(panesRef.current, masterRef.current)
      applyingRef.current = false
      blockPaneCommits()
    } else if (action.type === 'window') {
      const range = logicalRangeForWeeks(action.rowCount, action.weeks)
      if (!range) return
      masterRef.current.rowCount = action.rowCount
      masterRef.current.windowWeeks = action.weeks
      applyingRef.current = true
      const lead = leadChart(panesRef.current)
      const rows = timelineRowsRefInternal.current
      if (lead) {
        const fitted = applyLogicalRangeToLead(lead, range, action.rowCount, { timelineRows: rows })
        masterRef.current.logicalRange = fitted.range
        masterRef.current.scale = fitted.scale
      } else {
        masterRef.current.logicalRange = clampLogicalRange(range, action.rowCount) ?? range
      }
      applyMasterViewport(panesRef.current, masterRef.current)
      applyingRef.current = false
      blockPaneCommits()
    }
    bumpGeometry()
  }, [bumpGeometry, blockPaneCommits])

  const fitAllRows = React.useCallback(
    (rowCount, { force = false } = {}) => {
      if (!rowCount || rowCount <= 0) return
      const master = masterRef.current
      if (
        !force &&
        master.windowWeeks == null &&
        master.rowCount === rowCount &&
        master.logicalRange &&
        rangesEqual(master.logicalRange, logicalRangeFitAll(rowCount))
      ) {
        return
      }
      pendingActionRef.current = { type: 'fitAll', rowCount }
      if (panesRef.current.size === 0) return
      runPendingAction()
    },
    [runPendingAction],
  )

  const showWindow = React.useCallback(
    (rowCount, weeks, { force = false } = {}) => {
      if (!rowCount || rowCount <= 0) return
      const master = masterRef.current
      const range = logicalRangeForWeeks(rowCount, weeks)
      if (!force && rangesEqual(master.logicalRange, range) && master.rowCount === rowCount) return
      pendingActionRef.current = { type: 'window', rowCount, weeks }
      if (panesRef.current.size === 0) return
      runPendingAction()
    },
    [runPendingAction],
  )

  const resetViewport = React.useCallback(() => {
    masterRef.current = {
      logicalRange: null,
      scale: { ...DEFAULT_SCALE },
      rowCount: 0,
      windowWeeks: null,
    }
    pendingActionRef.current = null
    pendingCommitRef.current = null
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
    return {
      logicalRange: masterRef.current.logicalRange,
      crosshairTime: lastCrosshairTimeRef.current,
      panes: panesRef.current,
    }
  }, [])

  const registerPane = React.useCallback(
    (panelId, api) => {
      if (!panelId || !api?.chart) return () => {}
      panesRef.current.set(panelId, api)
      bumpGeometry()

      if (pendingActionRef.current) {
        runPendingAction()
      } else if (masterRef.current.logicalRange) {
        applyingRef.current = true
        applyMasterViewport(panesRef.current, masterRef.current)
        applyingRef.current = false
      }

      if (lastCrosshairTimeRef.current != null) {
        syncingCrosshairRef.current = true
        syncCrosshairToAll(panesRef.current, lastCrosshairTimeRef.current, panelId)
        syncingCrosshairRef.current = false
      }

      const onRange = (range) => {
        if (applyingRef.current || !range) return
        if (performance.now() < blockCommitsUntilRef.current) return
        pendingCommitRef.current = { chart: api.chart, range }
        flushCommit()
      }

      const onCrosshair = (param) => {
        if (syncingCrosshairRef.current) return
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

      api.chart.timeScale().subscribeVisibleLogicalRangeChange(onRange)
      api.chart.subscribeCrosshairMove(onCrosshair)

      const chartEl = api.chart.chartElement?.()
      const ro = chartEl ? new ResizeObserver(bumpGeometry) : null
      if (chartEl) ro.observe(chartEl)

      return () => {
        ro?.disconnect()
        try {
          api.chart.timeScale().unsubscribeVisibleLogicalRangeChange(onRange)
          api.chart.unsubscribeCrosshairMove(onCrosshair)
        } catch {
          /* ignore */
        }
        const current = panesRef.current.get(panelId)
        if (current?.chart === api.chart) panesRef.current.delete(panelId)
        if (crosshairSourceRef.current === panelId) crosshairSourceRef.current = null
      }
    },
    [bumpGeometry, flushCommit, runPendingAction, scheduleCrosshairSync, timelineRowsRef],
  )

  const setExternalCrosshair = React.useCallback(
    (time) => {
      if (time == null) {
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
      lastCrosshairTimeRef.current = snapToTimelineTime(time, rows)
      crosshairSourceRef.current = '__external__'
      syncingCrosshairRef.current = true
      syncCrosshairToAll(panesRef.current, lastCrosshairTimeRef.current)
      syncingCrosshairRef.current = false
      emitCrosshairTime()
    },
    [emitCrosshairTime, timelineRowsRef],
  )

  const setInteractionEnabled = React.useCallback((enabled) => {
    const scroll = {
      mouseWheel: false,
      pressedMouseMove: enabled,
      horzTouchDrag: enabled,
      vertTouchDrag: false,
    }
    const scale = {
      mouseWheel: enabled,
      pinch: enabled,
      axisPressedMouseMove: { time: enabled, price: false },
      axisDoubleClickReset: { time: false, price: false },
    }
    for (const pane of panesRef.current.values()) {
      try {
        pane.chart.applyOptions({
          handleScroll: scroll,
          handleScale: scale,
          kineticScroll: { touch: enabled, mouse: false },
        })
      } catch {
        /* ignore */
      }
    }
  }, [])

  const fitAllRowsRef = React.useRef(fitAllRows)
  fitAllRowsRef.current = fitAllRows

  React.useEffect(() => {
    if (!import.meta.env?.DEV || typeof window === 'undefined') return undefined
    window.__COT_WS_TIMELINE__ = {
      getMaster: () => JSON.parse(JSON.stringify(masterRef.current)),
      getPaneRanges: () => {
        const out = {}
        for (const [id, pane] of panesRef.current) {
          try {
            const range = pane.chart.timeScale().getVisibleLogicalRange()
            const scale = readTimeScale(pane.chart)
            out[id] = { range, scale }
          } catch {
            out[id] = null
          }
        }
        return out
      },
      fitAll: (rowCount) =>
        fitAllRowsRef.current(rowCount || masterRef.current.rowCount, { force: true }),
    }
    return () => {
      delete window.__COT_WS_TIMELINE__
    }
  }, [])

  return {
    registerPane,
    fitAllRows,
    showWindow,
    resetViewport,
    setExternalCrosshair,
    setInteractionEnabled,
    getPaneChart,
    getViewportState,
    subscribeGeometry,
    /** @deprecated use fitAllRows */
    fitAll: (key, opts) => {
      const rowCount = Number(String(key).split(':').pop())
      if (rowCount > 0) fitAllRows(rowCount, opts)
    },
  }
}
