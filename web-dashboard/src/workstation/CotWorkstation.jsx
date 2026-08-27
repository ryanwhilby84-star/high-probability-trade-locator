import React from 'react'

import { CHART_WS, PANEL_IDS } from '../charts/chartTheme.js'
import { buildCotWorkstation } from '../cot/buildCotWorkstation.js'
import {
  POSITIONING_DEFAULT_RANGE_ID,
  POSITIONING_RANGE_PRESETS,
  rangePresetById,
  fmtValue,
  fmtDelta,
} from '../cot/positioningChartMetrics.js'
import { useCot3ySeries, resolveCot3yBlock } from '../hooks/useCot3ySeries.js'
import { COT_3Y_PATH } from '../data/cot3ySeriesStore.js'
import { reloadCot3ySeries } from '../prices/stores/HistoricalCOTStore.js'
import { useLivePrice } from '../prices/usePriceStores.js'
import { useWorkstationOhlc } from './hooks/useWorkstationOhlc.js'
import { buildPositioningWorkstationSeries } from './data/buildPositioningWorkstationSeries.js'
import {
  labelFromTimelineTime,
  rowsToLinePoints,
} from './charts/buildWorkstationTimelineData.js'
import { useMasterCamera } from './charts/useMasterCamera.js'
import { useMasterCameraGestures } from './charts/useMasterCameraGestures.js'
import { useGlobalVerticalMagnification } from './charts/useGlobalVerticalMagnification.js'
import { SimpleChartPane } from './charts/SimpleChartPane.jsx'
import {
  WS_COT_PLOT_HEIGHT,
  WS_COT_RETAIL_PLOT_HEIGHT,
  WS_PRICE_PLOT_HEIGHT,
  WS_PANE_FLEX,
  WS_PANE_FLEX_BOUNDS,
} from './charts/workstationPanelSizing.js'
import { CotPanelResizeHandle } from './charts/CotPanelResizeHandle.jsx'
import { CotDrawingCoordinator } from './charts/CotDrawingCoordinator.jsx'
import { useCotWorkstationReady } from './charts/useCotWorkstationReady.js'
import {
  bumpRender,
  logDiagSnapshot,
  resetDiagCounters,
  setDiagInstrument,
} from './charts/cotWsRenderDiagnostics.js'
import { findTimeForDate } from './intelMarkers.js'
import {
  DEFAULT_LAYER_STATE,
  eventLayerIds,
  eventMatchesLayers,
  toResearchPins,
} from './researchEventUi.js'
import {
  ResearchEventNavigator,
  ResearchLayerBar,
  ResearchMarkerLegend,
  WeeklyHoverTooltip,
} from './ResearchWorkstationChrome.jsx'
import {
  buildWeeklyViewModel,
  researchEventId,
  resolveInspectedWeek,
} from './data/buildWeeklyViewModel.js'
import { resolveWeeklyInspectorBlock } from './data/expandWeeklyInspector.js'
import { WeeklyInspector } from './WeeklyInspector.jsx'
import {
  WeeklyAnalysisPanel,
  resolveWeeklyAnalysisBlock,
} from './WeeklyAnalysisPanel.jsx'

import '../charts/positioningChart.css'
import './cotWorkstation.css'

const PRICE_BODY_HEIGHT = WS_PRICE_PLOT_HEIGHT
const COT_BODY_HEIGHT = WS_COT_PLOT_HEIGHT

function formatYearsWeeks(weeks) {
  if (!weeks || weeks <= 0) return '0w'
  if (weeks >= 52) return `${(weeks / 52).toFixed(1)}Y`
  return `${weeks}w`
}

function buildVisibleSummary({ preset, rangeId, visibleWeeks, totalCotWeeks }) {
  const span = formatYearsWeeks(visibleWeeks)

  if (rangeId === 'all') {
    return visibleWeeks >= totalCotWeeks
      ? `${visibleWeeks} weeks · All available (${span})`
      : `${visibleWeeks} weeks · All (${span} of ${totalCotWeeks})`
  }

  if ((rangeId === '10y' || rangeId === '5y') && visibleWeeks < (preset.weeks ?? visibleWeeks)) {
    return `${visibleWeeks} weeks · ${preset.label} (${span} available)`
  }

  return `${visibleWeeks} weeks · ${preset.label}`
}

function deltaDirection(delta) {
  if (delta == null || !Number.isFinite(delta) || delta === 0) return 'flat'
  return delta > 0 ? 'up' : 'down'
}

/** Change over `weeksBack` completed reports, or null when history is too short. */
function changeOverWeeks(points, weeksBack) {
  const n = points?.length ?? 0
  if (n < 2) return null
  const last = points[n - 1]
  const prior = points[n - 1 - weeksBack]
  if (
    !last ||
    !prior ||
    typeof last.value !== 'number' ||
    typeof prior.value !== 'number'
  ) {
    return null
  }
  return last.value - prior.value
}

/** Compact legend badge: current value + 12W / 4W / 1W change over completed reports. */
function toValueBadge(points) {
  if (!points?.length) return null

  const last = points[points.length - 1]
  const windows = [
    { key: '12W', weeks: 12 },
    { key: '4W', weeks: 4 },
    { key: '1W', weeks: 1 },
  ]

  return {
    valueText: fmtValue(last.value),
    changes: windows.map(({ key, weeks }) => {
      const delta = changeOverWeeks(points, weeks)
      return {
        key,
        text: fmtDelta(delta),
        dir: deltaDirection(delta),
      }
    }),
  }
}

function PaneShell({
  panelId,
  label,
  className = '',
  bodyHeight = null,
  children,
}) {
  return (
    <section
      className={`cot-ws-panel ${className}`.trim()}
      data-panel={panelId}
      aria-label={label}
    >
      <div
        className="cot-ws-panel-body"
        style={bodyHeight != null ? { height: bodyHeight } : undefined}
      >
        {children}
      </div>
    </section>
  )
}

function WorkstationSkeleton({ message = 'Loading chart…' }) {
  return (
    <div
      className="cot-ws-chart-skeleton cot-ws-chart-skeleton--panel"
      aria-hidden="true"
    >
      <span className="cot-ws-chart-skeleton-label">{message}</span>
    </div>
  )
}

export function CotWorkstation({ marketId, variant = 'default' }) {
  const { doc, loading, errored } = useCot3ySeries()
  const { exportBlock, exportLoaded } = useWorkstationOhlc(marketId)
  const livePriceState = useLivePrice(marketId)

  const [rangeId, setRangeId] = React.useState(POSITIONING_DEFAULT_RANGE_ID)
  const [viewportEndLabel, setViewportEndLabel] = React.useState(null)
  /** Positioning research — on-chart event layers (any supported COT market). */
  const [researchBlock, setResearchBlock] = React.useState(null)
  /** Compact weekly percentile/flow series — merged without clearing selection. */
  const [weeklyInspectorBlock, setWeeklyInspectorBlock] = React.useState(null)
  const [weeklyAnalysisBlock, setWeeklyAnalysisBlock] = React.useState(null)
  const [weeklyAnalysisOpen, setWeeklyAnalysisOpen] = React.useState(false)
  const [layerState, setLayerState] = React.useState(DEFAULT_LAYER_STATE)
  /** Separate selection states — week may have 0..N events. */
  const [selectedWeek, setSelectedWeek] = React.useState(null)
  const [selectedEventId, setSelectedEventId] = React.useState(null)
  const [hoveredWeek, setHoveredWeek] = React.useState(null)
  const [eventNavCollapsed, setEventNavCollapsed] = React.useState(true)

  // Live price/COT height split (percent of the fitted surface). The draggable
  // splitter mutates only this value; the COT group takes the remainder and its
  // three panes redistribute evenly. Restoring WS_PANE_FLEX.price = "reset layout".
  const [priceFlex, setPriceFlex] = React.useState(WS_PANE_FLEX.price)

  const handleSplitterDrag = React.useCallback((deltaY, containerHeight) => {
    if (!containerHeight) return
    setPriceFlex((prev) => {
      const next = prev + (deltaY / containerHeight) * 100
      return Math.min(
        WS_PANE_FLEX_BOUNDS.priceMax,
        Math.max(WS_PANE_FLEX_BOUNDS.priceMin, next),
      )
    })
  }, [])

  const crosshairLabelRef = React.useRef(null)
  const panelsStackRef = React.useRef(null)
  const visibleRowsRef = React.useRef([])

  setDiagInstrument(marketId)
  bumpRender('CotWorkstation')

  const { block, matchedKey } = React.useMemo(
    () => resolveCot3yBlock(doc, marketId),
    [doc, marketId],
  )

  const model = React.useMemo(() => {
    if (!block) return null

    try {
      return buildCotWorkstation(block)
    } catch (error) {
      console.error('[cot-workstation] buildCotWorkstation failed', marketId, error)
      return {
        available: false,
        error: String(error?.message || error),
      }
    }
  }, [block, marketId])

  const binding = React.useMemo(() => {
    if (!model?.available) return null

    return buildPositioningWorkstationSeries(model, null, exportBlock, {
      preserveFullCotHistory: true,
    })
  }, [model, exportBlock])

  const timelineRows = binding?.rows ?? []
  const visibleBars = binding?.weeklyBars ?? []
  const totalCotWeeks = binding?.meta?.cotWeeks ?? timelineRows.length

  visibleRowsRef.current = timelineRows

  const commercialLinePoints = React.useMemo(
    () => rowsToLinePoints(timelineRows, 'commercial_net'),
    [timelineRows],
  )
  const institutionalLinePoints = React.useMemo(
    () => rowsToLinePoints(timelineRows, 'institutional_net'),
    [timelineRows],
  )
  const retailLinePoints = React.useMemo(
    () => rowsToLinePoints(timelineRows, 'retail_net'),
    [timelineRows],
  )

  const commercialBadge = React.useMemo(
    () => toValueBadge(commercialLinePoints),
    [commercialLinePoints],
  )
  const institutionalBadge = React.useMemo(
    () => toValueBadge(institutionalLinePoints),
    [institutionalLinePoints],
  )
  const retailBadge = React.useMemo(
    () => toValueBadge(retailLinePoints),
    [retailLinePoints],
  )

  const {
    chartsReady,
    cotSettled,
    ohlcSettled,
    cotDataReady,
  } = useCotWorkstationReady({
    marketId,
    cotLoading: loading,
    cotDoc: doc,
    cotBlock: block,
    modelAvailable: Boolean(model?.available),
    visibleRowCount: timelineRows.length,
    ohlcExportLoaded: exportLoaded,
  })

  const hasAnyOhlc =
    ohlcSettled && (binding?.weeklyBars?.length ?? 0) > 0
  const hasVisibleOhlc = visibleBars.length > 0
  const isFullscreen = variant === 'fullscreen'
  const priceBodyHeight = hasAnyOhlc ? PRICE_BODY_HEIGHT : 72

  const ohlcPartial = Boolean(
    hasAnyOhlc &&
      (binding?.meta?.incompleteHistory ||
        (binding?.meta?.alignedOhlcWeeks ?? 0) < totalCotWeeks),
  )

  const preset = React.useMemo(() => rangePresetById(rangeId), [rangeId])

  const latestTimelineRow = timelineRows[timelineRows.length - 1] ?? null
  const plottedLatestDate =
    latestTimelineRow?.date || latestTimelineRow?.label || null
  const latestMarkerTime = latestTimelineRow?.time ?? null

  const loadedLatestDate =
    block?.latest_date ??
    (Array.isArray(block?.series) && block.series.length
      ? block.series[block.series.length - 1]?.date
      : null) ??
    null

  const staleView =
    Boolean(loadedLatestDate && plottedLatestDate && viewportEndLabel) &&
    !(
      loadedLatestDate === plottedLatestDate &&
      plottedLatestDate === viewportEndLabel
    )

  const setCrosshairLabel = React.useCallback((text) => {
    if (crosshairLabelRef.current) {
      crosshairLabelRef.current.textContent = text || '—'
    }
  }, [])

  React.useEffect(() => {
    setCrosshairLabel(plottedLatestDate || '—')
  }, [plottedLatestDate, marketId, setCrosshairLabel])

  const allResearchEvents = React.useMemo(
    () => (Array.isArray(researchBlock?.markers) ? researchBlock.markers : []),
    [researchBlock],
  )

  const visibleResearchEvents = React.useMemo(
    () => allResearchEvents.filter((e) => eventMatchesLayers(e, layerState)),
    [allResearchEvents, layerState],
  )

  const eventCounts = React.useMemo(() => {
    const counts = {
      commercial_extremes: 0,
      commercial_rotations: 0,
      noncommercial_extremes: 0,
      noncommercial_rotations: 0,
      divergence: 0,
      nr_extremes: 0,
    }
    for (const e of allResearchEvents) {
      for (const id of eventLayerIds(e)) {
        if (counts[id] != null) counts[id] += 1
      }
    }
    return counts
  }, [allResearchEvents])

  const dateByTime = React.useMemo(() => {
    const map = new Map()
    for (const row of timelineRows) {
      if (row?.time != null) {
        map.set(row.time, String(row.date || row.label || '').slice(0, 10))
      }
    }
    return map
  }, [timelineRows])

  const dataStale = Boolean(
    loadedLatestDate &&
      plottedLatestDate &&
      loadedLatestDate !== plottedLatestDate,
  )

  const onCrosshairTime = React.useCallback(
    (time) => {
      setCrosshairLabel(labelFromTimelineTime(visibleRowsRef.current, time))
      const date = dateByTime.get(time) || null
      setHoveredWeek(date)
    },
    [setCrosshairLabel, dateByTime],
  )

  const onCrosshairClear = React.useCallback(() => {
    const latest = visibleRowsRef.current[visibleRowsRef.current.length - 1]
    setCrosshairLabel(latest?.date || latest?.label || '—')
    setHoveredWeek(null)
  }, [setCrosshairLabel])

  const {
    registerPane,
    goHome,
    goAll,
    goPreset,
    goToTime,
    resetCamera,
    panByPixels,
    panPaneVerticalByPixels,
    zoomAtClientX,
    adjustVerticalMagnification,
    fitVertical,
    onDragStart,
    onDragEnd,
    subscribeGeometry,
    getViewportState,
  } = useMasterCamera({
    timelineRowsRef: visibleRowsRef,
    onCrosshairTime,
    onCrosshairClear,
    homeWeeks: rangePresetById(POSITIONING_DEFAULT_RANGE_ID).weeks,
  })

  // Clear selection only when the instrument changes — never on data refetch,
  // matchedKey resolution, percentile load, or marker regeneration.
  React.useEffect(() => {
    setHoveredWeek(null)
    setSelectedWeek(null)
    setSelectedEventId(null)
    setResearchBlock(null)
    setWeeklyInspectorBlock(null)
    setWeeklyAnalysisBlock(null)
    setWeeklyAnalysisOpen(false)
  }, [marketId])

  // Research markers — keep previous block until the new one arrives (no flash-clear).
  React.useEffect(() => {
    let cancelled = false
    fetch('/data/cot_positioning_research_latest.json', { cache: 'no-store' })
      .then((r) => (r.ok ? r.json() : null))
      .then((doc) => {
        if (cancelled || !doc?.markets) return
        const markets = doc.markets
        const block =
          markets[marketId] ||
          (matchedKey ? markets[matchedKey] : null) ||
          Object.entries(markets).find(
            ([k]) => String(k).toLowerCase() === String(marketId || '').toLowerCase(),
          )?.[1] ||
          null
        if (block?.available) setResearchBlock(block)
      })
      .catch(() => {
        /* keep prior researchBlock */
      })
    return () => {
      cancelled = true
    }
  }, [marketId, matchedKey])

  // Percentile/flow series — independent fetch so it cannot wipe markers/selection.
  React.useEffect(() => {
    let cancelled = false
    fetch('/data/cot_weekly_inspector_latest.json', { cache: 'no-store' })
      .then((r) => (r.ok ? r.json() : null))
      .then((doc) => {
        if (cancelled || !doc) return
        const weeklyInspector = resolveWeeklyInspectorBlock(doc, marketId, matchedKey)
        if (weeklyInspector) setWeeklyInspectorBlock(weeklyInspector)
      })
      .catch(() => {
        /* keep prior weeklyInspectorBlock */
      })
    return () => {
      cancelled = true
    }
  }, [marketId, matchedKey])

  // Weekly Analysis — narrative layer over inspector + research (no recalc).
  React.useEffect(() => {
    let cancelled = false
    fetch('/data/cot_analyst_intelligence_latest.json', { cache: 'no-store' })
      .then((r) => (r.ok ? r.json() : null))
      .then((doc) => {
        if (cancelled || !doc) return
        const block = resolveWeeklyAnalysisBlock(doc, marketId, matchedKey)
        if (block) setWeeklyAnalysisBlock(block)
      })
      .catch(() => {
        /* keep prior weeklyAnalysisBlock */
      })
    return () => {
      cancelled = true
    }
  }, [marketId, matchedKey])

  const researchWithInspector = React.useMemo(() => {
    if (!researchBlock) return null
    return weeklyInspectorBlock
      ? { ...researchBlock, weekly_inspector: weeklyInspectorBlock }
      : researchBlock
  }, [researchBlock, weeklyInspectorBlock])

  const weeklyModel = React.useMemo(
    () =>
      buildWeeklyViewModel({
        timelineRows,
        researchBlock: researchWithInspector,
        instrument: marketId,
        loadedLatestDate,
        staleView: dataStale,
      }),
    [timelineRows, researchWithInspector, marketId, loadedLatestDate, dataStale],
  )

  const weeklyView = weeklyModel.weeklyView

  const inspectorOpen = Boolean(selectedWeek)
  const inspectedWeek = React.useMemo(
    () =>
      inspectorOpen
        ? resolveInspectedWeek({
            weeklyView,
            selectedWeek,
            hoveredWeek: null,
            latestDate: null,
          })
        : null,
    [weeklyView, selectedWeek, inspectorOpen],
  )

  const hoveredWeekData = React.useMemo(
    () => (hoveredWeek && weeklyView?.[hoveredWeek] ? weeklyView[hoveredWeek] : null),
    [hoveredWeek, weeklyView],
  )

  const highlightWeek = selectedWeek || null
  const eventHighlightTime = React.useMemo(() => {
    if (!highlightWeek) return null
    return findTimeForDate(timelineRows, highlightWeek)
  }, [highlightWeek, timelineRows])

  // Pane routing: group-scoped events only (+ Comm↔NR divergence where intended).
  // Rotations stay on their participant pane — not duplicated on price.
  const commercialEvents = React.useMemo(
    () =>
      visibleResearchEvents.filter(
        (e) =>
          e.group === 'commercial' || e.event_type === 'comm_nr_divergence',
      ),
    [visibleResearchEvents],
  )
  const institutionalEvents = React.useMemo(
    () => visibleResearchEvents.filter((e) => e.group === 'noncommercial'),
    [visibleResearchEvents],
  )
  const retailEvents = React.useMemo(
    () =>
      visibleResearchEvents.filter(
        (e) =>
          e.group === 'nonreportable' || e.event_type === 'comm_nr_divergence',
      ),
    [visibleResearchEvents],
  )
  // Marker selection glow follows locked week only — hover stays tooltip-only.
  const pinSelectedDate = selectedWeek || null
  const commercialResearchPins = React.useMemo(
    () =>
      toResearchPins(
        commercialEvents,
        timelineRows,
        pinSelectedDate,
        selectedEventId,
      ),
    [commercialEvents, timelineRows, pinSelectedDate, selectedEventId],
  )
  const institutionalResearchPins = React.useMemo(
    () =>
      toResearchPins(
        institutionalEvents,
        timelineRows,
        pinSelectedDate,
        selectedEventId,
      ),
    [institutionalEvents, timelineRows, pinSelectedDate, selectedEventId],
  )
  const retailResearchPins = React.useMemo(
    () =>
      toResearchPins(retailEvents, timelineRows, pinSelectedDate, selectedEventId),
    [retailEvents, timelineRows, pinSelectedDate, selectedEventId],
  )
  // Price pane: compact overview for Commercial extremes + DIV only (no rotations).
  const priceEvents = React.useMemo(
    () =>
      visibleResearchEvents.filter((e) => {
        if (e.event_type === 'comm_nr_divergence') return true
        if (e.group !== 'commercial') return false
        return (
          e.event_type === 'absolute_extreme' || e.event_type === 'local_extreme'
        )
      }),
    [visibleResearchEvents],
  )
  const priceResearchPins = React.useMemo(
    () =>
      toResearchPins(priceEvents, timelineRows, pinSelectedDate, selectedEventId),
    [priceEvents, timelineRows, pinSelectedDate, selectedEventId],
  )

  const lockWeek = React.useCallback((date, eventId = null) => {
    const d = String(date || '').slice(0, 10)
    if (!d) return
    setSelectedWeek(d)
    setSelectedEventId(eventId)
  }, [])

  const handleTimeClick = React.useCallback(
    (timeOrPin) => {
      // Chart body click (unix time) or legacy pin time — lock week, never jump camera.
      if (timeOrPin && typeof timeOrPin === 'object') {
        lockWeek(timeOrPin.date, timeOrPin.eventId || null)
        return
      }
      const date = dateByTime.get(timeOrPin)
      if (!date) return
      lockWeek(date, null)
    },
    [dateByTime, lockWeek],
  )

  const handlePinClick = React.useCallback(
    (pin) => {
      if (!pin?.date) return
      // Marker click opens inspector only — no zoom / scroll change.
      lockWeek(pin.date, pin.eventId || null)
    },
    [lockWeek],
  )

  const handleResearchEventSelect = React.useCallback(
    (event) => {
      if (!event) return
      lockWeek(event.date, researchEventId(event))
    },
    [lockWeek],
  )

  const clearWeekSelection = React.useCallback(() => {
    setSelectedWeek(null)
    setSelectedEventId(null)
  }, [])

  const jumpToWeek = React.useCallback(
    (date) => {
      const time = findTimeForDate(timelineRows, date)
      if (time == null) return
      goToTime(time, 104)
    },
    [timelineRows, goToTime],
  )

  const handleInspectorEventSelect = React.useCallback(
    (event) => {
      if (!event) return
      lockWeek(event.date, event.id || researchEventId(event))
    },
    [lockWeek],
  )

  useMasterCameraGestures({
    containerRef: panelsStackRef,
    enabled: chartsReady,
    onPanDelta: panByPixels,
    onVerticalPanDelta: panPaneVerticalByPixels,
    onZoomAt: zoomAtClientX,
    onDragStart,
    onDragEnd,
  })

  useGlobalVerticalMagnification({
    containerRef: panelsStackRef,
    enabled: chartsReady,
    onMagnifyDelta: adjustVerticalMagnification,
    onFitY: fitVertical,
  })

  React.useEffect(() => {
    resetCamera()
    resetDiagCounters()
    setRangeId(POSITIONING_DEFAULT_RANGE_ID)
  }, [marketId, resetCamera])

  React.useEffect(() => {
    if (!chartsReady || timelineRows.length === 0) return

    goPreset(preset.weeks)

    if (import.meta.env?.DEV) {
      requestAnimationFrame(() => logDiagSnapshot('cot-ws-camera'))
    }
  }, [
    chartsReady,
    timelineRows.length,
    preset.weeks,
    rangeId,
    goPreset,
    marketId,
    plottedLatestDate,
  ])

  React.useEffect(() => {
    return subscribeGeometry(() => {
      try {
        const state = getViewportState()
        const panes = state?.panes
        const pane =
          panes?.get(PANEL_IDS.commercial) ||
          panes?.values?.().next?.().value ||
          null
        const visibleRange = pane?.chart?.timeScale?.().getVisibleRange?.()

        if (visibleRange?.to == null) return

        const rows = visibleRowsRef.current
        const matchedRow = rows.find((row) => row.time === visibleRange.to)

        setViewportEndLabel(
          matchedRow?.date ||
            matchedRow?.label ||
            labelFromTimelineTime(rows, visibleRange.to) ||
            String(visibleRange.to),
        )
      } catch {
        // Ignore stale chart references during remounts.
      }
    })
  }, [subscribeGeometry, getViewportState])

  const handleAll = React.useCallback(() => {
    setRangeId('all')
    goAll()
  }, [goAll])

  const handleHome = React.useCallback(() => {
    const defaultPreset = rangePresetById(POSITIONING_DEFAULT_RANGE_ID)
    setRangeId(POSITIONING_DEFAULT_RANGE_ID)
    goHome(defaultPreset.weeks)
  }, [goHome])

  const handlePreset = React.useCallback((nextRangeId) => {
    const nextPreset = rangePresetById(nextRangeId)
    setRangeId(nextRangeId)
    goPreset(nextPreset.weeks)
  }, [goPreset])

  const handleReload = React.useCallback(() => {
    Promise.resolve(reloadCot3ySeries()).finally(() => {
      const defaultPreset = rangePresetById(POSITIONING_DEFAULT_RANGE_ID)
      setRangeId(POSITIONING_DEFAULT_RANGE_ID)
      goHome(defaultPreset.weeks)
    })
  }, [goHome])

  const paneProps = React.useMemo(
    () => ({
      timelineRows,
      registerPane,
      chartsReady,
      passiveCamera: true,
      nativeWheelZoom: false,
    }),
    [timelineRows, registerPane, chartsReady],
  )

  if (loading && !doc) {
    return (
      <div className="cot-workstation cot-workstation--loading">
        <div className="cot-ws-status" role="status">
          Loading COT series from <code>{COT_3Y_PATH}</code>…
        </div>
      </div>
    )
  }

  if (cotSettled && errored && !doc) {
    return (
      <div className="cot-ws-status cot-ws-status--error">
        COT fetch failed for <strong>{marketId}</strong>. Check{' '}
        <code>{COT_3Y_PATH}</code>.
      </div>
    )
  }

  if (cotSettled && !cotDataReady) {
    return (
      <div className="cot-ws-status cot-ws-status--error">
        <p>
          No COT workstation data for <strong>{marketId}</strong>.
        </p>
        {model?.error ? (
          <p className="cot-ws-status-detail">{model.error}</p>
        ) : null}
        {!block && doc ? (
          <p className="cot-ws-status-detail">
            Market not found in {COT_3Y_PATH}.
          </p>
        ) : null}
      </div>
    )
  }

  const institutionalTitle = model.institutionalGroup || 'Non-Commercial'
  const retailTitle = model.retailGroup || 'Non-Reportable'

  const priceQuality = exportBlock?.price_quality
  const priceQualityWarning =
    priceQuality?.status && priceQuality.status !== 'PASS'
      ? `PRICE ${priceQuality.status}: ${
          priceQuality.warning || 'verify source/date'
        }`
      : null

  const priceRangeSubtitle = ohlcPartial
    ? `OHLC ${binding?.meta?.range?.ohlcFirst ?? '—'} → ${
        binding?.meta?.range?.ohlcLast ?? '—'
      } · COT full history`
    : null

  const priceSubtitle =
    [priceQualityWarning, priceRangeSubtitle].filter(Boolean).join(' · ') || null

  const visibleWeeks =
    preset.weeks == null
      ? totalCotWeeks
      : Math.min(preset.weeks, totalCotWeeks)

  const visibleSummary = buildVisibleSummary({
    preset,
    rangeId,
    visibleWeeks,
    totalCotWeeks,
  })

  const shellLoading = !chartsReady

  return (
    <div
      className={`cot-workstation positioning-chart-stack positioning-chart-stack--cot3y${
        isFullscreen ? ' cot-workstation--fullscreen' : ''
      }${hasAnyOhlc ? '' : ' cot-workstation--no-ohlc'}${
        shellLoading ? ' cot-workstation--loading' : ''
      }${inspectorOpen ? ' cot-workstation--inspector-open' : ''}`}
      data-market={marketId}
      data-charts-ready={chartsReady ? '1' : '0'}
      data-inspector-open={inspectorOpen ? '1' : '0'}
      style={{
        '--ws-price-flex': priceFlex,
        '--ws-cot-group-flex': 100 - priceFlex,
        '--ws-price-flex-no-ohlc': WS_PANE_FLEX.priceNoOhlc,
      }}
    >
      <header className="cot-ws-toolbar">
        <div className="cot-ws-toolbar-left">
          <span className="cot-ws-history">{model.historyLabel}</span>
          <span className="cot-ws-weeks">
            {shellLoading ? 'Preparing timeline…' : visibleSummary}
          </span>
        </div>

        <div className="cot-ws-toolbar-center">
          <span
            className="cot-ws-crosshair-label"
            ref={crosshairLabelRef}
          />
        </div>

        <div className="cot-ws-toolbar-right">
          {staleView ? (
            <button
              type="button"
              className="cot-ws-stale-view"
              onClick={handleReload}
              title="Chart is behind the latest published data — click to reload"
            >
              STALE
            </button>
          ) : null}

          <div
            className="cot-ws-range-toggles"
            role="group"
            aria-label="Chart range"
          >
            <button
              type="button"
              className="cot-ws-range-btn cot-ws-range-btn--fit"
              disabled={shellLoading}
              onClick={handleAll}
            >
              All
            </button>

            <button
              type="button"
              className="cot-ws-range-btn"
              disabled={shellLoading}
              onClick={handleHome}
            >
              Home
            </button>

            {POSITIONING_RANGE_PRESETS.map((rangePreset) => (
              <button
                key={rangePreset.id}
                type="button"
                className={`cot-ws-range-btn${
                  rangePreset.id === rangeId ? ' active' : ''
                }`}
                disabled={shellLoading}
                onClick={() => handlePreset(rangePreset.id)}
              >
                {rangePreset.label}
              </button>
            ))}
          </div>

          <button
            type="button"
            className="cot-ws-reload-btn"
            onClick={handleReload}
            disabled={shellLoading}
            title="Reload the latest published COT data"
            aria-label="Reload latest data"
          >
            ⟳
          </button>
        </div>
      </header>

      {researchBlock ? (
        <div className="cot-ws-research-chrome">
          <div className="cot-ws-research-bar-row">
            <ResearchLayerBar
              layerState={layerState}
              onChange={setLayerState}
              eventCounts={eventCounts}
            />
            <div className="cot-ws-research-chrome-actions">
              <button
                type="button"
                className={`cot-ws-analysis-btn${weeklyAnalysisOpen ? ' is-open' : ''}`}
                aria-pressed={weeklyAnalysisOpen}
                onClick={() => setWeeklyAnalysisOpen((v) => !v)}
                title="Open Weekly Analysis"
              >
                Analysis
              </button>
            </div>
          </div>
          <ResearchMarkerLegend />
          <ResearchEventNavigator
            events={visibleResearchEvents}
            selectedDate={selectedWeek}
            selectedEventId={selectedEventId}
            onSelect={handleResearchEventSelect}
            collapsed={eventNavCollapsed}
            onCollapsedChange={setEventNavCollapsed}
          />
        </div>
      ) : (
        <div className="cot-ws-research-chrome cot-ws-research-chrome--analysis-only">
          <div className="cot-ws-research-chrome-actions">
            <button
              type="button"
              className={`cot-ws-analysis-btn${weeklyAnalysisOpen ? ' is-open' : ''}`}
              aria-pressed={weeklyAnalysisOpen}
              onClick={() => setWeeklyAnalysisOpen((v) => !v)}
              title="Open Weekly Analysis"
            >
              Analysis
            </button>
          </div>
        </div>
      )}

      <WeeklyInspector
        week={inspectedWeek}
        open={inspectorOpen}
        selectedEventId={selectedEventId}
        onClose={clearWeekSelection}
        onJumpToWeek={jumpToWeek}
        onSelectEvent={handleInspectorEventSelect}
        analysisOpen={weeklyAnalysisOpen}
        onToggleAnalysis={() => setWeeklyAnalysisOpen((v) => !v)}
      />

      <div className="cot-ws-research-stage">
        <div className="cot-ws-canvas-scroll">
        <div
          className="cot-ws-panels cot-ws-panels--camera"
          ref={panelsStackRef}
        >
          <CotDrawingCoordinator
            subscribeGeometry={subscribeGeometry}
            panelsRef={panelsStackRef}
            getViewportState={getViewportState}
          />

          <PaneShell
            panelId={PANEL_IDS.price}
            label="Weekly OHLC price"
            className={`cot-ws-panel--price${
              hasAnyOhlc ? '' : ' cot-ws-panel--price-empty'
            }`}
            bodyHeight={isFullscreen ? undefined : priceBodyHeight}
          >
            {chartsReady ? (
              <SimpleChartPane
                {...paneProps}
                panelId={PANEL_IDS.price}
                mode="candle"
                legendLabel={
                  priceSubtitle ? `Weekly OHLC · ${priceSubtitle}` : 'Weekly OHLC'
                }
                candleBars={visibleBars}
                syncOnly={!hasVisibleOhlc}
                livePrice={
                  livePriceState.quote?.mid ??
                  livePriceState.streamPrice?.currentPrice ??
                  null
                }
                livePriceAsOf={livePriceState.quote?.asOf ?? null}
                livePriceSource={livePriceState.quote?.source ?? null}
                livePriceStale={livePriceState.status !== 'LIVE'}
                livePriceAgeMs={livePriceState.freshness?.ageMs ?? null}
                livePriceStatus={livePriceState.status}
                livePricePrecision={
                  livePriceState.quote?.pricePrecision ??
                  livePriceState.streamPrice?.pricePrecision ??
                  null
                }
                livePriceBid={livePriceState.quote?.bid ?? null}
                livePriceAsk={livePriceState.quote?.ask ?? null}
                livePriceProvider={
                  livePriceState.streamPrice?.provider ??
                  livePriceState.quote?.provider ??
                  null
                }
                livePriceSymbol={
                  livePriceState.quote?.symbol ??
                  livePriceState.streamPrice?.providerSymbol ??
                  null
                }
                activeWeeklyCandle={livePriceState.activeWeeklyCandle}
                latestMarkerTime={latestMarkerTime}
                latestMarkerLabel={plottedLatestDate}
                showLatestLabel
                researchPins={priceResearchPins}
                researchPinVariant="price"
                eventHighlightTime={eventHighlightTime}
                onTimeClick={handleTimeClick}
                onPinClick={handlePinClick}
                onFitY={fitVertical}
                emptyMessage={
                  hasAnyOhlc && !hasVisibleOhlc
                    ? 'No weekly OHLC in this range.'
                    : ohlcSettled
                      ? 'Weekly OHLC unavailable for this market.'
                      : 'Loading weekly OHLC…'
                }
              />
            ) : (
              <WorkstationSkeleton message="Preparing price chart…" />
            )}
          </PaneShell>

          {isFullscreen && hasAnyOhlc ? (
            <CotPanelResizeHandle onDragDelta={handleSplitterDrag} />
          ) : null}

          <div className="cot-ws-cot-group">
            <PaneShell
              panelId={PANEL_IDS.commercial}
              label="Commercial net positioning"
              className="cot-ws-panel--cot"
              bodyHeight={isFullscreen ? undefined : COT_BODY_HEIGHT}
            >
              {chartsReady ? (
                <SimpleChartPane
                  {...paneProps}
                  panelId={PANEL_IDS.commercial}
                  mode="line"
                  legendLabel="Commercial"
                  lineColor={CHART_WS.commercial}
                  linePoints={commercialLinePoints}
                  latestMarkerTime={latestMarkerTime}
                  researchPins={commercialResearchPins}
                  researchPinVariant="cot"
                  eventHighlightTime={eventHighlightTime}
                  onTimeClick={handleTimeClick}
                  onPinClick={handlePinClick}
                  valueBadge={commercialBadge}
                  onFitY={fitVertical}
                  zeroLine
                />
              ) : (
                <WorkstationSkeleton />
              )}
            </PaneShell>

            <PaneShell
              panelId={PANEL_IDS.institutional}
              label={`${institutionalTitle} net positioning`}
              className="cot-ws-panel--cot"
              bodyHeight={isFullscreen ? undefined : COT_BODY_HEIGHT}
            >
              {chartsReady ? (
                <SimpleChartPane
                  {...paneProps}
                  panelId={PANEL_IDS.institutional}
                  mode="line"
                  legendLabel={institutionalTitle}
                  lineColor={CHART_WS.institutional}
                  linePoints={institutionalLinePoints}
                  latestMarkerTime={latestMarkerTime}
                  researchPins={institutionalResearchPins}
                  researchPinVariant="cot"
                  eventHighlightTime={eventHighlightTime}
                  onTimeClick={handleTimeClick}
                  onPinClick={handlePinClick}
                  valueBadge={institutionalBadge}
                  onFitY={fitVertical}
                  zeroLine
                />
              ) : (
                <WorkstationSkeleton />
              )}
            </PaneShell>

            <PaneShell
              panelId={PANEL_IDS.retail}
              label={`${retailTitle} net positioning`}
              className="cot-ws-panel--cot cot-ws-panel--retail"
              bodyHeight={
                isFullscreen ? undefined : WS_COT_RETAIL_PLOT_HEIGHT
              }
            >
              {chartsReady ? (
                <SimpleChartPane
                  {...paneProps}
                  panelId={PANEL_IDS.retail}
                  mode="line"
                  showTimeAxis
                  legendLabel={retailTitle}
                  lineColor={CHART_WS.retail}
                  linePoints={retailLinePoints}
                  latestMarkerTime={latestMarkerTime}
                  researchPins={retailResearchPins}
                  researchPinVariant="cot"
                  eventHighlightTime={eventHighlightTime}
                  onTimeClick={handleTimeClick}
                  onPinClick={handlePinClick}
                  valueBadge={retailBadge}
                  onFitY={fitVertical}
                  zeroLine
                />
              ) : (
                <WorkstationSkeleton />
              )}
            </PaneShell>
          </div>
        </div>
        </div>

        {hoveredWeekData && !inspectorOpen ? (
          <WeeklyHoverTooltip week={hoveredWeekData} />
        ) : null}
      </div>

      <WeeklyAnalysisPanel
        open={weeklyAnalysisOpen}
        onClose={() => setWeeklyAnalysisOpen(false)}
        intel={weeklyAnalysisBlock}
      />
    </div>
  )
}
