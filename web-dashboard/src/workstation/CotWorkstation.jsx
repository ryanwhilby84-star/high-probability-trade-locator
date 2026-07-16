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

  const onCrosshairTime = React.useCallback(
    (time) => {
      setCrosshairLabel(labelFromTimelineTime(visibleRowsRef.current, time))
    },
    [setCrosshairLabel],
  )

  const onCrosshairClear = React.useCallback(() => {
    const latest = visibleRowsRef.current[visibleRowsRef.current.length - 1]
    setCrosshairLabel(latest?.date || latest?.label || '—')
  }, [setCrosshairLabel])

  const {
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
      }`}
      data-market={marketId}
      data-charts-ready={chartsReady ? '1' : '0'}
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
    </div>
  )
}
