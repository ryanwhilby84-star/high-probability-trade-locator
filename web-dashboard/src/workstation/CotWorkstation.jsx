import React from 'react'

import { CHART_WS, PANEL_IDS } from '../charts/chartTheme.js'
import { buildCotWorkstation } from '../cot/buildCotWorkstation.js'
import {
  POSITIONING_DEFAULT_RANGE_ID,
  POSITIONING_RANGE_PRESETS,
  rangePresetById,
} from '../cot/positioningChartMetrics.js'
import { useCot3ySeries, resolveCot3yBlock } from '../hooks/useCot3ySeries.js'
import { COT_3Y_PATH } from '../data/cot3ySeriesStore.js'
import { useWorkstationOhlc } from './hooks/useWorkstationOhlc.js'
import {
  buildPositioningWorkstationSeries,
} from './data/buildPositioningWorkstationSeries.js'
import { labelFromTimelineTime, rowsToLinePoints } from './charts/buildWorkstationTimelineData.js'
import { useMasterCamera } from './charts/useMasterCamera.js'
import { useMasterCameraGestures } from './charts/useMasterCameraGestures.js'
import { useGlobalVerticalMagnification } from './charts/useGlobalVerticalMagnification.js'
import { SimpleChartPane } from './charts/SimpleChartPane.jsx'
import {
  WS_COT_PLOT_HEIGHT,
  WS_PRICE_PLOT_HEIGHT,
} from './charts/workstationPanelSizing.js'
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

function PanelBlock({
  panelId,
  title,
  subtitle = null,
  panelClass = '',
  bodyHeight,
  children,
}) {
  return (
    <div className={`cot-ws-panel ${panelClass}`.trim()} data-panel={panelId}>
      <div className="cot-ws-panel-head">
        <span className="cot-ws-panel-title">{title}</span>
        {subtitle ? <span className="cot-ws-panel-subtitle">{subtitle}</span> : null}
      </div>
      <div className="cot-ws-panel-body" style={{ height: bodyHeight }}>
        {children}
      </div>
    </div>
  )
}

function formatYearsWeeks(weeks) {
  if (!weeks || weeks <= 0) return '0w'
  if (weeks >= 52) return `${(weeks / 52).toFixed(1)}Y`
  return `${weeks}w`
}

function buildVisibleSummary({ preset, rangeId, visibleWeeks, totalCotWeeks }) {
  const span = formatYearsWeeks(visibleWeeks)
  if (rangeId === 'all') {
    if (visibleWeeks >= totalCotWeeks) return `${visibleWeeks} weeks · All available (${span})`
    return `${visibleWeeks} weeks · All (${span} of ${totalCotWeeks})`
  }
  if ((rangeId === '10y' || rangeId === '5y') && visibleWeeks < (preset.weeks ?? visibleWeeks)) {
    return `${visibleWeeks} weeks · ${preset.label} (${span} available)`
  }
  return `${visibleWeeks} weeks · ${preset.label}`
}

function CotWorkstationSkeleton({ message = 'Loading COT series…' }) {
  return (
    <div className="cot-ws-chart-skeleton cot-ws-chart-skeleton--panel" aria-hidden="true">
      <span className="cot-ws-chart-skeleton-label">{message}</span>
    </div>
  )
}

export function CotWorkstation({ marketId, variant = 'default' }) {
  const { doc, loading, errored } = useCot3ySeries()
  const { exportBlock, exportLoaded } = useWorkstationOhlc(marketId)
  const [rangeId, setRangeId] = React.useState(POSITIONING_DEFAULT_RANGE_ID)
  const crosshairLabelRef = React.useRef(null)
  const panelsStackRef = React.useRef(null)
  const visibleRowsRef = React.useRef([])

  setDiagInstrument(marketId)
  bumpRender('CotWorkstation')

  const { block } = React.useMemo(() => resolveCot3yBlock(doc, marketId), [doc, marketId])

  const model = React.useMemo(() => {
    if (!block) return null
    try {
      return buildCotWorkstation(block)
    } catch (err) {
      console.error('[cot-workstation] buildCotWorkstation failed', marketId, err)
      return { available: false, error: String(err?.message || err) }
    }
  }, [block, marketId])

  const binding = React.useMemo(() => {
    if (!model?.available) return null
    return buildPositioningWorkstationSeries(model, null, exportBlock, {
      preserveFullCotHistory: true,
    })
  }, [model, exportBlock])

  const timelineRows = binding?.rows ?? []
  const totalCotWeeks = binding?.meta?.cotWeeks ?? timelineRows.length

  visibleRowsRef.current = timelineRows

  const visibleBars = binding?.weeklyBars ?? []

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

  const { chartsReady, cotSettled, ohlcSettled, cotDataReady } = useCotWorkstationReady({
    marketId,
    cotLoading: loading,
    cotDoc: doc,
    cotBlock: block,
    modelAvailable: Boolean(model?.available),
    visibleRowCount: timelineRows.length,
    ohlcExportLoaded: exportLoaded,
  })

  const hasAnyOhlc = ohlcSettled && (binding?.weeklyBars?.length ?? 0) > 0
  const hasVisibleOhlc = visibleBars.length > 0
  const ohlcPartial = Boolean(
    hasAnyOhlc &&
      (binding?.meta?.incompleteHistory ||
        (binding?.meta?.alignedOhlcWeeks ?? 0) < totalCotWeeks),
  )
  const isFullscreen = variant === 'fullscreen'

  const priceBodyHeight = hasAnyOhlc ? PRICE_BODY_HEIGHT : 72

  const preset = React.useMemo(() => rangePresetById(rangeId), [rangeId])

  const defaultWeekLabel =
    timelineRows[timelineRows.length - 1]?.date || timelineRows[timelineRows.length - 1]?.label || '—'

  const setCrosshairLabel = React.useCallback((text) => {
    if (crosshairLabelRef.current) crosshairLabelRef.current.textContent = text || '—'
  }, [])

  React.useEffect(() => {
    setCrosshairLabel(defaultWeekLabel)
  }, [defaultWeekLabel, marketId, setCrosshairLabel])

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

  const { registerPane, goHome, goAll, goPreset, resetCamera, panByPixels, zoomAtClientX, adjustVerticalMagnification, onDragStart, onDragEnd, subscribeGeometry, getViewportState } =
    useMasterCamera({
      timelineRowsRef: visibleRowsRef,
      onCrosshairTime,
      onCrosshairClear,
      homeWeeks: rangePresetById(POSITIONING_DEFAULT_RANGE_ID).weeks,
    })

  useMasterCameraGestures({
    containerRef: panelsStackRef,
    enabled: chartsReady,
    onPanDelta: panByPixels,
    onZoomAt: zoomAtClientX,
    onDragStart,
    onDragEnd,
  })

  useGlobalVerticalMagnification({
    containerRef: panelsStackRef,
    enabled: chartsReady,
    onMagnifyDelta: adjustVerticalMagnification,
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
  }, [chartsReady, timelineRows.length, preset.weeks, rangeId, goPreset])

  const handleAll = React.useCallback(() => {
    goAll()
    setRangeId('all')
  }, [goAll])

  const handleHome = React.useCallback(() => {
    setRangeId(POSITIONING_DEFAULT_RANGE_ID)
    goHome(rangePresetById(POSITIONING_DEFAULT_RANGE_ID).weeks)
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
        <p className="cot-ws-status" role="status">
          Loading COT series from <code>{COT_3Y_PATH}</code>…
        </p>
      </div>
    )
  }

  if (cotSettled && errored && !doc) {
    return (
      <div className="cot-ws-status cot-ws-status--error">
        <p>
          COT fetch failed for <strong>{marketId}</strong>. Check network tab for{' '}
          <code>{COT_3Y_PATH}</code>.
        </p>
      </div>
    )
  }

  if (cotSettled && !cotDataReady) {
    return (
      <div className="cot-ws-status cot-ws-status--error">
        <p>
          No COT workstation data for <strong>{marketId}</strong>.
        </p>
        {model?.error ? <p className="cot-ws-status-detail">{model.error}</p> : null}
        {!block && doc ? (
          <p className="cot-ws-status-detail">Market not found in {COT_3Y_PATH}.</p>
        ) : null}
      </div>
    )
  }

  const instTitle = model.institutionalGroup || 'Non-Commercial'
  const retailTitle = model.retailGroup || 'Non-Reportable'

  const priceSubtitle = ohlcPartial
    ? `OHLC ${binding?.meta?.range?.ohlcFirst ?? '—'} → ${binding?.meta?.range?.ohlcLast ?? '—'} · COT full history`
    : null

  const windowWeeks =
    preset.weeks == null ? totalCotWeeks : Math.min(preset.weeks, totalCotWeeks)

  const visibleSummary = buildVisibleSummary({
    preset,
    rangeId,
    visibleWeeks: windowWeeks,
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
    >
      <header className="cot-ws-toolbar">
        <div className="cot-ws-toolbar-left">
          <span className="cot-ws-build-badge" aria-label="Build marker">
            MASTER CAMERA BUILD 12
          </span>
          <span className="cot-ws-history">{model.historyLabel}</span>
          <span className="cot-ws-weeks">
            {shellLoading ? 'Preparing timeline…' : visibleSummary}
          </span>
          {binding?.meta?.rangeNote ? (
            <span className="cot-ws-range-note">{binding.meta.rangeNote}</span>
          ) : null}
        </div>
        <div className="cot-ws-toolbar-center">
          <span className="cot-ws-crosshair-label" ref={crosshairLabelRef} />
        </div>
        <div className="cot-ws-range-toggles" role="group" aria-label="Chart range">
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
          {POSITIONING_RANGE_PRESETS.map((p) => (
            <button
              key={p.id}
              type="button"
              className={`cot-ws-range-btn${p.id === rangeId ? ' active' : ''}`}
              disabled={shellLoading}
              onClick={() => setRangeId(p.id)}
            >
              {p.label}
            </button>
          ))}
        </div>
      </header>

      {import.meta.env?.DEV ? (
        <div className="cot-ws-diag" aria-hidden="true">
          {marketId} · ready={chartsReady ? 'y' : 'n'} · cot={cotSettled ? 'y' : 'n'} · ohlc=
          {ohlcSettled ? 'y' : 'n'} · rows={timelineRows.length}
        </div>
      ) : null}

      <div className="cot-ws-canvas-scroll">
        <div className="cot-ws-panels cot-ws-panels--camera" ref={panelsStackRef}>
          <CotDrawingCoordinator
            subscribeGeometry={subscribeGeometry}
            panelsRef={panelsStackRef}
            getViewportState={getViewportState}
          />
          <PanelBlock
            panelId={PANEL_IDS.price}
            title="Weekly OHLC"
            subtitle={priceSubtitle}
            panelClass={`cot-ws-panel--price${hasAnyOhlc ? '' : ' cot-ws-panel--price-empty'}`}
            bodyHeight={priceBodyHeight}
          >
            {chartsReady ? (
              <SimpleChartPane
                {...paneProps}
                panelId={PANEL_IDS.price}
                mode="candle"
                candleBars={visibleBars}
                syncOnly={!hasVisibleOhlc}
                emptyMessage={
                  hasAnyOhlc && !hasVisibleOhlc
                    ? 'No weekly OHLC in this range.'
                    : ohlcSettled
                      ? 'Weekly OHLC unavailable for this market.'
                      : 'Loading weekly OHLC…'
                }
              />
            ) : (
              <CotWorkstationSkeleton />
            )}
          </PanelBlock>

          <div className="cot-ws-cot-group">
            <div className="cot-ws-cot-group-head">COT positioning</div>
            <PanelBlock
              panelId={PANEL_IDS.commercial}
              title="Commercial net"
              panelClass="cot-ws-panel--cot"
              bodyHeight={COT_BODY_HEIGHT}
            >
              {chartsReady ? (
                <SimpleChartPane
                  {...paneProps}
                  panelId={PANEL_IDS.commercial}
                  mode="line"
                  lineColor={CHART_WS.commercial}
                  linePoints={commercialLinePoints}
                  zeroLine
                />
              ) : (
                <CotWorkstationSkeleton />
              )}
            </PanelBlock>

            <PanelBlock
              panelId={PANEL_IDS.institutional}
              title={`${instTitle} net`}
              panelClass="cot-ws-panel--cot"
              bodyHeight={COT_BODY_HEIGHT}
            >
              {chartsReady ? (
                <SimpleChartPane
                  {...paneProps}
                  panelId={PANEL_IDS.institutional}
                  mode="line"
                  lineColor={CHART_WS.institutional}
                  linePoints={institutionalLinePoints}
                  zeroLine
                />
              ) : (
                <CotWorkstationSkeleton />
              )}
            </PanelBlock>

            <PanelBlock
              panelId={PANEL_IDS.retail}
              title={`${retailTitle} net`}
              panelClass="cot-ws-panel--cot cot-ws-panel--retail"
              bodyHeight={COT_BODY_HEIGHT}
            >
              {chartsReady ? (
                <SimpleChartPane
                  {...paneProps}
                  panelId={PANEL_IDS.retail}
                  mode="line"
                  showTimeAxis
                  lineColor={CHART_WS.retail}
                  linePoints={retailLinePoints}
                  zeroLine
                />
              ) : (
                <CotWorkstationSkeleton />
              )}
            </PanelBlock>
          </div>
        </div>
      </div>
    </div>
  )
}
