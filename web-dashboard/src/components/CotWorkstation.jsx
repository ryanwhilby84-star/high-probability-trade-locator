import React from 'react'
import { Brush, Line, LineChart, ResponsiveContainer, XAxis, YAxis } from 'recharts'

import { useCurrencyFuturesIVE } from '../hooks/useCurrencyFuturesIVE.js'
import {
  buildCotWorkstation,
  COT_WS_DEFAULT_WEEKS,
  COT_WS_RANGE_PRESETS,
  presetRange,
  sliceCotWorkstationRange,
} from '../cot/buildCotWorkstation.js'
import { enrichChartWorkstationSeries, chartSupplementMeta } from '../charts/buildChartWorkstation.js'
import {
  enrichChartAnalytics,
  percentileExtremeThresholds,
  pointExplainSnapshot,
  sliceSeriesForReplay,
} from '../charts/chartAnalytics.js'
import { ChartDrawingToolbar } from '../charts/ChartDrawingToolbar.jsx'
import { ChartPanel } from '../charts/ChartPanel.jsx'
import { ChartCrosshairHeader, ChartPointSidePanel } from '../charts/ChartPointSidePanel.jsx'
import { SeasonalityDecisionPanel } from './SeasonalityDecisionPanel.jsx'
import { CHART_SYNC_ID, CHART_WS, PANEL_IDS } from '../charts/chartTheme.js'
import { useChartDrawings } from '../charts/useChartDrawings.js'
import { DRAWING_TOOLS } from '../charts/chartDrawings.js'
import { HPTL_LINE_TYPE } from '../charts/hptlLine.js'
import '../charts/chartWorkstation.css'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function PriceCoverageBanner({ coverage }) {
  if (!coverage || coverage.status === 'OK') return null
  const cls =
    coverage.status === 'PARTIAL'
      ? 'cot-ws-warn cot-ws-warn-price'
      : 'cot-ws-warn cot-ws-warn-history'
  return (
    <p className={cls} role="alert">
      Price coverage {coverage.status}: {coverage.reason}
      {coverage.priceSource ? ` Source: ${coverage.priceSource}.` : ''}
    </p>
  )
}

function CoverageStats({ coverage }) {
  if (!coverage) return null
  const statusClass = `cot-ws-cov-status cot-ws-cov-status--${String(coverage.status || 'unknown').toLowerCase()}`
  return (
    <div className="cot-ws-coverage">
      <span className="cot-ws-history-item">
        <span className="cot-ws-history-k">COT weeks</span>
        <span className="cot-ws-history-v">{coverage.cotWeeks ?? '—'}</span>
      </span>
      <span className="cot-ws-history-item">
        <span className="cot-ws-history-k">Price matched</span>
        <span className="cot-ws-history-v">{coverage.priceMatched ?? '—'}</span>
      </span>
      <span className="cot-ws-history-item">
        <span className="cot-ws-history-k">Match %</span>
        <span className="cot-ws-history-v">
          {coverage.matchPct != null ? `${coverage.matchPct}%` : '—'}
        </span>
      </span>
      <span className="cot-ws-history-item">
        <span className="cot-ws-history-k">Price source</span>
        <span className="cot-ws-history-v cot-ws-history-source">{coverage.priceSource || '—'}</span>
      </span>
      <span className="cot-ws-history-item">
        <span className="cot-ws-history-k">Status</span>
        <span className={statusClass}>{coverage.status || '—'}</span>
      </span>
    </div>
  )
}

function priceCoverageWarning(coverage, priceAudit) {
  if (coverage?.reason && coverage.status !== 'OK') return coverage.reason
  if (!priceAudit) return null
  const missingBefore = priceAudit?.missing_before
  const missingWeeks = priceAudit?.missing_price_weeks ?? 0
  if (missingBefore && missingWeeks > 0) {
    return `Price history missing before ${missingBefore} (${missingWeeks} COT weeks without price).`
  }
  return null
}

/** Chart Workstation V2 — synced crosshair, extremes, click-to-explain, replay, drawings. */
export function CotWorkstation({
  block,
  marketId = null,
  seasonalityDoc = null,
  valHistDoc = null,
  valuationDoc = null,
  locationDoc = null,
  v3Doc = null,
  confluenceRow = null,
  confluenceHistory = null,
}) {
  const futuresIveDoc = useCurrencyFuturesIVE()
  const baseModel = React.useMemo(() => (block ? buildCotWorkstation(block) : null), [block])

  const enrichOpts = React.useMemo(
    () => ({
      marketId,
      seasonalityDoc,
      valHistDoc,
      valuationDoc,
      locationDoc,
      v3Doc,
      futuresIveDoc,
      confluenceRow,
      confluenceHistory,
    }),
    [marketId, seasonalityDoc, valHistDoc, valuationDoc, locationDoc, v3Doc, futuresIveDoc, confluenceRow, confluenceHistory],
  )

  const enrichedSeries = React.useMemo(() => {
    if (!baseModel?.series) return []
    return enrichChartWorkstationSeries(baseModel.series, enrichOpts)
  }, [baseModel?.series, enrichOpts])

  const fullAnalytics = React.useMemo(
    () => enrichChartAnalytics(enrichedSeries),
    [enrichedSeries],
  )

  const model = React.useMemo(() => {
    if (!baseModel?.available) return baseModel
    return { ...baseModel, series: fullAnalytics }
  }, [baseModel, fullAnalytics])

  const supplement = React.useMemo(
    () => chartSupplementMeta(marketId, { ...enrichOpts, priceSeries: baseModel?.series }),
    [marketId, enrichOpts, baseModel?.series],
  )

  const [replayCutoffIndex, setReplayCutoffIndex] = React.useState(null)
  const [sidePoint, setSidePoint] = React.useState(null)
  const [brushRange, setBrushRange] = React.useState({ startIndex: 0, endIndex: 0 })
  const [activePreset, setActivePreset] = React.useState('260')
  const [showExtremes, setShowExtremes] = React.useState(true)
  const [hoverPoint, setHoverPoint] = React.useState(null)

  const workingSeries = React.useMemo(() => {
    if (replayCutoffIndex == null) return fullAnalytics
    const replayHist = enrichChartAnalytics(enrichedSeries, replayCutoffIndex)
    return sliceSeriesForReplay(replayHist, replayCutoffIndex)
  }, [fullAnalytics, enrichedSeries, replayCutoffIndex])

  const seriesLen = workingSeries.length
  const rowCount = block?.series?.length ?? fullAnalytics.length
  const replayActive = replayCutoffIndex != null

  const extremeThresholds = React.useMemo(
    () => ({
      institutional: percentileExtremeThresholds(workingSeries, 'institutional_net'),
      retail: percentileExtremeThresholds(workingSeries, 'retail_net'),
      commercial: percentileExtremeThresholds(workingSeries, 'commercial_net'),
    }),
    [workingSeries],
  )

  const instrumentId = marketId || model?.market || 'unknown'
  const {
    tool,
    setTool,
    drawings,
    selectedId,
    draft,
    getRef,
    onDrawPointerDown,
    onDrawPointerMove,
    onDrawPointerUp,
    deleteSelected,
    clearAll,
    overlayActive,
    selectDrawing,
  } = useChartDrawings(instrumentId)

  const selectMode = tool === DRAWING_TOOLS.SELECT
  const drawCursor = selectMode ? 'default' : 'crosshair'
  const clickToExplain = selectMode && !overlayActive

  const onPoint = React.useCallback((p) => setHoverPoint(p), [])
  const onClear = React.useCallback(() => setHoverPoint(null), [])

  React.useEffect(() => {
    if (!seriesLen) return
    setBrushRange(presetRange(seriesLen, COT_WS_DEFAULT_WEEKS))
    setActivePreset('260')
    setHoverPoint(null)
    setSidePoint(null)
    setReplayCutoffIndex(null)
  }, [model?.market, fullAnalytics.length])

  React.useEffect(() => {
    if (replayCutoffIndex == null || !seriesLen) return
    setBrushRange(presetRange(seriesLen, COT_WS_DEFAULT_WEEKS))
    setActivePreset('custom')
  }, [replayCutoffIndex, seriesLen])

  const visibleData = React.useMemo(
    () => sliceCotWorkstationRange(workingSeries, brushRange.startIndex, brushRange.endIndex),
    [workingSeries, brushRange],
  )

  const latest = visibleData[visibleData.length - 1] || workingSeries[workingSeries.length - 1]
  const readoutPoint = hoverPoint || latest
  const activeLabel = hoverPoint?.label || hoverPoint?.date || null

  const applyPreset = (preset) => {
    setActivePreset(preset.id)
    setBrushRange(presetRange(seriesLen, preset.weeks))
  }

  const onBrushChange = (range) => {
    if (!range || range.startIndex == null || range.endIndex == null) return
    setBrushRange({ startIndex: range.startIndex, endIndex: range.endIndex })
    setActivePreset('custom')
  }

  const onDateClick = React.useCallback(
    (p) => {
      if (!p) return
      const full = fullAnalytics.find((r) => r.date === p.date || r.label === p.label) || p
      setSidePoint(pointExplainSnapshot(full))
    },
    [fullAnalytics],
  )

  const exitReplay = React.useCallback(() => {
    setReplayCutoffIndex(null)
    setSidePoint(null)
  }, [])

  const startReplayFromSide = React.useCallback(() => {
    if (sidePoint?.index == null) return
    setReplayCutoffIndex(sidePoint.index)
  }, [sidePoint])

  if (!model?.available) {
    return (
      <section className="chart-ws cot-ws-tv" data-component="ChartWorkstation">
        <header className="chart-ws-toolbar">
          <h1 className="chart-ws-title">Chart Workstation</h1>
        </header>
        <p className="chart-ws-empty">{model?.reason || 'Chart workstation unavailable for this market.'}</p>
        <p className="chart-ws-debug">
          rows: {rowCount} · source: cot_3y_series_latest.json
          {marketId ? ` · ${marketId}` : ''}
        </p>
      </section>
    )
  }

  const rangeLabel =
    visibleData.length && visibleData[0]?.label && latest?.label
      ? `${visibleData[0].label.slice(0, 7)} → ${latest.label.slice(0, 7)} · ${visibleData.length} visible`
      : ''

  const coverage = model.priceCoverage
  const priceWarn = priceCoverageWarning(coverage, model.priceAudit)
  const priceIncomplete = coverage?.priceIncomplete === true
  const pricePointsInView = visibleData.filter((d) => isNum(d.price)).length
  const priceGapInView = visibleData.length > 0 && pricePointsInView < visibleData.length
  const priceConnectNulls = coverage?.status === 'OK' && !priceGapInView

  const instTitle = model.institutionalGroup || 'Institutional positioning'
  const retailTitle = model.retailGroup || 'Retail positioning'

  const panelProps = {
    data: visibleData,
    syncId: CHART_SYNC_ID,
    drawings,
    draft,
    selectedId,
    overlayActive,
    selectMode,
    onSelectDrawing: selectDrawing,
    drawCursor,
    onDrawPointerDown,
    onDrawPointerMove,
    onDrawPointerUp,
    onPoint,
    onClear,
    activeLabel,
    onDateClick,
    clickToExplain,
  }

  return (
    <section
      className="chart-ws cot-ws-tv chart-ws-shell"
      aria-label="Chart Workstation"
      data-component="ChartWorkstation"
    >
      <header className="chart-ws-toolbar">
        <div className="chart-ws-toolbar-left">
          <h1 className="chart-ws-title">Chart Workstation</h1>
          <p className="chart-ws-meta">{model.market}</p>
          <div className="cot-ws-history">
            <span className="cot-ws-history-item">
              <span className="cot-ws-history-k">History</span>
              <span className="cot-ws-history-v">{model.historyLabel}</span>
            </span>
            <span className="cot-ws-history-item">
              <span className="cot-ws-history-k">Weeks</span>
              <span className="cot-ws-history-v">{replayActive ? `${seriesLen} (replay)` : rowCount}</span>
            </span>
            {rangeLabel ? (
              <span className="cot-ws-history-item cot-ws-history-view">
                <span className="cot-ws-history-k">View</span>
                <span className="cot-ws-history-v">{rangeLabel}</span>
              </span>
            ) : null}
          </div>
          <CoverageStats coverage={coverage} />
        </div>
        <div className="chart-ws-toolbar-right">
          <ChartDrawingToolbar
            tool={tool}
            onToolChange={setTool}
            onDeleteSelected={deleteSelected}
            onClearAll={clearAll}
            selectedId={selectedId}
            drawingCount={drawings.length}
          />
          <label className="cot-ws-toggle">
            <input
              type="checkbox"
              checked={showExtremes}
              onChange={(e) => setShowExtremes(e.target.checked)}
            />
            Show extremes
          </label>
          <div className="cot-ws-controls" role="group" aria-label="Date range">
            {COT_WS_RANGE_PRESETS.map((preset) => (
              <button
                key={preset.id}
                type="button"
                className={`cot-ws-preset${activePreset === preset.id ? ' active' : ''}`}
                onClick={() => applyPreset(preset)}
              >
                {preset.label}
              </button>
            ))}
          </div>
        </div>
      </header>

      {replayActive ? (
        <div className="chart-ws-replay-banner" role="status">
          <span>
            Replay mode — chart frozen at{' '}
            <strong>{workingSeries[replayCutoffIndex]?.date || '—'}</strong>. Future data hidden.
          </span>
          <button type="button" className="chart-ws-replay-exit" onClick={exitReplay}>
            Exit replay
          </button>
        </div>
      ) : null}

      {model.historyWarning ? (
        <p className="cot-ws-warn cot-ws-warn-history" role="alert">
          {model.historyWarning}
        </p>
      ) : null}

      <PriceCoverageBanner coverage={coverage} />

      {priceWarn && coverage?.status === 'OK' ? (
        <p className="cot-ws-warn cot-ws-warn-price" role="alert">
          {priceWarn}
        </p>
      ) : null}

      <div className={`chart-ws-body${sidePoint ? ' chart-ws-body--side-open' : ''}`}>
        <div className="chart-ws-main">
          <ChartCrosshairHeader point={readoutPoint} supplement={supplement} />

          <div className="chart-ws-stack">
            <ChartPanel
              panelId={PANEL_IDS.price}
              title="Price"
              dataKey="price"
              color={CHART_WS.price}
              yFormatter={(v) => (isNum(v) ? v.toFixed(4) : '—')}
              height={260}
              showXAxis={false}
              connectNulls={priceConnectNulls}
              interactionRef={getRef(PANEL_IDS.price)}
              panelWarning={
                priceIncomplete
                  ? 'Price line may show gaps — incomplete COT alignment.'
                  : priceGapInView && visibleData[0] && !isNum(visibleData[0].price)
                    ? `No price in view before ${visibleData.find((d) => isNum(d.price))?.label || '—'}.`
                    : null
              }
              {...panelProps}
            />

            <ChartPanel
              panelId={PANEL_IDS.institutional}
              title={instTitle}
              subtitle="Non-commercial net · top/bottom 10% shaded"
              dataKey="institutional_net"
              color={CHART_WS.institutional}
              yFormatter={(v) => (isNum(v) ? Math.round(v).toLocaleString() : '—')}
              height={240}
              showXAxis={false}
              extremes={{ high: extremeThresholds.institutional.high, low: extremeThresholds.institutional.low }}
              showExtremes={showExtremes}
              showZeroLine
              interactionRef={getRef(PANEL_IDS.institutional)}
              {...panelProps}
            />

            <ChartPanel
              panelId={PANEL_IDS.retail}
              title={retailTitle}
              subtitle="Non-reportable net · top/bottom 10% shaded"
              dataKey="retail_net"
              color={CHART_WS.retail}
              yFormatter={(v) => (isNum(v) ? Math.round(v).toLocaleString() : '—')}
              height={240}
              showXAxis={false}
              extremes={{ high: extremeThresholds.retail.high, low: extremeThresholds.retail.low }}
              showExtremes={showExtremes}
              showZeroLine
              interactionRef={getRef(PANEL_IDS.retail)}
              {...panelProps}
            />

            <ChartPanel
              panelId={PANEL_IDS.commercial}
              title="Commercial"
              subtitle="Commercial net · top/bottom 10% shaded"
              dataKey="commercial_net"
              color={CHART_WS.commercial}
              yFormatter={(v) => (isNum(v) ? Math.round(v).toLocaleString() : '—')}
              height={240}
              showXAxis={false}
              extremes={{ high: extremeThresholds.commercial.high, low: extremeThresholds.commercial.low }}
              showExtremes={showExtremes}
              showZeroLine
              interactionRef={getRef(PANEL_IDS.commercial)}
              panelWarning={
                !model.hasCommercial ? 'Commercial net unavailable for this market.' : null
              }
              {...panelProps}
            />

            <ChartPanel
              panelId={PANEL_IDS.location}
              title="Location"
              subtitle={supplement.locationSubtitle || 'Where price sits in its 52-week range'}
              dataKey={supplement.locationLineKey || 'location_percentile_52w'}
              color={CHART_WS.location}
              yFormatter={(v) => (isNum(v) ? `${v.toFixed(0)}th pct` : '—')}
              yDomain={[0, 100]}
              height={220}
              showXAxis
              showZeroLine={false}
              connectNulls={false}
              interactionRef={getRef(PANEL_IDS.location)}
              panelWarning={
                !supplement.hasLocation
                  ? supplement.locationNote || 'Insufficient price history for 52-week location.'
                  : null
              }
              {...panelProps}
            />

            <ChartPanel
              panelId={PANEL_IDS.valuation}
              title="Valuation"
              subtitle={supplement.valuationSubtitle || 'UNAVAILABLE'}
              dataKey={supplement.valuationLineKey || 'valuation_fair'}
              color={CHART_WS.valuation}
              yFormatter={(v) => (isNum(v) ? `${v >= 0 ? '+' : ''}${v.toFixed(1)}%` : '—')}
              height={220}
              showXAxis
              showZeroLine={false}
              connectNulls={false}
              interactionRef={getRef(PANEL_IDS.valuation)}
        panelWarning={
          supplement.valuationMode !== 'wired' || !supplement.hasValuation
            ? supplement.valuationNote ||
              'No approved valuation model passed audit for this instrument.'
            : null
        }
              {...panelProps}
            />
          </div>

          <SeasonalityDecisionPanel marketId={marketId} seasonalityDoc={seasonalityDoc} cotBlock={block} />

          <div className="chart-ws-brush-wrap">
            <ResponsiveContainer width="100%" height={40}>
              <LineChart data={workingSeries} margin={{ top: 2, right: 14, left: 2, bottom: 0 }}>
                <XAxis
                  dataKey="label"
                  tick={{ fontSize: 10, fill: CHART_WS.axis, fontFamily: CHART_WS.fontFamily }}
                  interval="preserveStartEnd"
                  minTickGap={56}
                  axisLine={{ stroke: CHART_WS.border }}
                  tickLine={false}
                />
                <YAxis hide domain={['auto', 'auto']} />
                <Line
                  type={HPTL_LINE_TYPE}
                  dataKey="institutional_net"
                  stroke={CHART_WS.institutional}
                  dot={false}
                  strokeWidth={1}
                  strokeOpacity={0.35}
                  isAnimationActive={false}
                />
                <Brush
                  dataKey="label"
                  height={22}
                  stroke="#64748b"
                  fill="#141c2b"
                  travellerWidth={10}
                  startIndex={brushRange.startIndex}
                  endIndex={brushRange.endIndex}
                  onChange={onBrushChange}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>

          <p className="chart-ws-hint">
            Hover any panel for synced crosshair · Click a date (Select tool) for point analysis
          </p>

          <p className="chart-ws-debug">
            {coverage?.status ? `coverage: ${coverage.status}` : ''}
            {coverage?.matchPct != null ? ` · ${coverage.matchPct}% matched` : ''}
            {drawings.length ? ` · ${drawings.length} drawing(s)` : ''}
            {marketId ? ` · ${marketId}` : ''}
          </p>
        </div>

        {sidePoint ? (
          <ChartPointSidePanel
            point={sidePoint}
            onClose={() => setSidePoint(null)}
            onReplayFromHere={startReplayFromSide}
            replayActive={replayActive}
          />
        ) : null}
      </div>
    </section>
  )
}
