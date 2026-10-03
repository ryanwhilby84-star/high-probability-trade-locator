import React from 'react'

import { ChartCrosshairHeader } from '../../charts/ChartPointSidePanel.jsx'
import { findCotRowAsOf, cotDateToBarTime } from '../../charts/positioningTimelineAlign.js'
import { CHART_WS, PANEL_IDS } from '../../charts/chartTheme.js'
import { fmtDelta, fmtValue, seriesMetrics } from '../../cot/positioningChartMetrics.js'
import { fmtPrice } from '../../priceData.js'
import { useWeeklyTimelineOptional } from '../context/WeeklyTimelineContext.jsx'
import { WORKSTATION_DRAWING_TOOLS, DEFAULT_VLINE_STYLE } from '../canvas/workstationDrawingTypes.js'
import { WorkstationDataErrorPanel } from '../components/WorkstationDataErrorPanel.jsx'
import { PositioningChartChrome } from '../../components/PositioningChartChrome.jsx'
import { labelFromTimelineTime, rowsToLinePoints } from './buildWorkstationTimelineData.js'
import { useLinkedChartTimeline } from './useLinkedChartTimeline.js'
import { WorkstationChartPane } from './WorkstationChartPane.jsx'
import { WorkstationDrawingToolbar } from './WorkstationDrawingToolbar.jsx'
import { GlobalTimelineVlineOverlay } from './GlobalTimelineVlineOverlay.jsx'
import { LiveQuoteRefreshBar } from '../../components/LiveQuoteRefreshBar.jsx'
import { GoldPriceTruthPanel } from '../components/GoldPriceTruthPanel.jsx'
import { logPriceEngineReport } from '../../prices/priceEngineReport.js'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

import { ResizablePlotShell } from './ResizablePlotShell.jsx'
import { useCotPanelHeights } from './useCotPanelHeights.js'
import { WS_PRICE_PLOT_HEIGHT } from './workstationPanelSizing.js'

function SeriesBadge({ shortLabel, metrics, color }) {
  const deltaClass = (v) => {
    if (!Number.isFinite(v)) return ''
    if (v > 0) return ' pos-delta--up'
    if (v < 0) return ' pos-delta--down'
    return ''
  }
  return (
    <aside className="pos-series-badge" style={{ '--series-color': color }}>
      <div className="pos-series-badge-label">{shortLabel}</div>
      <div className="pos-series-badge-value">{fmtValue(metrics.value)}</div>
      <div className="pos-series-badge-deltas">
        <span className={deltaClass(metrics.wow)}>WoW {fmtDelta(metrics.wow)}</span>
        <span className={deltaClass(metrics.w4)}>4W {fmtDelta(metrics.w4)}</span>
        <span className={deltaClass(metrics.w13)}>13W {fmtDelta(metrics.w13)}</span>
      </div>
    </aside>
  )
}

function CandleBadge({ candle, weekLabel, isWeeklyDefault = false }) {
  if (!candle) {
    return (
      <aside className="pos-series-badge pos-series-badge--candle">
        <div className="pos-series-badge-label">Weekly OHLC</div>
        <div className="pos-series-badge-value">—</div>
      </aside>
    )
  }
  const chg = isNum(candle.open) && isNum(candle.close) ? candle.close - candle.open : null
  const deltaClass = chg > 0 ? ' pos-delta--up' : chg < 0 ? ' pos-delta--down' : ''
  return (
    <aside className="pos-series-badge pos-series-badge--candle">
      {weekLabel ? <div className="pos-series-badge-week">{weekLabel}</div> : null}
      {!weekLabel && isWeeklyDefault ? (
        <div className="pos-series-badge-week">Completed weekly candle</div>
      ) : null}
      <div className="pos-series-badge-label">Weekly close</div>
      <div className="pos-series-badge-value">{fmtPrice(candle.close, 2)}</div>
      <div className="pos-series-badge-deltas">
        <span>O {fmtPrice(candle.open, 2)}</span>
        <span>H {fmtPrice(candle.high, 2)}</span>
        <span>L {fmtPrice(candle.low, 2)}</span>
        <span className={deltaClass}>Δ {fmtDelta(chg)}</span>
      </div>
    </aside>
  )
}

function CotPanelShell({
  title,
  shortLabel,
  panelId,
  color,
  dataKey,
  wowKey,
  fullSeries,
  plotHeight,
  onPlotResize,
  children,
  panelWarning,
}) {
  const metrics = React.useMemo(
    () => seriesMetrics(fullSeries, dataKey, wowKey),
    [fullSeries, dataKey, wowKey],
  )
  return (
    <div className="pos-chart-panel pos-chart-panel--canvas" data-panel={panelId}>
      <div className="pos-chart-panel-head">
        <span className="pos-chart-panel-title">{title}</span>
      </div>
      {panelWarning ? <p className="pos-chart-panel-warn">{panelWarning}</p> : null}
      <div className="pos-chart-panel-body pos-chart-panel-body--canvas">
        <ResizablePlotShell panelId={panelId} height={plotHeight} onResize={onPlotResize}>
          {children}
        </ResizablePlotShell>
        <SeriesBadge shortLabel={shortLabel} metrics={metrics} color={color} />
      </div>
    </div>
  )
}

export function SynchronizedWorkstationPanels({
  marketId,
  model,
  visibleData,
  visibleBars,
  fullSeries,
  rangeId,
  onRangeChange,
  instTitle,
  retailTitle,
  priceWarning,
  dataWarnings = [],
  drawingsApi,
  ohlcSourceLabel = null,
  priceContext = null,
  liveQuote = null,
  liveQuoteDoc = null,
  exportBlock = null,
  exportGeneratedAt = null,
  valuationBlock = null,
  liveQuoteFreshness = null,
  liveQuotesFetchUrl = null,
  liveQuotesFetchedAtMs = null,
  liveQuotesRefreshing = false,
  liveQuotesRefreshError = null,
  refreshLiveQuotes = null,
}) {
  const timeline = useWeeklyTimelineOptional()
  const linked = useLinkedChartTimeline()
  const chartStackRef = React.useRef(null)
  const { heightForPanel, setPanelHeight } = useCotPanelHeights(marketId)
  const [hoverRow, setHoverRow] = React.useState(null)
  const [hoverCandle, setHoverCandle] = React.useState(null)

  const timelineRows = visibleData
  const fitKey = `${marketId}:${rangeId}:${timelineRows.length}:${visibleBars.length}`

  React.useEffect(() => {
    linked.fitAll(fitKey)
  }, [fitKey, linked])

  React.useEffect(() => {
    linked.setExternalCrosshair(timeline?.crosshairTime ?? null)
  }, [timeline?.crosshairTime, linked])

  const drawingTool = drawingsApi?.activeTool ?? WORKSTATION_DRAWING_TOOLS.SELECT
  const toolBlocksPan = drawingTool !== WORKSTATION_DRAWING_TOOLS.SELECT

  React.useEffect(() => {
    linked.setInteractionEnabled(!toolBlocksPan)
  }, [toolBlocksPan, linked])

  React.useEffect(() => {
    const onKey = (e) => {
      if (e.key !== 'Delete' && e.key !== 'Backspace') return
      const tag = String(e.target?.tagName || '').toLowerCase()
      if (tag === 'input' || tag === 'textarea' || e.target?.isContentEditable) return
      if (drawingsApi?.selectedId) {
        e.preventDefault()
        drawingsApi.deleteSelected?.()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [drawingsApi])

  const onCrosshairMove = React.useCallback(
    (payload) => {
      if (!payload?.time) return
      const label = labelFromTimelineTime(timelineRows, payload.time)
      const row = findCotRowAsOf(fullSeries, label)
      if (row) setHoverRow(row)
      if (payload.candle) setHoverCandle(payload.candle)
      timeline?.setCrosshair?.(payload.time, label)
    },
    [timelineRows, fullSeries, timeline],
  )

  const onCrosshairClear = React.useCallback(() => {
    setHoverRow(null)
    setHoverCandle(null)
    timeline?.clearCrosshair?.()
  }, [timeline])

  const readoutPoint = hoverRow || timelineRows[timelineRows.length - 1]
  const latestBar = visibleBars[visibleBars.length - 1]
  const displayCandle = React.useMemo(
    () =>
      hoverCandle ||
      (latestBar
        ? { open: latestBar.open, high: latestBar.high, low: latestBar.low, close: latestBar.close }
        : null),
    [hoverCandle, latestBar],
  )

  const goldDisplaySnapshot = React.useMemo(
    () => ({
      crosshairHeaderPrice: readoutPoint?.price ?? null,
      crosshairHeaderDate: readoutPoint?.label || readoutPoint?.date || null,
      crosshairHeaderSource: 'HistoricalCOTStore + OHLC binding',
      liveMarkerPrice: priceContext?.liveStatus === 'LIVE' ? priceContext?.liveMid : null,
      latestVisibleBarClose: latestBar?.close ?? null,
      latestVisibleBarDate: latestBar?.date ?? null,
      candleTooltipClose: displayCandle?.close ?? null,
      candleTooltipDate: hoverCandle
        ? timeline?.crosshairLabel
        : latestBar?.date ?? null,
      candleTooltipSource: hoverCandle ? 'Hovered weekly OHLC candle' : 'Latest visible weekly OHLC candle',
      candleTooltipComponent: hoverCandle ? 'CandleBadge (crosshair hover)' : 'CandleBadge (latest bar default)',
      cotRowPrice: readoutPoint?.price ?? null,
    }),
    [
      readoutPoint,
      priceContext?.liveMid,
      priceContext?.liveStatus,
      latestBar,
      displayCandle,
      hoverCandle,
      timeline?.crosshairLabel,
    ],
  )

  React.useEffect(() => {
    if (!marketId || !priceContext) return
    logPriceEngineReport(marketId, {
      liveMarkerPrice: goldDisplaySnapshot.liveMarkerPrice,
      valuationLivePrice: priceContext.valuationLiveMid,
      truthTableLivePrice: priceContext.liveMid,
      headerLivePrice: priceContext.liveMid,
      weeklyChartClose: latestBar?.close ?? priceContext.weeklyClose,
      candleBadgeClose: displayCandle?.close ?? priceContext.weeklyClose,
      historicalHeaderPrice: goldDisplaySnapshot.crosshairHeaderPrice,
      historicalHeaderDate: goldDisplaySnapshot.crosshairHeaderDate,
      crosshairDate: goldDisplaySnapshot.crosshairHeaderDate,
    })
  }, [marketId, priceContext, goldDisplaySnapshot, latestBar, displayCandle])

  const commercialPoints = React.useMemo(
    () => rowsToLinePoints(timelineRows, 'commercial_net'),
    [timelineRows],
  )
  const institutionalPoints = React.useMemo(
    () => rowsToLinePoints(timelineRows, 'institutional_net'),
    [timelineRows],
  )
  const retailPoints = React.useMemo(() => rowsToLinePoints(timelineRows, 'retail_net'), [timelineRows])

  const dateToTime = React.useCallback(
    (date) => cotDateToBarTime(timelineRows, date) ?? cotDateToBarTime(visibleBars, date),
    [timelineRows, visibleBars],
  )

  const handleDrawingCommit = React.useCallback(
    (drag) => {
      if (!drag || !drawingsApi?.addDrawing) return
      if (drag.type === 'vline') {
        if (!drag.date) return
        drawingsApi.addDrawing({
          type: 'vline',
          date: drag.date,
          color: DEFAULT_VLINE_STYLE.color,
          width: DEFAULT_VLINE_STYLE.width,
        })
        return
      }
      if (drag.type === 'hline') {
        drawingsApi.addDrawing({
          type: 'hline',
          panelId: drag.panelId,
          value: drag.value,
        })
        return
      }
      if (drag.type === 'rect') {
        if (
          drag.timeStart === drag.timeEnd &&
          Math.abs(Number(drag.valueTop) - Number(drag.valueBottom)) < 1e-9
        ) {
          return
        }
        drawingsApi.addDrawing({
          type: 'rect',
          panelId: drag.panelId,
          dateStart: labelFromTimelineTime(timelineRows, drag.timeStart),
          dateEnd: labelFromTimelineTime(timelineRows, drag.timeEnd),
          valueTop: drag.valueTop,
          valueBottom: drag.valueBottom,
        })
      }
    },
    [drawingsApi, timelineRows],
  )

  const handleVlineUpdate = React.useCallback(
    (id, { date }) => {
      if (!id || !date || !drawingsApi?.updateDrawing) return
      drawingsApi.updateDrawing(id, { date })
    },
    [drawingsApi],
  )

  const sharedPaneProps = {
    timelineRows,
    registerPane: linked.registerPane,
    onCrosshairMove,
    onCrosshairClear,
    externalCrosshairTime: timeline?.crosshairTime ?? null,
    drawings: drawingsApi?.drawings ?? [],
    drawingTool,
    selectedDrawingId: drawingsApi?.selectedId ?? null,
    onSelectDrawing: drawingsApi?.setSelectedId,
    onDrawingCommit: handleDrawingCommit,
    drawingMode: toolBlocksPan,
  }

  return (
    <div className="positioning-chart-stack pos-chart-card positioning-chart-stack--cot3y positioning-chart-stack--integrated positioning-chart-stack--canvas positioning-chart-stack--synced">
      <PositioningChartChrome
        fullSeries={timelineRows}
        rangeId={rangeId}
        onRangeChange={onRangeChange}
        compact
      />
      <WorkstationDrawingToolbar
        activeTool={drawingTool}
        onToolChange={drawingsApi?.setActiveTool}
        onClear={drawingsApi?.clearDrawings}
        drawingCount={drawingsApi?.drawings?.length ?? 0}
      />
      {dataWarnings.map((w) => (
        <WorkstationDataErrorPanel key={w} message={w} />
      ))}
      {marketId ? (
        <GoldPriceTruthPanel
          marketId={marketId}
          valuationBlock={valuationBlock}
          priceContext={priceContext}
          displaySnapshot={goldDisplaySnapshot}
        />
      ) : null}
      <ChartCrosshairHeader point={readoutPoint} supplement={null} />
      <div className="pos-chart-stack pos-chart-stack--synced" ref={chartStackRef}>
        <div className="pos-chart-panel pos-chart-panel--candles" data-panel={PANEL_IDS.price}>
          <div className="pos-chart-panel-head pos-chart-panel-head--price">
            <span className="pos-chart-panel-title">Weekly price · OHLC</span>
            {ohlcSourceLabel ? (
              <span className="pos-chart-panel-source">OHLC source: {ohlcSourceLabel}</span>
            ) : null}
          </div>
          <div className="pos-chart-panel-price-labels">
            <LiveQuoteRefreshBar
              freshness={liveQuoteFreshness}
              fetchUrl={liveQuotesFetchUrl}
              fetchedAtMs={liveQuotesFetchedAtMs}
              refreshing={liveQuotesRefreshing}
              refreshError={liveQuotesRefreshError}
              onRefresh={refreshLiveQuotes}
              compact
            />
            {priceContext?.weeklyClose != null || priceContext?.chartClose != null ? (
              <span className="pos-chart-panel-price-label">
                Weekly close:{' '}
                <strong>{fmtPrice(priceContext.weeklyClose ?? priceContext.chartClose, 2)}</strong>
                {(priceContext.weeklyCloseDate ?? priceContext.chartCloseDate)
                  ? ` (${priceContext.weeklyCloseDate ?? priceContext.chartCloseDate})`
                  : ''}
              </span>
            ) : null}
            {priceContext?.liveStatus === 'UNAVAILABLE' ? (
              <span className="pos-chart-panel-price-label pos-chart-panel-price-label--unavailable">
                Live price: <strong>LIVE UNAVAILABLE</strong>
              </span>
            ) : priceContext?.liveMid != null ? (
              <span
                className={`pos-chart-panel-price-label pos-chart-panel-price-label--live${priceContext.liveQuoteStale ? ' pos-chart-panel-price-label--stale' : ''}`}
              >
                Live price:{' '}
                <strong>{fmtPrice(priceContext.liveMid, 2)}</strong>
                {priceContext.liveQuoteStale ? (
                  <span className="live-quote-stale-badge">STALE LIVE</span>
                ) : null}
                {priceContext.livePriceSource ? ` · ${priceContext.livePriceSource}` : ''}
                {priceContext.livePriceAsOf
                  ? ` · ${String(priceContext.livePriceAsOf).slice(0, 19)}`
                  : ''}
              </span>
            ) : null}
            {priceContext?.valuationStaleNote && priceContext?.valuationPriceUsed == null ? (
              <span className="pos-chart-panel-price-label">
                Valuation price: <strong>{priceContext.valuationStaleNote}</strong>
              </span>
            ) : priceContext?.valuationPriceUsed != null ? (
              <span className="pos-chart-panel-price-label">
                Valuation price:{' '}
                <strong>{fmtPrice(priceContext.valuationPriceUsed, 2)}</strong>
                {priceContext.valuationPriceSource ? ` · ${priceContext.valuationPriceSource}` : ''}
                {priceContext.valuationStaleNote ? (
                  <span className="pos-chart-panel-stale-note"> ({priceContext.valuationStaleNote})</span>
                ) : null}
              </span>
            ) : null}
          </div>
          {priceWarning ? <p className="pos-chart-panel-warn">{priceWarning}</p> : null}
          <div className="pos-chart-panel-body pos-chart-panel-body--canvas">
            <div
              className="pos-chart-panel-plot pos-chart-panel-plot--synced pos-chart-panel-plot--candles"
              style={{ height: WS_PRICE_PLOT_HEIGHT, minHeight: WS_PRICE_PLOT_HEIGHT }}
            >
              {visibleBars.length ? (
                <WorkstationChartPane
                  panelId={PANEL_IDS.price}
                  mode="candle"
                  candleBars={visibleBars}
                  livePrice={priceContext?.liveStatus === 'LIVE' ? priceContext?.liveMid : null}
                  {...sharedPaneProps}
                />
              ) : (
                <p className="pos-chart-panel-empty">Weekly OHLC unavailable.</p>
              )}
            </div>
            <CandleBadge
              candle={displayCandle}
              weekLabel={timeline?.crosshairLabel}
              isWeeklyDefault={!hoverCandle && displayCandle != null}
            />
          </div>
        </div>

        <CotPanelShell
          title="Commercial"
          shortLabel="Comm"
          panelId={PANEL_IDS.commercial}
          color={CHART_WS.commercial}
          dataKey="commercial_net"
          wowKey="commercial_wow"
          fullSeries={fullSeries}
          plotHeight={heightForPanel(PANEL_IDS.commercial)}
          onPlotResize={setPanelHeight}
          panelWarning={!model.hasCommercial ? 'Commercial net unavailable for this market.' : null}
        >
          <WorkstationChartPane
            panelId={PANEL_IDS.commercial}
            mode="line"
            lineColor={CHART_WS.commercial}
            linePoints={commercialPoints}
            zeroLine
            {...sharedPaneProps}
          />
        </CotPanelShell>

        <CotPanelShell
          title={instTitle}
          shortLabel="NC"
          panelId={PANEL_IDS.institutional}
          color={CHART_WS.institutional}
          dataKey="institutional_net"
          wowKey="institutional_wow"
          fullSeries={fullSeries}
          plotHeight={heightForPanel(PANEL_IDS.institutional)}
          onPlotResize={setPanelHeight}
        >
          <WorkstationChartPane
            panelId={PANEL_IDS.institutional}
            mode="line"
            lineColor={CHART_WS.institutional}
            linePoints={institutionalPoints}
            zeroLine
            {...sharedPaneProps}
          />
        </CotPanelShell>

        <CotPanelShell
          title={retailTitle}
          shortLabel="NR"
          panelId={PANEL_IDS.retail}
          color={CHART_WS.retail}
          dataKey="retail_net"
          wowKey="retail_wow"
          fullSeries={fullSeries}
          plotHeight={heightForPanel(PANEL_IDS.retail)}
          onPlotResize={setPanelHeight}
          panelWarning={!model.hasRetail ? 'Non-reportable net unavailable for this market.' : null}
        >
          <WorkstationChartPane
            panelId={PANEL_IDS.retail}
            mode="line"
            showTimeAxis
            lineColor={CHART_WS.retail}
            linePoints={retailPoints}
            zeroLine
            {...sharedPaneProps}
          />
        </CotPanelShell>
        <GlobalTimelineVlineOverlay
          stackRef={chartStackRef}
          getPaneChart={linked.getPaneChart}
          subscribeGeometry={linked.subscribeGeometry}
          timelineRows={timelineRows}
          drawings={drawingsApi?.drawings ?? []}
          selectedId={drawingsApi?.selectedId ?? null}
          activeTool={drawingTool}
          dateToTime={dateToTime}
          onSelectDrawing={drawingsApi?.setSelectedId}
          onDrawingCommit={handleDrawingCommit}
          onDrawingUpdate={handleVlineUpdate}
        />
      </div>
    </div>
  )
}
