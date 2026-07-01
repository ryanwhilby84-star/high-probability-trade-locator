import React from 'react'
import { findTimelineRowByTime } from './data/normalizeWeeklyTimeline.js'
import { WeeklyCandlestickChart } from './charts/WeeklyCandlestickChart.jsx'
import { useWeeklyTimeline } from './context/WeeklyTimelineContext.jsx'
import { fmtPrice } from '../priceData.js'

function deviationLabel(pct) {
  if (pct == null || !Number.isFinite(Number(pct))) return '—'
  const n = Number(pct)
  return `${n >= 0 ? '+' : ''}${n.toFixed(1)}%`
}

function CrosshairReadout({ row, marketId }) {
  if (!row) {
    return <div className="irw-readout irw-readout--empty">Hover chart for weekly detail · scroll to zoom · drag to pan</div>
  }
  const dev = deviationLabel(row.deviation_pct)
  const extreme =
    row.deviation_pct != null && Math.abs(Number(row.deviation_pct)) >= 15 ? ' irw-readout--extreme' : ''
  return (
    <div className={`irw-readout${extreme}`}>
      <span className="irw-readout-date">{row.date}</span>
      <span>
        O {fmtPrice(row.open, 2)} H {fmtPrice(row.high, 2)} L {fmtPrice(row.low, 2)} C{' '}
        <strong>{fmtPrice(row.close, 2)}</strong>
      </span>
      <span className="irw-readout-fv">
        Fair {row.fair_value != null ? fmtPrice(row.fair_value, 2) : '—'}
        {row.fair_value != null ? ` · Dev ${dev}` : ''}
      </span>
      {row.model_id ? <span className="irw-readout-model">{row.model_id}</span> : null}
      {row.valuation_publish === false ? (
        <span className="irw-readout-withheld">withheld</span>
      ) : null}
    </div>
  )
}

/**
 * Master weekly candlestick + valuation overlay panel.
 * Requires WeeklyTimelineProvider from InstrumentWorkstationLayout.
 */
export function InstrumentResearchWorkstationPanel() {
  const {
    marketId,
    weeklyBars,
    fairValuePoints,
    timelineRows,
    hasValuationOverlay,
    valuationMeta,
    priceMeta,
    crosshairTime,
    setCrosshairTime,
    setVisibleRange,
  } = useWeeklyTimeline()

  const chartRef = React.useRef(null)

  const activeRow = React.useMemo(() => {
    if (!crosshairTime) return timelineRows[timelineRows.length - 1] || null
    return findTimelineRowByTime(timelineRows, crosshairTime) || null
  }, [crosshairTime, timelineRows])

  const handleCrosshair = React.useCallback(
    (payload) => {
      setCrosshairTime(payload?.time ?? null)
    },
    [setCrosshairTime],
  )

  if (!weeklyBars.length) {
    return (
      <div className="irw-panel irw-empty">
        <p>Weekly OHLC unavailable for {marketId}.</p>
        <p className="irw-muted">Ensure prices_latest.json includes daily or weekly bars.</p>
      </div>
    )
  }

  return (
    <div className="irw-body">
      <div className="irw-toolbar">
        <div className="irw-toolbar-title">Weekly price · institutional fair value</div>
        <div className="irw-toolbar-actions">
          <button type="button" className="irw-btn" onClick={() => chartRef.current?.resetZoom()}>
            Reset zoom
          </button>
        </div>
        <div className="irw-toolbar-meta">
          {weeklyBars.length} weeks
          {priceMeta.weeklySource === 'derived_from_daily' ? ' · OHLC derived from daily' : ''}
          {priceMeta.asOf ? ` · prices ${String(priceMeta.asOf).slice(0, 10)}` : ''}
          {hasValuationOverlay
            ? ` · valuation ${valuationMeta.nWithFairValue || 0} pts`
            : ' · valuation history pending export'}
        </div>
      </div>

      {!hasValuationOverlay ? (
        <div className="irw-banner">
          Historical fair-value overlay not loaded. Run visualization export:{' '}
          <code>python scripts/export_instrument_valuation_history_viz.py --market {marketId}</code>
        </div>
      ) : null}

      <CrosshairReadout row={activeRow} marketId={marketId} />

      <WeeklyCandlestickChart
        chartRef={chartRef}
        weeklyBars={weeklyBars}
        fairValuePoints={fairValuePoints}
        onCrosshairMove={handleCrosshair}
        onVisibleRangeChange={setVisibleRange}
      />

      <div className="irw-legend">
        <span className="irw-legend-item">
          <span className="irw-swatch irw-swatch--candle" /> Weekly OHLC
        </span>
        <span className="irw-legend-item">
          <span className="irw-swatch irw-swatch--fv" /> Institutional fair value (point-in-time)
        </span>
      </div>
    </div>
  )
}

/**
 * @deprecated Use InstrumentWorkstationLayout + InstrumentResearchWorkstationPanel
 */
export function InstrumentResearchWorkstation({ marketId }) {
  if (!marketId) return null
  return (
    <section className="irw-root" id="instrument-research-workstation" aria-label="Research workstation">
      <header className="irw-header">
        <h2 className="irw-heading">Research workstation</h2>
        <p className="irw-subheading">
          Weekly candles with point-in-time institutional valuation — no look-ahead
        </p>
      </header>
      <InstrumentResearchWorkstationPanel />
    </section>
  )
}
