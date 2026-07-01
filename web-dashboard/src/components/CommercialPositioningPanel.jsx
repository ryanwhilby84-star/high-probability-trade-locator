import React from 'react'

import { buildCommercialPositioningModel } from '../cot/commercialPositioning.js'
import { useLegacyCot } from '../hooks/useLegacyCot.js'
import { CotPositioningChart } from './CotPositioningChart.jsx'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

const fmtInt = (v) => {
  if (!isNum(v)) return '—'
  return Math.round(v).toLocaleString()
}

const fmtDelta = (v) => {
  if (!isNum(v)) return '—'
  const sign = v > 0 ? '+' : ''
  return `${sign}${Math.round(v).toLocaleString()}`
}

const fmtPct = (v) => {
  if (!isNum(v)) return '—'
  return `${v.toFixed(1)}%`
}

function Stat({ label, value, highlight, tone }) {
  return (
    <span className={`cot-ws-history-item${highlight ? ' cot-comm-stat-highlight' : ''}`}>
      <span className="cot-ws-history-k">{label}</span>
      <span className={`cot-ws-history-v${tone ? ` cot-comm-stat-${tone}` : ''}`}>{value}</span>
    </span>
  )
}

export function CommercialPositioningPanel({ instrumentId, visibleRange = null }) {
  const { instrumentData, loading, error } = useLegacyCot(instrumentId)

  const model = React.useMemo(
    () => buildCommercialPositioningModel(instrumentData, visibleRange),
    [instrumentData, visibleRange],
  )

  if (loading) {
    return (
      <section className="chart-ws-panel cot-comm-panel" aria-label="Commercial positioning">
        <header className="chart-ws-panel-head">
          <h3 className="chart-ws-panel-title">Commercial positioning</h3>
        </header>
        <p className="chart-ws-empty">Loading commercial COT…</p>
      </section>
    )
  }

  if (error) {
    return (
      <section className="chart-ws-panel cot-comm-panel" aria-label="Commercial positioning">
        <header className="chart-ws-panel-head">
          <h3 className="chart-ws-panel-title">Commercial positioning</h3>
        </header>
        <p className="cot-chart-empty">Commercial data unavailable — {error}</p>
      </section>
    )
  }

  if (!model.available) {
    return (
      <section className="chart-ws-panel cot-comm-panel" aria-label="Commercial positioning">
        <header className="chart-ws-panel-head">
          <h3 className="chart-ws-panel-title">Commercial positioning</h3>
          <p className="chart-ws-panel-sub">Legacy CFTC commercial hedgers</p>
        </header>
        <p className="cot-chart-empty">Commercial data unavailable — {model.reason}</p>
      </section>
    )
  }

  const snap = model.snapshot
  const extremeTone = snap.extreme ? (snap.extreme_label === 'Top 10%' ? 'high' : 'low') : null

  return (
    <section className="chart-ws-panel cot-comm-panel" aria-label="Commercial positioning">
      <header className="chart-ws-panel-head">
        <div>
          <h3 className="chart-ws-panel-title">Commercial positioning</h3>
          <p className="chart-ws-panel-sub">
            Commercial long · short · net · {model.totalWeeks} weeks · {snap.source}
          </p>
        </div>
        <div className="cot-ws-history cot-comm-stats">
          <Stat label="Report" value={snap.reportDate || '—'} />
          <Stat label="Commercial net" value={fmtInt(snap.commercial_net)} highlight />
          <Stat label="Weekly Δ net" value={fmtDelta(snap.commercial_weekly_change)} />
          <Stat label="Commercial long" value={fmtInt(snap.commercial_long)} />
          <Stat label="Commercial short" value={fmtInt(snap.commercial_short)} />
          <Stat label="3Y percentile" value={fmtPct(snap.commercial_percentile)} />
          {snap.extreme ? (
            <Stat label="Extreme" value={snap.extreme_label || snap.percentile_class} tone={extremeTone} />
          ) : (
            <Stat label="Extreme" value="—" />
          )}
        </div>
      </header>

      <CotPositioningChart
        chartData={model.chartData}
        groupId="commercial"
        showBands="13"
        chartMode="full"
        highlightLatest
      />
    </section>
  )
}
