import React from 'react'
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)
const fmtSpot = (v) => (isNum(v) ? Number(v).toLocaleString(undefined, { maximumFractionDigits: 5 }) : '—')
const fmtDiff = (v) => (isNum(v) ? `${v >= 0 ? '+' : ''}${v.toFixed(2)}pp` : '—')

function PanelChart({ title, data, dataKey, yFormatter, height = 130, currentValue, currentLabel }) {
  const hasData = (data || []).length > 0
  return (
    <div className="fxvh-panel">
      <div className="fxvh-panel-head">
        <h4 className="fxvh-panel-title">{title}</h4>
        {currentLabel ? (
          <span className="fxvh-panel-current">
            Current {currentLabel}: <strong>{currentValue ?? '—'}</strong>
          </span>
        ) : null}
      </div>
      {!hasData ? (
        <p className="fxvh-panel-empty">Insufficient aligned rate/yield history for this panel.</p>
      ) : (
        <ResponsiveContainer width="100%" height={height}>
          <LineChart data={data} margin={{ top: 4, right: 12, left: 4, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.2)" />
            <XAxis dataKey="date" tick={{ fontSize: 9 }} minTickGap={48} />
            <YAxis tick={{ fontSize: 9 }} width={48} tickFormatter={yFormatter} domain={['auto', 'auto']} />
            <Tooltip
              contentStyle={{ fontSize: 11 }}
              formatter={(v) => [yFormatter(v), title]}
              labelFormatter={(l) => l}
            />
            <Line type="linear" dataKey={dataKey} stroke="#38bdf8" strokeWidth={1.5} dot={false} connectNulls />
          </LineChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}

function CurrentStrip({ current }) {
  if (!current) return null
  return (
    <div className="fxvh-current">
      <span>Spot: <strong>{fmtSpot(current.spot)}</strong></span>
      <span>2Y diff: <strong>{fmtDiff(current.yield_2y_diff)}</strong></span>
      <span>Policy diff: <strong>{fmtDiff(current.policy_rate_diff)}</strong></span>
      <span>10Y diff: <strong>{fmtDiff(current.yield_10y_diff)}</strong></span>
      <span>Grade: <strong>{current.valuation_grade || '—'}</strong></span>
    </div>
  )
}

/** Research-only FX valuation history (yield/rate differentials vs spot). */
export function FxValuationHistoryChart({ block }) {
  if (!block) return null
  if (!block.available) {
    return (
      <section className="fxvh-section">
        <h3 className="fxvh-title">FX Valuation History — Yield/Rate Differential V1</h3>
        <p className="fxvh-empty">
          {block.reason || 'Historical valuation chart unavailable — insufficient rate/yield history.'}
        </p>
      </section>
    )
  }

  const panels = block.panels || {}
  const current = block.current || {}
  const spotSeries = (panels.spot?.series || []).map((r) => ({ date: r.date, spot: r.spot }))
  const y2Series = (panels.yield_2y_diff?.series || []).map((r) => ({ date: r.date, value: r.value }))
  const polSeries = (panels.policy_rate_diff?.series || []).map((r) => ({ date: r.date, value: r.value }))
  const y10Series = (panels.yield_10y_diff?.series || []).map((r) => ({ date: r.date, value: r.value }))

  return (
    <section className="fxvh-section">
      <header className="fxvh-head">
        <div>
          <h3 className="fxvh-title">{block.title || 'FX Valuation History — Yield/Rate Differential V1'}</h3>
          <p className="fxvh-note">
            {block.note ||
              'This shows historical macro support versus spot price. Fair-value regression is not yet modelled.'}
          </p>
          <span className="sea-data-mode-badge sea-data-mode-badge--staging">Research only — not scored</span>
        </div>
      </header>

      <CurrentStrip current={current} />

      <div className="fxvh-panels">
        <PanelChart
          title="Panel A — FX spot price"
          data={spotSeries}
          dataKey="spot"
          yFormatter={fmtSpot}
          height={150}
          currentLabel="spot"
          currentValue={fmtSpot(current.spot)}
        />
        <PanelChart
          title="Panel B — 2Y yield differential (base − quote)"
          data={y2Series}
          dataKey="value"
          yFormatter={fmtDiff}
          currentLabel="2Y diff"
          currentValue={fmtDiff(current.yield_2y_diff)}
        />
        <PanelChart
          title="Panel C — Policy rate differential (base − quote)"
          data={polSeries}
          dataKey="value"
          yFormatter={fmtDiff}
          currentLabel="policy diff"
          currentValue={fmtDiff(current.policy_rate_diff)}
        />
        <PanelChart
          title="Panel D — 10Y yield differential (base − quote)"
          data={y10Series}
          dataKey="value"
          yFormatter={fmtDiff}
          currentLabel="10Y diff"
          currentValue={fmtDiff(current.yield_10y_diff)}
        />
      </div>
    </section>
  )
}
