import React from 'react'
import './annualTurningPointPanel.css'

const pct = (v) => (Number.isFinite(Number(v)) ? `${Math.round(Number(v) * 100)}%` : '—')
const move = (v) => {
  if (!Number.isFinite(Number(v))) return '—'
  const n = Number(v)
  return `${n > 0 ? '+' : ''}${n.toFixed(2)}%`
}

function TurnCard({ title, turn, tone }) {
  if (!turn?.available) return null
  const f4 = turn.forward_returns?.['4w'] || {}
  const f8 = turn.forward_returns?.['8w'] || {}
  const f12 = turn.forward_returns?.['12w'] || {}
  return (
    <div className={`atp-card atp-${tone}`}>
      <div className="atp-card-head">
        <div>
          <span className="atp-eyebrow">{title}</span>
          <strong>Week {turn.median_week}</strong>
        </div>
        <span className={`atp-status atp-status-${String(turn.status || '').toLowerCase()}`}>{turn.status}</span>
      </div>
      <div className="atp-grid">
        <div><strong>{turn.within_window_count}/{turn.sample_size}</strong><small>within ±{turn.window_half_width_weeks} weeks</small></div>
        <div><strong>{pct(turn.concentration)}</strong><small>historical concentration</small></div>
        <div><strong>{turn.distance_to_median_week}w</strong><small>from median turn</small></div>
        <div><strong>{move(f4.median_pct)}</strong><small>median next 4W</small></div>
        <div><strong>{move(f8.median_pct)}</strong><small>median next 8W</small></div>
        <div><strong>{move(f12.median_pct)}</strong><small>median next 12W</small></div>
      </div>
      <details className="atp-details">
        <summary>Historical turning years</summary>
        <div className="atp-years">
          {(turn.turns || []).map((row) => (
            <span key={`${row.year}-${row.week}`}>{row.year}: W{row.week}</span>
          ))}
        </div>
      </details>
    </div>
  )
}

export function AnnualTurningPointPanel({ instrumentId }) {
  const [doc, setDoc] = React.useState(null)
  const [error, setError] = React.useState(null)

  React.useEffect(() => {
    let cancelled = false
    fetch('/data/annual_turning_points_latest.json', { cache: 'no-store' })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((body) => !cancelled && setDoc(body))
      .catch((err) => !cancelled && setError(err?.message || 'unavailable'))
    return () => { cancelled = true }
  }, [])

  const row = (doc?.instrument_results || []).find((r) => r.instrument_id === instrumentId)
  const tp = row?.turning_points
  if (!doc) {
    return <section className="atp-panel"><h2>Annual Turning Points</h2><p>{error ? 'Turning-point scan unavailable.' : 'Loading annual turning-point scan…'}</p></section>
  }
  if (row?.status !== 'ok' || !tp?.available) {
    return <section className="atp-panel"><h2>{instrumentId} — Annual Turning Points</h2><p>No audited annual turning-point study is available for this market yet.</p></section>
  }

  const rp = tp.range_52w?.percentile
  return (
    <section className="atp-panel">
      <div className="atp-header">
        <div>
          <h2>{instrumentId} — Annual Turning Points</h2>
          <p>Where this market has historically made its yearly low and high. Uses completed years only and a 3-week median-smoothed weekly series.</p>
        </div>
        <div className="atp-context">
          <strong>Week {tp.current_week}</strong>
          <span>{tp.sample_size}Y sample</span>
          <span>52W location: {pct(rp)}</span>
        </div>
      </div>
      <div className="atp-cards">
        <TurnCard title="ANNUAL LOW WINDOW" turn={tp.annual_low} tone="low" />
        <TurnCard title="ANNUAL HIGH WINDOW" turn={tp.annual_high} tone="high" />
      </div>
      <p className="atp-foot">This is a timing study, not a buy/sell signal. The useful setup is when timing, 52-week price location, COT, valuation and your supply/demand location agree.</p>
    </section>
  )
}
