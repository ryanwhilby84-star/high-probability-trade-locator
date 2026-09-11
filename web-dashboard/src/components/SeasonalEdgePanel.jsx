import React from 'react'
import { navigateToSeasonalityWorkstation } from '../routing.js'
import './seasonalEdgePanel.css'

const pct = (v) => (Number.isFinite(Number(v)) ? `${Math.round(Number(v) * 100)}%` : '—')
const ret = (v) => {
  if (!Number.isFinite(Number(v))) return '—'
  const n = Number(v)
  return `${n > 0 ? '+' : ''}${n.toFixed(2)}%`
}

function EdgeRow({ edge, alert = false }) {
  const s15 = edge?.lookbacks?.['15Y'] || {}
  const s10 = edge?.lookbacks?.['10Y'] || {}
  const s5 = edge?.lookbacks?.['5Y'] || {}
  const dominant = edge.direction === 'Bullish' ? s15.bullish : s15.bearish
  const dirClass = edge.direction === 'Bullish' ? 'sep-bull' : edge.direction === 'Bearish' ? 'sep-bear' : ''
  return (
    <button
      type="button"
      className={`sep-row ${alert ? 'sep-row-alert' : ''}`}
      onClick={() => navigateToSeasonalityWorkstation(edge.instrument_id)}
    >
      <div className="sep-market">
        <strong>{edge.instrument_id}</strong>
        <span>{edge.window}</span>
      </div>
      <div>
        <span className={`sep-direction ${dirClass}`}>{edge.direction}</span>
        <small>{dominant ?? '—'}/{s15.n ?? '—'} in 15Y</small>
      </div>
      <div>
        <strong>{pct(s15.directional_frequency)}</strong>
        <small>15Y hit rate</small>
      </div>
      <div>
        <strong>{ret(s15.median_pct)}</strong>
        <small>median move</small>
      </div>
      <div>
        <strong>{pct(s10.directional_frequency)} · {pct(s5.directional_frequency)}</strong>
        <small>10Y · 5Y</small>
      </div>
      <div>
        <span className={`sep-grade sep-grade-${String(edge.grade || '').toLowerCase()}`}>{edge.grade}</span>
        <small>{edge.days_until_start === 0 ? 'active now' : `starts in ${edge.days_until_start}d`}</small>
      </div>
    </button>
  )
}

export function SeasonalEdgePanel() {
  const [doc, setDoc] = React.useState(null)
  const [error, setError] = React.useState(null)

  React.useEffect(() => {
    let cancelled = false
    fetch('/data/seasonal_edge_scan_latest.json', { cache: 'no-store' })
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((body) => {
        if (!cancelled) setDoc(body)
      })
      .catch((err) => {
        if (!cancelled) setError(err?.message || 'scan unavailable')
      })
    return () => {
      cancelled = true
    }
  }, [])

  if (!doc) {
    return (
      <section className="sep-panel">
        <div className="sep-header">
          <div>
            <h2>Seasonal Edge Alerts</h2>
            <p>Recurring calendar windows with unusually persistent 5Y / 10Y / 15Y behaviour.</p>
          </div>
        </div>
        <p className="sep-empty">
          {error ? 'Seasonal edge scan has not been exported yet.' : 'Loading seasonal edge scan…'}{' '}
          Run <code>python scripts/build_seasonal_edge_scan.py</code>.
        </p>
      </section>
    )
  }

  const alerts = Array.isArray(doc.alerts) ? doc.alerts.slice(0, 8) : []
  const edges = Array.isArray(doc.top_edges) ? doc.top_edges.filter((e) => !alerts.includes(e)).slice(0, 8) : []

  return (
    <section className="sep-panel" aria-label="Seasonal edge alerts">
      <div className="sep-header">
        <div>
          <h2>Seasonal Edge Alerts</h2>
          <p>
            Finds boring recurring behaviour before it arrives: strong calendar months and robust rolling windows across the seasonality universe.
          </p>
        </div>
        <div className="sep-meta">
          <strong>{doc.alert_count ?? alerts.length} live alerts</strong>
          <span>{doc.available_instruments ?? '—'}/{doc.instrument_count ?? '—'} markets scanned · {doc.asof || '—'}</span>
        </div>
      </div>

      {alerts.length ? (
        <div className="sep-block">
          <h3>Approaching now</h3>
          <div className="sep-list">
            {alerts.map((edge) => <EdgeRow key={`${edge.instrument_id}-${edge.window}`} edge={edge} alert />)}
          </div>
        </div>
      ) : <p className="sep-empty">No strong seasonal window starts within the next 14 days.</p>}

      {edges.length ? (
        <details className="sep-more">
          <summary>Next strongest seasonal patterns</summary>
          <div className="sep-list">
            {edges.map((edge) => <EdgeRow key={`${edge.instrument_id}-${edge.window}`} edge={edge} />)}
          </div>
        </details>
      ) : null}

      <p className="sep-foot">
        Edge grade requires more than a high hit rate: 5Y/10Y/15Y direction stability, mean/median agreement and nearby-window robustness are included to reduce calendar overfitting. COT alignment is the next layer, not baked into this score yet.
      </p>
    </section>
  )
}
