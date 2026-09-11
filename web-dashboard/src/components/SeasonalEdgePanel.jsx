import React from 'react'
import { navigateToSeasonalityWorkstation } from '../routing.js'
import './seasonalEdgePanel.css'

const pct = (v) => (Number.isFinite(Number(v)) ? `${Math.round(Number(v) * 100)}%` : '—')
const ret = (v) => {
  if (!Number.isFinite(Number(v))) return '—'
  const n = Number(v)
  return `${n > 0 ? '+' : ''}${n.toFixed(2)}%`
}

function cotAlignment(edge, commercialAttention) {
  const rows = Array.isArray(commercialAttention?.attention_board) ? commercialAttention.attention_board : []
  const row = rows.find((r) => r.instrument === edge.instrument_id)
  if (!row) return { label: 'COT unavailable', tone: 'neutral', detail: null }

  const text = [
    row.commercial_regime,
    row.narratives?.commercial_regime,
    row.narratives?.commercials,
    row.narratives?.alignment,
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase()

  const bull = /bull|accumulat|long|buy|strength/.test(text) && !/bear/.test(text)
  const bear = /bear|distribut|short|sell|weak/.test(text) && !/bull/.test(text)
  const seasonalBull = edge.direction === 'Bullish'
  const seasonalBear = edge.direction === 'Bearish'

  if ((seasonalBull && bull) || (seasonalBear && bear)) {
    return { label: 'COT supportive', tone: 'support', detail: row.narratives?.commercials || row.commercial_regime || null }
  }
  if ((seasonalBull && bear) || (seasonalBear && bull)) {
    return { label: 'COT contradicts', tone: 'contradict', detail: row.narratives?.commercials || row.commercial_regime || null }
  }
  return { label: 'COT neutral', tone: 'neutral', detail: row.narratives?.commercials || row.commercial_regime || null }
}

function EdgeRow({ edge, alert = false, commercialAttention }) {
  const s15 = edge?.lookbacks?.['15Y'] || {}
  const s10 = edge?.lookbacks?.['10Y'] || {}
  const s5 = edge?.lookbacks?.['5Y'] || {}
  const dominant = edge.direction === 'Bullish' ? s15.bullish : s15.bearish
  const dirClass = edge.direction === 'Bullish' ? 'sep-bull' : edge.direction === 'Bearish' ? 'sep-bear' : ''
  const cot = cotAlignment(edge, commercialAttention)
  return (
    <button
      type="button"
      className={`sep-row ${alert ? 'sep-row-alert' : ''}`}
      onClick={() => navigateToSeasonalityWorkstation(edge.instrument_id)}
      title={cot.detail || edge.thesis || ''}
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
        <small className={`sep-cot sep-cot-${cot.tone}`}>{cot.label}</small>
      </div>
    </button>
  )
}

export function SeasonalEdgePanel({ commercialAttention = null }) {
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
            {alerts.map((edge) => <EdgeRow key={`${edge.instrument_id}-${edge.window}`} edge={edge} alert commercialAttention={commercialAttention} />)}
          </div>
        </div>
      ) : <p className="sep-empty">No strong seasonal window starts within the next 14 days.</p>}

      {edges.length ? (
        <details className="sep-more">
          <summary>Next strongest seasonal patterns</summary>
          <div className="sep-list">
            {edges.map((edge) => <EdgeRow key={`${edge.instrument_id}-${edge.window}`} edge={edge} commercialAttention={commercialAttention} />)}
          </div>
        </details>
      ) : null}

      <p className="sep-foot">
        Seasonal edge is ranked independently from COT. Each row then overlays Commercial positioning as supportive, contradictory or neutral so the seasonality can start a thesis without contaminating the historical score.
      </p>
    </section>
  )
}
