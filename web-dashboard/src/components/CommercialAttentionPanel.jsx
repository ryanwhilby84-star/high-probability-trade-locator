import React from 'react'
import { navigateToCotWorkstation, navigateToInstrument } from '../routing.js'
import { isRadarEligible } from '../radarEligibility.js'
import './commercialAttention.css'

const fmt = (v) => {
  if (v == null || v === '') return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  const sign = n > 0 ? '+' : ''
  return `${sign}${n.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
}

const pct = (v) => {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  return `${Number(v).toFixed(0)}th`
}

function tierClass(tier) {
  if (tier === 'high_attention') return 'ca-tier-high'
  if (tier === 'developing') return 'ca-tier-developing'
  if (tier === 'watchlist') return 'ca-tier-watch'
  return 'ca-tier-low'
}

function AttentionCard({ row }) {
  const c = row.commercial || {}
  const narr = row.narratives || {}
  const events = row.events || []

  return (
    <article className={`ca-card ${tierClass(row.attention_tier)}`}>
      <header className="ca-card-head">
        <h3 className="ca-card-title">
          {row.instrument}
          <span className={`ca-tier-pill ${tierClass(row.attention_tier)}`}>{row.attention_label}</span>
        </h3>
        <p className="ca-card-week">COT week {row.source_week || '—'}</p>
      </header>

      <p className="ca-summary">{narr.commercials || '—'}</p>

      {events.length ? (
        <div className="ca-events">
          <span className="ca-events-label">Detected signals</span>
          <ul>
            {events.map((e) => (
              <li key={e} className="ca-badge">
                {e}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      <div className="ca-details">
        <div className="ca-grid">
          <div>
            <h4>Supporting detail</h4>
            <ul className="ca-metrics">
              <li>1W change: {fmt(c.change_1w)}</li>
              <li>1W |Δ| percentile: {pct(c.change_1w_abs_percentile)}</li>
              <li>Net percentile: {pct(c.net_percentile)}</li>
              <li>4W / 12W: {fmt(c.change_4w)} / {fmt(c.change_12w)}</li>
            </ul>
          </div>
          <div>
            <h4>Commercial regime</h4>
            <p>{narr.commercial_regime || row.commercial_regime || '—'}</p>
            <h4 className="ca-subh">Non-Commercials</h4>
            <p>{narr.noncommercials || '—'}</p>
            <h4 className="ca-subh">Non-Reportables</h4>
            <p>{narr.nonreportables || '—'}</p>
            {narr.alignment ? <p className="ca-align">{narr.alignment}</p> : null}
          </div>
        </div>
      </div>

      <div className="ca-actions">
        <button type="button" className="ca-btn ca-btn-primary" onClick={() => navigateToCotWorkstation(row.instrument)}>
          Open COT Workstation
        </button>
        <button type="button" className="ca-btn" onClick={() => navigateToInstrument(row.instrument)}>
          Instrument
        </button>
      </div>

      <details className="ca-why">
        <summary>Why ranked here</summary>
        <ul>
          {(row.rank_reasons || []).map((r, i) => (
            <li key={`${row.instrument}-r${i}`}>{r}</li>
          ))}
        </ul>
        <p className="ca-evidence">Evidence points: {row.evidence_points} (transparent event weights — not a black-box score)</p>
      </details>
    </article>
  )
}

/**
 * Commercial-led weekly attention board for Market Radar.
 */
export function CommercialAttentionPanel({ doc, radarEligibleOnly = true, topN = 8 }) {
  const board = React.useMemo(() => {
    let rows = Array.isArray(doc?.attention_board) ? doc.attention_board : []
    if (radarEligibleOnly) rows = rows.filter((r) => isRadarEligible(r.instrument))
    return rows.slice(0, topN)
  }, [doc, radarEligibleOnly, topN])

  if (!doc) {
    return (
      <section className="ca-panel ca-empty" aria-label="Commercial COT attention">
        <h2 className="ca-title">Commercial COT Attention</h2>
        <p className="ca-meta">
          Loading commercial attention export… Run{' '}
          <code>python scripts/run_commercial_attention_engine.py</code> if empty.
        </p>
      </section>
    )
  }

  const summary = doc.summary || {}

  return (
    <section className="ca-panel" aria-label="Commercial COT attention">
      <header className="ca-header">
        <div>
          <h2 className="ca-title">Commercial COT Attention</h2>
          <p className="ca-sub">
            Unusual Commercial positioning with Non-Commercial and Non-Reportable context. Attention triage only — not
            buy/sell signals.
          </p>
        </div>
        <div className="ca-meta">
          <span>Week {doc.source_week || doc.calendar_week || '—'}</span>
          <span>
            High {summary.high_attention ?? '—'} · Developing {summary.developing ?? '—'} · Watch{' '}
            {summary.watchlist ?? '—'}
          </span>
        </div>
      </header>

      {board.length ? (
        <div className="ca-list">
          {board.map((row) => (
            <AttentionCard key={row.instrument} row={row} />
          ))}
        </div>
      ) : (
        <p className="ca-meta">No commercial attention rows for this week.</p>
      )}
    </section>
  )
}
