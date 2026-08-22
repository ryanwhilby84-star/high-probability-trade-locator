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

/** Presentation-only tone from free text (narratives / regimes / events). */
function toneFromText(text) {
  const t = String(text || '').toLowerCase()
  if (!t || t === '—') return 'neutral'
  if (
    t.includes('divergence') ||
    t.includes('transition') ||
    t.includes('extreme') ||
    t.includes('opposing') ||
    t.includes('fading') ||
    t.includes('weakening')
  ) {
    return 'warn'
  }
  const bear = t.includes('bear')
  const bull = t.includes('bull')
  if (bear && !bull) return 'bear'
  if (bull && !bear) return 'bull'
  if (t.includes('short') && !t.includes('short covering')) return 'bear'
  if (t.includes('long') || t.includes('accumulation')) return 'bull'
  if (t.includes('rising') || t.includes('strengthen')) return 'bull'
  if (t.includes('falling')) return 'bear'
  return 'neutral'
}

function toneFromSigned(n) {
  if (n == null || !Number.isFinite(Number(n)) || Number(n) === 0) return 'neutral'
  return Number(n) > 0 ? 'bull' : 'bear'
}

/** High commercial net percentile ≈ long extreme (bullish); low ≈ short extreme. */
function toneFromNetPercentile(p) {
  if (p == null || !Number.isFinite(Number(p))) return 'neutral'
  const n = Number(p)
  if (n >= 80) return 'bull'
  if (n <= 20) return 'bear'
  return 'neutral'
}

function toneClass(tone, soft = false) {
  const base = soft ? `hptl-tone-${tone}-soft` : `hptl-tone-${tone}`
  return tone ? base : 'hptl-tone-neutral'
}

function MetricRow({ label, value, tone }) {
  return (
    <li className="ca-metric-row">
      <span className="ca-metric-label">{label}</span>
      <span className={`ca-metric-value ${toneClass(tone)}`}>{value}</span>
    </li>
  )
}

function AttentionCard({ row }) {
  const c = row.commercial || {}
  const narr = row.narratives || {}
  const events = row.events || []
  const primaryNarrative = narr.commercials || '—'
  const regimeText = narr.commercial_regime || row.commercial_regime || '—'
  const regimeTone = toneFromText(regimeText)
  const narrativeTone = toneFromText(primaryNarrative)

  return (
    <article className={`ca-card ${tierClass(row.attention_tier)}`}>
      <header className="ca-card-head">
        <div className="ca-card-title-row">
          <h3 className="ca-card-title">{row.instrument}</h3>
          <span className={`ca-tier-pill ${tierClass(row.attention_tier)}`}>{row.attention_label}</span>
        </div>
        <p className="ca-card-week">COT week {row.source_week || '—'}</p>
      </header>

      <p className={`ca-summary ${toneClass(narrativeTone)}`}>{primaryNarrative}</p>

      <div className={`ca-regime ${toneClass(regimeTone, true)}`}>
        <span className="ca-regime-label">Commercial state</span>
        <span className={`ca-regime-value ${toneClass(regimeTone)}`}>{regimeText}</span>
      </div>

      {events.length ? (
        <div className="ca-events">
          <span className="ca-events-label">Detected signals</span>
          <ul>
            {events.map((e) => {
              const tone = toneFromText(e)
              return (
                <li key={e} className={`ca-badge ${toneClass(tone, true)}`}>
                  {e}
                </li>
              )
            })}
          </ul>
        </div>
      ) : null}

      <div className="ca-details">
        <div className="ca-grid">
          <div>
            <h4>Key levels</h4>
            <ul className="ca-metrics">
              <MetricRow label="Net percentile" value={pct(c.net_percentile)} tone={toneFromNetPercentile(c.net_percentile)} />
              <MetricRow label="1W change" value={fmt(c.change_1w)} tone={toneFromSigned(c.change_1w)} />
              <MetricRow
                label="1W |Δ| percentile"
                value={pct(c.change_1w_abs_percentile)}
                tone={
                  Number(c.change_1w_abs_percentile) >= 80
                    ? 'warn'
                    : Number(c.change_1w_abs_percentile) >= 60
                      ? 'info'
                      : 'neutral'
                }
              />
              <MetricRow
                label="4W / 12W"
                value={`${fmt(c.change_4w)} / ${fmt(c.change_12w)}`}
                tone={toneFromSigned(c.change_4w)}
              />
            </ul>
          </div>
          <div>
            <h4>Context</h4>
            <p className={`ca-context-line ca-accent-${toneFromText(narr.noncommercials)}`}>
              <span className="ca-context-k">Non-Commercials</span>
              <span className="ca-context-body">{narr.noncommercials || '—'}</span>
            </p>
            <p className={`ca-context-line ca-accent-${toneFromText(narr.nonreportables)}`}>
              <span className="ca-context-k">Non-Reportables</span>
              <span className="ca-context-body">{narr.nonreportables || '—'}</span>
            </p>
            {narr.alignment ? (
              <p className={`ca-align ca-accent-${toneFromText(narr.alignment)}`}>{narr.alignment}</p>
            ) : null}
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
