import { AssetIcon } from './AssetIcon.jsx'
import { attentionForWeek } from '../data/attentionWeek.js'
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


function ExtremeAttentionBoard({ historical, radarEligibleOnly, loading, errored, error }) {
  const [showAll, setShowAll] = React.useState(false)
  const [lookback, setLookback] = React.useState("all")
  const { rows, latestDate, eligibleCount } = React.useMemo(() => {
    const all = Array.isArray(historical?.rows) ? historical.rows : []
    const selected = all.map(v => {
      const p = v.percentilesByWindow?.[lookback]
      if (!p) return null // Never display a misleading fallback percentile.
      const c = p.c, nc = p.nc, nr = p.nr
      return { ...v, c, nc, nr,
        commercialExtreme: c != null && (c >= 80 || c <= 20),
        retailExtreme: nr != null && (nr >= 80 || nr <= 20) }
    }).filter(Boolean)
    return { rows: radarEligibleOnly ? selected.filter(v => isRadarEligible(v.name)) : selected,
      latestDate: historical?.latestDate || null, eligibleCount: historical?.eligibleCount || 0 }
  }, [historical, radarEligibleOnly, lookback])
  const isLoading = !historical && !errored
  const extremes = rows.filter(v => v.commercialExtreme || v.retailExtreme)
  const displayed = showAll ? rows : extremes
  const tile = { background: '#111418', border: '1px solid #343a40', borderRadius: 10, padding: 16 }
  return <section aria-label="New 80/20 Pay Attention Board" style={{ background: '#0b0d0f', border: '2px solid #8dc63f', borderRadius: 12, padding: 18, marginBottom: 22 }}>
    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: 12, marginBottom: 18 }}>
      <div><div style={{ color: '#9bd84d', fontSize: 11, letterSpacing: 2, fontWeight: 800 }}>NEW · WEEKLY POSITIONING RADAR</div>
        <h2 style={{ margin: '6px 0', color: '#f4f7fa', fontSize: 25 }}>PAY ATTENTION <span style={{ color: '#e7a06d' }}>80 / 20</span></h2>
        <p style={{ margin: 0, color: '#9aa0a6', fontSize: 13 }}>Commercial and Non-Reportable extremes independently trigger alerts. Non-Commercial positioning is shown for timing context.</p>
      </div>
      <div style={{ color: '#cfd3d7', textAlign: 'right', fontSize: 13 }}>COT report <strong>{isLoading ? 'Loading…' : (latestDate || '—')}</strong><div>{isLoading ? 'Loading alerts and market percentiles…' : `${extremes.length} alerts · ${rows.length} markets with percentiles`}</div></div>
    </div>
    <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' }}>
      <label style={{ color: '#cfd3d7', fontSize: 12 }} htmlFor="attention-lookback">Inspector lookback</label>
      <select id="attention-lookback" value={lookback} onChange={e => setLookback(e.target.value)} style={{ background: '#171a1e', color: '#fff', padding: '8px', borderRadius: 6 }}>
        {[["3y","3Y"],["5y","5Y"],["10y","10Y"],["15y","15Y"],["all","ALL"]].map(([id,label]) => <option key={id} value={id}>{label}</option>)}
      </select>
      <button type="button" onClick={() => setShowAll(false)} aria-pressed={!showAll} style={{ padding: '8px 14px', background: !showAll ? '#276a80' : '#1d2b39', border: '1px solid #456176', borderRadius: 7, color: '#fff', cursor: 'pointer' }}>80/20 alerts ({isLoading ? '…' : extremes.length})</button>
      <button type="button" onClick={() => setShowAll(true)} aria-pressed={showAll} style={{ padding: '8px 14px', background: showAll ? '#276a80' : '#1d2b39', border: '1px solid #456176', borderRadius: 7, color: '#fff', cursor: 'pointer' }}>All positioning ({isLoading ? '…' : rows.length})</button>
    </div>
    {errored ? <p role="alert" style={{ color: '#e7a06d' }}>Historical COT data could not be loaded. {error?.message || 'Please retry the page.'} The alert count is unavailable, not zero.</p> :
    !historical ? <p style={{ color: '#c6d5e1' }}>Loading historical COT series…</p> :
      !rows.length ? <p style={{ color: '#e7a06d' }}>No eligible historical positioning series with 52+ valid weeks. Historical export or market mapping needs checking ({eligibleCount} candidates).</p> :
      !displayed.length ? <p style={{ color: '#c6d5e1' }}>No 80/20 extremes for {latestDate}. Select “All positioning” to inspect every available percentile.</p> :
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(min(100%,320px),1fr))', gap: 12 }}>
        {displayed.map(v => <article key={v.name} style={tile}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, alignItems: 'center' }}>
            <h3 style={{ margin: 0, fontSize: 18, color: '#fff', display: 'flex', alignItems: 'center', gap: 9 }}><AssetIcon name={v.name} size={30} /><span>{v.name}</span></h3>
            <span style={{ color: v.commercialExtreme || v.retailExtreme ? '#e7a06d' : '#aeb4ba', fontWeight: 700, fontSize: 11 }}>{v.commercialExtreme && v.retailExtreme ? 'DUAL EXTREME' : v.commercialExtreme ? 'COMMERCIAL ALERT' : v.retailExtreme ? 'RETAIL ALERT' : 'MONITOR'}</span>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,minmax(0,1fr))', gap: 7, margin: '14px 0' }}>
            {[['COMMERCIAL',v.c],['NON-COMM',v.nc],['NON-REPORT',v.nr]].map(([label,n]) => <div key={label} style={{ background: '#171a1e', padding: '12px 5px', textAlign: 'center', borderRadius: 7, border: n != null && (n >= 80 || n <= 20) ? '1px solid #d49b4f' : '1px solid #383e44' }}>
              <div style={{ fontSize: 10, color: '#aeb4ba' }}>{label}</div><div style={{ fontSize: 23, color: n != null && (n >= 80 || n <= 20) ? '#a8e15d' : '#fff', fontWeight: 800 }}>{n == null ? '—' : n.toFixed(0) + '%'}</div>
            </div>)}
          </div>
          <div style={{ color: '#cfd3d7', fontSize: 12 }}>Commercial WoW net change: <strong style={{ color: '#fff' }}>{fmt(v.change)}</strong></div>
          <div style={{ color: '#b8c9d6', fontSize: 12, marginTop: 5 }}>Non-Commercial WoW net change: <strong style={{ color: '#fff' }}>{fmt(v.ncChange)}</strong></div>
          <p style={{ color: '#9aa0a6', fontSize: 12, margin: '9px 0' }}>{v.retailExtreme && !v.commercialExtreme ? 'Non-Reportable crowding: check Commercial alignment and Non-Commercial rotation.' : 'Commercial positioning extreme: check Non-Commercial rotation and fresh higher-timeframe supply/demand.'}</p>
          <button className="ca-btn ca-btn-primary" type="button" onClick={() => navigateToCotWorkstation(v.name)}>Open COT Workstation →</button>
        </article>)}
      </div>}
    <p style={{ color: '#8d949b', fontSize: 11, margin: '15px 0 0' }}>COT range index (0–100), not a statistical percentile rank, for the selected Inspector lookback ({lookback.toUpperCase()}). Extremes are attention triggers, not trade entries. Commercial and Non-Reportable extremes are the primary attention triggers.</p>
    <div style={{ textAlign: 'right', marginTop: 12, fontSize: 10, letterSpacing: 1.3, color: '#717880' }}>POWERED BY <a href="https://www.ztack.co.uk/" target="_blank" rel="noopener noreferrer" style={{ color: '#9bd84d', textDecoration: 'none', fontWeight: 800 }}>ZTACK</a></div>
  </section>
}

export function CommercialAttentionPanel({ date, radarEligibleOnly = true }) {
  const [state, setState] = React.useState({ doc: null, loading: true, error: null })
  React.useEffect(() => {
    const controller = new AbortController()
    setState({ doc: null, loading: true, error: null })
    fetch('/data/cot_attention_80_20_latest.json', { signal: controller.signal, cache: 'no-store' })
      .then(r => r.ok ? r.json() : Promise.reject(new Error('HTTP ' + r.status)))
      .then(async doc => {
        if (date && date !== doc.latestDate) {
          const response = await fetch('/data/cot_weekly_inspector_latest.json', { signal: controller.signal, cache: 'no-store' })
          if (!response.ok) throw new Error('Weekly Inspector HTTP ' + response.status)
          doc = attentionForWeek(await response.json(), date)
        }
        if (controller.signal.aborted) return
        if (!Array.isArray(doc?.rows) || !doc.latestDate) throw new Error('Invalid attention snapshot')
        setState({ doc, loading: false, error: null })
      })
      .catch(error => { if (!controller.signal.aborted) setState({ doc: null, loading: false, error }) })
    return () => controller.abort()
  }, [date])
  return <ExtremeAttentionBoard historical={state.doc} loading={state.loading} errored={Boolean(state.error)} error={state.error} radarEligibleOnly={radarEligibleOnly} />
}
