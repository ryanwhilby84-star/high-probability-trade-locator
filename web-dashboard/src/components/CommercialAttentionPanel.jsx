import React from 'react'
import { useCot3ySeries } from '../hooks/useCot3ySeries.js'
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


function ExtremeAttentionBoard({ historical, radarEligibleOnly }) {
  const [showAll, setShowAll] = React.useState(false)
  const { rows, latestDate, eligibleCount } = React.useMemo(() => {
    const blocks = historical?.markets || historical?.instruments || historical?.data || {}
    const entries = Array.isArray(blocks) ? blocks.map(v => [v.market || v.instrument, v]) : Object.entries(blocks)
    const rank = (series, field) => {
      const values = series.slice(-156).map(v => v[field] == null ? null : Number(v[field])).filter(Number.isFinite)
      if (values.length < 52 || !Number.isFinite(Number(series.at(-1)?.[field]))) return null
      const n = Number(series.at(-1)[field])
      return Math.round(1000 * (values.filter(v => v < n).length + (values.filter(v => v === n).length - 1) / 2) / (values.length - 1)) / 10
    }
    const eligible = entries.flatMap(([key, block]) => {
      const name = block?.market || key
      if (radarEligibleOnly && !isRadarEligible(name) && !isRadarEligible(key)) return []
      const series = Array.isArray(block?.series) ? block.series : []
      if (!series.length) return []
      const last = series.at(-1)
      const c = rank(series, 'commercial_net'), nc = rank(series, 'institutional_net'), nr = rank(series, 'retail_net')
      if (c == null && nc == null && nr == null) return []
      return [{ name, date: String(last.date || '').slice(0, 10), c, nc, nr,
        change: last.commercial_wow == null ? null : Number(last.commercial_wow) }]
    })
    const latestDate = eligible.map(v => v.date).sort().at(-1) || null
    const rows = eligible.filter(v => v.date === latestDate).map(v => ({
      ...v, commercialExtreme: v.c != null && (v.c >= 80 || v.c <= 20),
      retailExtreme: v.nr != null && (v.nr >= 80 || v.nr <= 20),
    })).sort((a,b) => Number(b.commercialExtreme || b.retailExtreme) - Number(a.commercialExtreme || a.retailExtreme) ||
      Math.max(Math.abs((b.c ?? 50) - 50), Math.abs((b.nr ?? 50) - 50)) - Math.max(Math.abs((a.c ?? 50) - 50), Math.abs((a.nr ?? 50) - 50)))
    return { rows, latestDate, eligibleCount: eligible.length }
  }, [historical, radarEligibleOnly])
  const extremes = rows.filter(v => v.commercialExtreme || v.retailExtreme)
  const displayed = showAll ? rows : extremes
  const tile = { background: '#111c27', border: '1px solid #344c61', borderRadius: 10, padding: 16 }
  return <section aria-label="New 80/20 Pay Attention Board" style={{ background: '#0c1621', border: '2px solid #477f9b', borderRadius: 12, padding: 18, marginBottom: 22 }}>
    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: 12, marginBottom: 18 }}>
      <div><div style={{ color: '#71c9df', fontSize: 11, letterSpacing: 2, fontWeight: 800 }}>NEW · WEEKLY POSITIONING RADAR</div>
        <h2 style={{ margin: '6px 0', color: '#f4f7fa', fontSize: 25 }}>PAY ATTENTION <span style={{ color: '#f0bd72' }}>80 / 20</span></h2>
        <p style={{ margin: 0, color: '#a8bacb', fontSize: 13 }}>Commercial and Non-Reportable extremes independently trigger alerts. Non-Commercial positioning is shown for timing context.</p>
      </div>
      <div style={{ color: '#c8d6e3', textAlign: 'right', fontSize: 13 }}>COT report <strong>{latestDate || '—'}</strong><div>{extremes.length} alerts · {rows.length} markets with percentiles</div></div>
    </div>
    <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap' }}>
      <button type="button" onClick={() => setShowAll(false)} aria-pressed={!showAll} style={{ padding: '8px 14px', background: !showAll ? '#276a80' : '#1d2b39', border: '1px solid #456176', borderRadius: 7, color: '#fff', cursor: 'pointer' }}>80/20 alerts ({extremes.length})</button>
      <button type="button" onClick={() => setShowAll(true)} aria-pressed={showAll} style={{ padding: '8px 14px', background: showAll ? '#276a80' : '#1d2b39', border: '1px solid #456176', borderRadius: 7, color: '#fff', cursor: 'pointer' }}>All positioning ({rows.length})</button>
    </div>
    {!historical ? <p style={{ color: '#c6d5e1' }}>Loading historical COT series…</p> :
      !rows.length ? <p style={{ color: '#f0bd72' }}>No eligible historical positioning series with 52+ valid weeks. Historical export or market mapping needs checking ({eligibleCount} candidates).</p> :
      !displayed.length ? <p style={{ color: '#c6d5e1' }}>No 80/20 extremes for {latestDate}. Select “All positioning” to inspect every available percentile.</p> :
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(min(100%,320px),1fr))', gap: 12 }}>
        {displayed.map(v => <article key={v.name} style={tile}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, alignItems: 'center' }}>
            <h3 style={{ margin: 0, fontSize: 18, color: '#fff' }}>{v.name}</h3>
            <span style={{ color: v.commercialExtreme || v.retailExtreme ? '#f0bd72' : '#9db6c9', fontWeight: 700, fontSize: 11 }}>{v.commercialExtreme && v.retailExtreme ? 'DUAL EXTREME' : v.commercialExtreme ? 'COMMERCIAL ALERT' : v.retailExtreme ? 'RETAIL ALERT' : 'MONITOR'}</span>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,minmax(0,1fr))', gap: 7, margin: '14px 0' }}>
            {[['COMMERCIAL',v.c],['NON-COMM',v.nc],['NON-REPORT',v.nr]].map(([label,n]) => <div key={label} style={{ background: '#223445', padding: '12px 5px', textAlign: 'center', borderRadius: 7, border: n != null && (n >= 80 || n <= 20) ? '1px solid #d49b4f' : '1px solid #34495b' }}>
              <div style={{ fontSize: 10, color: '#b6c8d6' }}>{label}</div><div style={{ fontSize: 23, color: n != null && (n >= 80 || n <= 20) ? '#f5c27c' : '#fff', fontWeight: 800 }}>{n == null ? '—' : n.toFixed(0) + '%'}</div>
            </div>)}
          </div>
          <div style={{ color: '#b8c9d6', fontSize: 12 }}>Commercial WoW net change: <strong style={{ color: '#fff' }}>{fmt(v.change)}</strong></div>
          <p style={{ color: '#a6b8c8', fontSize: 12, margin: '9px 0' }}>{v.retailExtreme && !v.commercialExtreme ? 'Non-Reportable crowding: check Commercial alignment and Non-Commercial rotation.' : 'Commercial positioning extreme: check Non-Commercial rotation and fresh higher-timeframe supply/demand.'}</p>
          <button className="ca-btn ca-btn-primary" type="button" onClick={() => navigateToCotWorkstation(v.name)}>Open COT Workstation →</button>
        </article>)}
      </div>}
    <p style={{ color: '#7f9aad', fontSize: 11, margin: '15px 0 0' }}>Percentile rank over up to 156 available COT weeks. Extremes are attention triggers, not trade entries. Commercial and Non-Reportable extremes are the primary attention triggers.</p>
  </section>
}

/** Market Radar's single 80/20 attention board. Legacy cards retired. */
export function CommercialAttentionPanel({ radarEligibleOnly = true }) {
  const { doc: historical } = useCot3ySeries()
  return <ExtremeAttentionBoard historical={historical} radarEligibleOnly={radarEligibleOnly} />
}
