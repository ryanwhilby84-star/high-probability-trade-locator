import { percentileTone } from '../data/percentileTone.js'
import { AssetIcon } from './AssetIcon.jsx'
import { attentionForWeek } from '../data/attentionWeek.js'
import React from 'react'
import { navigateToCotWorkstation, navigateToInstrument } from '../routing.js'
import { isRadarEligible } from '../radarEligibility.js'
import './commercialAttention.css'
import { savePositioningPlan } from '../data/positioningPlans.js'
import { navigateToTradePlanner } from '../routing.js'

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



function attentionLevel(v) {
    const c = v.c, nr = v.nr
    const extreme = n => n != null && Number.isFinite(Number(n)) && (n <= 20 || n >= 80)
    if (!extreme(c) && !extreme(nr)) return 0
    if (extreme(c) && extreme(nr) && ((c <= 20 && nr >= 80) || (c >= 80 && nr <= 20))) return 2
    return 1
}
// Single presentation shared by the Radar and an immortalized Planner snapshot.
// No recalculation or mutable data is introduced when the card is copied.
export function PositioningTile({ tile: v, rotation = null, lookback = '3y', reportDate = null, onAdd = null }) {
  const latestDate = reportDate
  return (<article key={v.name} className="ie-radar-card" style={{ display: 'flex', flexDirection: 'column', minHeight: 345, ...(attentionLevel(v) ? {
          position: 'relative',
          border: attentionLevel(v) === 2 ? '1px solid rgba(243,191,92,.78)' : '1px solid rgba(213,181,119,.46)',
          boxShadow: attentionLevel(v) === 2
            ? '0 0 0 1px rgba(243,191,92,.18), 0 0 26px rgba(220,153,47,.18), inset 0 1px 0 rgba(255,233,172,.16)'
            : '0 0 16px rgba(210,163,72,.08), inset 0 1px 0 rgba(255,233,172,.08)',
          backgroundImage: 'linear-gradient(150deg,rgba(255,223,155,.09),transparent 36%)'
        } : {}) }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, alignItems: 'center' }}>
            <h3 style={{ margin: 0, fontSize: 18, color: '#fff', display: 'flex', alignItems: 'center', gap: 9 }}><AssetIcon name={v.name} size={30} /><span>{v.name}</span></h3>
            {attentionLevel(v) ? <span aria-label={attentionLevel(v) === 2 ? 'High attention: opposing commercial and non-reportable extremes' : 'Attention: commercial or non-reportable extreme'} style={{
              display: 'inline-flex', alignItems: 'center', gap: 5, flexShrink: 0,
              borderRadius: 7, padding: '5px 8px', fontSize: 10, fontWeight: 900,
              letterSpacing: '.07em', color: '#fff1c6',
              border: '1px solid rgba(255,218,135,.6)',
              background: 'linear-gradient(180deg,#9d7134 0%,#59401e 48%,#392a18 100%)',
              boxShadow: 'inset 0 1px 0 rgba(255,240,190,.45),0 2px 10px rgba(214,154,55,.22)'
            }}><span aria-hidden="true" style={{ fontSize: 15, lineHeight: 1 }}>⚡</span>{attentionLevel(v) === 2 ? 'HIGH ATTENTION' : 'ATTENTION'}</span>
              : <span style={{ color: '#aeb4ba', fontWeight: 700, fontSize: 11 }}>MONITOR</span>}
          </div>
          {rotation ? (() => {
            const ev = rotation
            const bullish = ev.direction > 0
            const shortDate = new Date(ev.date + 'T12:00:00Z').toLocaleDateString('en-GB', { day: 'numeric', month: 'short', timeZone: 'UTC' })
            const label = ev.state === 'developing' ? 'NC ROTATION DEVELOPING'
              : ev.state === 'paused' ? 'NC ROTATION PAUSED'
              : ev.state === 'reversed' ? 'NC MOVE REVERSED' : 'NC AGGRESSIVE TURN'
            return <div style={{ marginTop: 12, padding: '10px 11px', borderRadius: 8, border: '1px solid rgba(107,186,225,.45)', background: 'linear-gradient(130deg,rgba(30,85,122,.35),rgba(21,35,54,.65))' }}>
              <div style={{ fontSize: 12, fontWeight: 850, color: bullish ? '#83dbb4' : '#f0a0a6' }}>{bullish ? '▲' : '▼'} {label}</div>
              <div style={{ fontSize: 12, color: '#d8f1ff', marginTop: 5 }}>{bullish ? 'Bullish' : 'Bearish'} NC movement · started {shortDate} ({ev.weeksAgo} reports ago)</div>
              <div style={{ fontSize: 11, color: '#aebdcb', marginTop: 4 }}>Initial move: {fmt(ev.delta)} contracts · aggression {ev.aggression}th percentile</div>
              <div style={{ fontSize: 11, color: '#aebdcb', marginTop: 3 }}>Since start: {fmt(ev.netMovement)} net · {ev.alignedWeeks}/{ev.observedWeeks} weeks aligned{ev.flip ? ' · net-zero flip' : ''}</div>
            </div>
          })() : null}
          <div style={{ margin: '14px 0', flex: 1 }}>
            {[['COMMERCIALS', v.c], ['NON-COMMERCIALS', v.nc], ['NON-REPORTABLES', v.nr]].map(([label, value]) => {
              const n = value == null || !Number.isFinite(Number(value)) ? null : Math.max(0, Math.min(100, Number(value)))
              const distance = n == null ? null : Math.min(n, 100 - n)
              const status = distance == null ? 'Unavailable' : distance <= 10 ? 'Deep extreme' : distance <= 20 ? 'Extreme' : distance <= 30 ? 'Approaching' : 'Neutral'
              // Position direction controls colour: low = red, middle = blue, high = green.
              // Interpolate through blue so every value has a consistent, readable meaning.
              const interpolate = (a, b, t) => a.map((v, i) => Math.round(v + (b[i] - v) * t))
              const rgb = n == null ? [113, 128, 150] : n <= 50
                ? interpolate([221, 76, 83], [102, 157, 211], n / 50)
                : interpolate([102, 157, 211], [43, 190, 119], (n - 50) / 50)
              const color = 'rgb(' + rgb.join(',') + ')'
              return <div key={label} style={{ marginBottom: 11 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, marginBottom: 5 }}>
                  <span style={{ color: '#cbd6df', fontSize: 11, fontWeight: 700 }}>{label}</span>
                  <span style={{ color, fontSize: 12, fontWeight: 700 }}>{n == null ? '—' : n.toFixed(0) + '/100'} · {status}</span>
                </div>
                <div role="meter" aria-label={label + ' positioning range index'} aria-valuemin={0} aria-valuemax={100} aria-valuenow={n == null ? undefined : n} aria-valuetext={status} style={{ height: 9, borderRadius: 8, overflow: 'hidden', background: '#283744' }}>
                  {n != null ? <div style={{ width: n + '%', height: '100%', borderRadius: 8, background: color }} /> : null}
                </div>
              </div>
            })}
          </div>
          {[
            ['Commercial', v.change],
            ['Non-Commercial', v.ncChange],
          ].map(([group, value]) => {
            const n = value == null || value === '' ? null : Number(value)
            const tone = n == null || !Number.isFinite(n) || n === 0 ? '#aebdcb' : n > 0 ? '#72dfac' : '#f18c97'
            const direction = n == null || !Number.isFinite(n) || n === 0 ? 'unchanged or unavailable' : n > 0 ? 'net buying' : 'net selling'
            return <div key={group} style={{ color: '#b8c9d6', fontSize: 12, marginTop: group === 'Commercial' ? 0 : 5 }}>
              {group} WoW net change: <strong title={group + ' ' + direction} style={{ color: tone, fontVariantNumeric: 'tabular-nums' }}>{fmt(value)}</strong>
            </div>
          })}
          <p style={{ color: '#a4b3c5', fontSize: 12, margin: '9px 0' }}>{v.retailExtreme && !v.commercialExtreme ? 'Non-Reportable extreme: check Commercial alignment and Non-Commercial rotation.' : v.commercialExtreme ? 'Commercial extreme: check Non-Commercial rotation and supply/demand.' : 'Monitor developing positioning; inspect before making a trade decision.'}</p>
          {onAdd ? <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}><button className="ca-btn ca-btn-primary" type="button" onClick={() => onAdd?.(v, lookback, latestDate, rotation)}>＋ Add to Plan</button><button className="ca-btn" type="button" onClick={() => navigateToCotWorkstation(v.name)}>Open COT Workstation →</button></div> : null}
        </article>)
}

// Historical, point-in-time NC movement: identify aggressive turns at extremes,
// remember their onset, and distinguish sustained movement from a one-week reversal.
function ncRotation(block, snapshotDate, lookback, desired) {
  const index = { '3y': 4, '5y': 12, '10y': 14, '15y': 13, all: 15 }[lookback]
  const weeks = (block?.rows || []).filter(r =>
    Array.isArray(r) && typeof r[0] === 'string' && r[0] <= snapshotDate &&
    Number.isFinite(r[2]?.[0])
  ).sort((a,b) => a[0].localeCompare(b[0]))
  if (weeks.length < 54 || weeks[weeks.length - 1][0] !== snapshotDate) return null
  const recent = weeks.slice(-9)
  const events = []
  for (let i = Math.max(53, weeks.length - 9); i < weeks.length; i++) {
    const now = weeks[i], prev = weeks[i - 1]
    const net = now[2][0], prior = prev[2][0], delta = net - prior
    const baseline = weeks.slice(Math.max(0, i - 157), i).map(r => r[2][0])
    const moves = baseline.slice(1).map((v,j) => Math.abs(v - baseline[j])).sort((x,y)=>x-y)
    if (moves.length < 52) continue
    const aggression = Math.round(100 * moves.filter(v => v <= Math.abs(delta)).length / moves.length)
    const ncIndex = prev[2]?.[index]
    const cIndex = prev[1]?.[index], nrIndex = prev[3]?.[index]
    const ncExtreme = Number.isFinite(ncIndex) && (desired < 0 ? ncIndex >= 80 : ncIndex <= 20)
    const commercialExtreme = Number.isFinite(cIndex) && (desired < 0 ? cIndex <= 20 : cIndex >= 80)
    const retailExtreme = Number.isFinite(nrIndex) && (desired < 0 ? nrIndex >= 80 : nrIndex <= 20)
    if (Math.sign(delta) !== desired || !(ncExtreme && (commercialExtreme || retailExtreme))) continue
    const flip = (prior > 0 && net < 0) || (prior < 0 && net > 0)
    if (aggression >= 90 || flip) events.push({ date: now[0], delta, aggression, flip, index: i })
  }
  if (!events.length) return null
  const event = events[0]
  const last = weeks.length - 1
  const since = weeks.slice(event.index)
  const netNow = weeks[last][2][0]
  const netBefore = weeks[event.index - 1][2][0]
  const netMovement = netNow - netBefore
  const alignedWeeks = since.filter((r,j) => Math.sign(r[2][0] - weeks[event.index + j - 1][2][0]) === desired).length
  const recentMove = weeks[last][2][0] - weeks[last - 1][2][0]
  const stillAligned = Math.sign(netMovement) === desired
  return { ...event, weeksAgo: last - event.index, netMovement, alignedWeeks,
    observedWeeks: since.length, recentMove,
    state: !stillAligned ? 'reversed' : recentMove * desired <= 0 ? 'paused' : since.length >= 2 && alignedWeeks >= 2 ? 'developing' : 'initial',
    direction: desired }
}

function ExtremeAttentionBoard({ historical, inspector, radarEligibleOnly, loading, errored, error }) {
  const [savedNotice, setSavedNotice] = React.useState('')
  const addPlan = (v, lookback, latestDate, rotation) => {
    try {
      const item = savePositioningPlan({ market: v.name, reportDate: latestDate, lookback, tile: { ...v }, rotation: rotation || null })
      setSavedNotice(item.market + ' saved to Positioning Tracker')
    } catch (error) { setSavedNotice('Could not save: ' + error.message) }
  }
  const [showAll, setShowAll] = React.useState(true)
  const [lookback, setLookback] = React.useState("3y")
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
  const eventsByName = React.useMemo(() => {
    const out = {}
    const markets = inspector?.markets || {}
    const keys = Object.keys(markets)
    for (const row of rows) {
      if (!(row.commercialExtreme || row.retailExtreme) || !latestDate) continue
      const desired = row.commercialExtreme ? (row.c >= 80 ? 1 : -1) : (row.nr >= 80 ? -1 : 1)
      const key = keys.find(k => k.toLowerCase() === row.name.toLowerCase())
      if (key) out[row.name] = ncRotation(markets[key], latestDate, lookback, desired)
    }
    return out
  }, [inspector, rows, latestDate, lookback])
  const isLoading = !historical && !errored
  const extremes = rows.filter(v => v.commercialExtreme || v.retailExtreme)
  // Both groups independently trigger attention. Opposing extremes get a stronger
  // visual treatment, but are not trade entries or an inferred direction.

  const displayed = [...(showAll ? rows : extremes)].sort((a, b) =>
    attentionLevel(b) - attentionLevel(a) || Number(Boolean(eventsByName[b.name])) - Number(Boolean(eventsByName[a.name])) || a.name.localeCompare(b.name))
  return <section aria-label="New 80/20 Pay Attention Board" className="ie-radar">
    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: 12, marginBottom: 18 }}>
      <div><div style={{ color: '#a6bedc', fontSize: 11, letterSpacing: 2, fontWeight: 800 }}>NEW · WEEKLY POSITIONING RADAR</div>
        <h2 style={{ margin: '6px 0', color: '#f4f7fa', fontSize: 25 }}>PAY ATTENTION <span style={{ color: '#d5bd91' }}>80 / 20</span></h2>
        <p style={{ margin: 0, color: '#a4b3c5', fontSize: 13 }}>All markets shown by default. Commercial and Non-Reportable extremes lead attention; Non-Commercial positioning helps time flips.</p>
      </div>
      <div style={{ color: '#cfd3d7', textAlign: 'right', fontSize: 13 }}>Latest published COT report <strong>{isLoading ? 'Loading…' : (latestDate || '—')}</strong><div>{isLoading ? 'Loading alerts and market percentiles…' : `${extremes.length} alerts · ${rows.length} markets with percentiles`}</div></div>
    </div>
    <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' }}>
      <label style={{ color: '#cfd3d7', fontSize: 12 }} htmlFor="attention-lookback">Inspector lookback</label>
      <select id="attention-lookback" value={lookback} onChange={e => setLookback(e.target.value)} className="ie-radar-select">
        {[["3y","3Y"],["5y","5Y"],["10y","10Y"],["15y","15Y"],["all","ALL"]].map(([id,label]) => <option key={id} value={id}>{label}</option>)}
      </select>
      <button type="button" onClick={() => setShowAll(false)} aria-pressed={!showAll} className="ie-radar-toggle">80/20 alerts ({isLoading ? '…' : extremes.length})</button>
      <button type="button" onClick={() => setShowAll(true)} aria-pressed={showAll} className="ie-radar-toggle">All positioning ({isLoading ? '…' : rows.length})</button>
    </div>
    {savedNotice ? <div role="status" style={{ padding: 12, marginBottom: 12, borderRadius: 10, background: '#193c36', color: '#b5f2d6', display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center' }}><span>✓ {savedNotice}</span><button type="button" className="ca-btn" onClick={navigateToTradePlanner}>Open Planner →</button></div> : null}
    {errored ? <p role="alert" style={{ color: '#d5bd91' }}>Historical COT data could not be loaded. {error?.message || 'Please retry the page.'} The alert count is unavailable, not zero.</p> :
    !historical ? <p style={{ color: '#c6d5e1' }}>Loading historical COT series…</p> :
      !rows.length ? <p style={{ color: '#d5bd91' }}>No eligible historical positioning series with 52+ valid weeks. Historical export or market mapping needs checking ({eligibleCount} candidates).</p> :
      !displayed.length ? <p style={{ color: '#c6d5e1' }}>No 80/20 extremes for {latestDate}. Select “All positioning” to inspect every available percentile.</p> :
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(min(100%,320px),1fr))', gap: 12 }}>
        rcentileTone } from '../data/percentileTone.js'
import { AssetIcon } from './AssetIcon.jsx'
import { attentionForWeek } from '../data/attentionWeek.js'
import React from 'react'
import { navigateToCotWorkstation, navigateToInstrument } from '../routing.js'
import { isRadarEligible } from '../radarEligibility.js'
import './commercialAttention.css'
import { savePositioningPlan } from '../data/positioningPlans.js'
import { navigateToTradePlanner } from '../routing.js'

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



function attentionLevel(v) {
    const c = v.c, nr = v.nr
    const extreme = n => n != null && Number.isFinite(Number(n)) && (n <= 20 || n >= 80)
    if (!extreme(c) && !extreme(nr)) return 0
    if (extreme(c) && extreme(nr) && ((c <= 20 && nr >= 80) || (c >= 80 && nr <= 20))) return 2
    return 1
}
// Single presentation shared by the Radar and an immortalized Planner snapshot.
// No recalculation or mutable data is introduced when the card is copied.
export function PositioningTile({ tile: v, rotation = null, lookback = '3y', reportDate = null, onAdd = null }) {
  const latestDate = reportDate
  return (<article key={v.name} className="ie-radar-card" style={{ display: 'flex', flexDirection: 'column', minHeight: 345, ...(attentionLevel(v) ? {
          position: 'relative',
          border: attentionLevel(v) === 2 ? '1px solid rgba(243,191,92,.78)' : '1px solid rgba(213,181,119,.46)',
          boxShadow: attentionLevel(v) === 2
            ? '0 0 0 1px rgba(243,191,92,.18), 0 0 26px rgba(220,153,47,.18), inset 0 1px 0 rgba(255,233,172,.16)'
            : '0 0 16px rgba(210,163,72,.08), inset 0 1px 0 rgba(255,233,172,.08)',
          backgroundImage: 'linear-gradient(150deg,rgba(255,223,155,.09),transparent 36%)'
        } : {}) }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, alignItems: 'center' }}>
            <h3 style={{ margin: 0, fontSize: 18, color: '#fff', display: 'flex', alignItems: 'center', gap: 9 }}><AssetIcon name={v.name} size={30} /><span>{v.name}</span></h3>
            {attentionLevel(v) ? <span aria-label={attentionLevel(v) === 2 ? 'High attention: opposing commercial and non-reportable extremes' : 'Attention: commercial or non-reportable extreme'} style={{
              display: 'inline-flex', alignItems: 'center', gap: 5, flexShrink: 0,
              borderRadius: 7, padding: '5px 8px', fontSize: 10, fontWeight: 900,
              letterSpacing: '.07em', color: '#fff1c6',
              border: '1px solid rgba(255,218,135,.6)',
              background: 'linear-gradient(180deg,#9d7134 0%,#59401e 48%,#392a18 100%)',
              boxShadow: 'inset 0 1px 0 rgba(255,240,190,.45),0 2px 10px rgba(214,154,55,.22)'
            }}><span aria-hidden="true" style={{ fontSize: 15, lineHeight: 1 }}>⚡</span>{attentionLevel(v) === 2 ? 'HIGH ATTENTION' : 'ATTENTION'}</span>
              : <span style={{ color: '#aeb4ba', fontWeight: 700, fontSize: 11 }}>MONITOR</span>}
          </div>
          {rotation ? (() => {
            const ev = rotation
            const bullish = ev.direction > 0
            const shortDate = new Date(ev.date + 'T12:00:00Z').toLocaleDateString('en-GB', { day: 'numeric', month: 'short', timeZone: 'UTC' })
            const label = ev.state === 'developing' ? 'NC ROTATION DEVELOPING'
              : ev.state === 'paused' ? 'NC ROTATION PAUSED'
              : ev.state === 'reversed' ? 'NC MOVE REVERSED' : 'NC AGGRESSIVE TURN'
            return <div style={{ marginTop: 12, padding: '10px 11px', borderRadius: 8, border: '1px solid rgba(107,186,225,.45)', background: 'linear-gradient(130deg,rgba(30,85,122,.35),rgba(21,35,54,.65))' }}>
              <div style={{ fontSize: 12, fontWeight: 850, color: bullish ? '#83dbb4' : '#f0a0a6' }}>{bullish ? '▲' : '▼'} {label}</div>
              <div style={{ fontSize: 12, color: '#d8f1ff', marginTop: 5 }}>{bullish ? 'Bullish' : 'Bearish'} NC movement · started {shortDate} ({ev.weeksAgo} reports ago)</div>
              <div style={{ fontSize: 11, color: '#aebdcb', marginTop: 4 }}>Initial move: {fmt(ev.delta)} contracts · aggression {ev.aggression}th percentile</div>
              <div style={{ fontSize: 11, color: '#aebdcb', marginTop: 3 }}>Since start: {fmt(ev.netMovement)} net · {ev.alignedWeeks}/{ev.observedWeeks} weeks aligned{ev.flip ? ' · net-zero flip' : ''}</div>
            </div>
          })() : null}
          <div style={{ margin: '14px 0', flex: 1 }}>
            {[['COMMERCIALS', v.c], ['NON-COMMERCIALS', v.nc], ['NON-REPORTABLES', v.nr]].map(([label, value]) => {
              const n = value == null || !Number.isFinite(Number(value)) ? null : Math.max(0, Math.min(100, Number(value)))
              const distance = n == null ? null : Math.min(n, 100 - n)
              const status = distance == null ? 'Unavailable' : distance <= 10 ? 'Deep extreme' : distance <= 20 ? 'Extreme' : distance <= 30 ? 'Approaching' : 'Neutral'
              // Position direction controls colour: low = red, middle = blue, high = green.
              // Interpolate through blue so every value has a consistent, readable meaning.
              const interpolate = (a, b, t) => a.map((v, i) => Math.round(v + (b[i] - v) * t))
              const rgb = n == null ? [113, 128, 150] : n <= 50
                ? interpolate([221, 76, 83], [102, 157, 211], n / 50)
                : interpolate([102, 157, 211], [43, 190, 119], (n - 50) / 50)
              const color = 'rgb(' + rgb.join(',') + ')'
              return <div key={label} style={{ marginBottom: 11 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, marginBottom: 5 }}>
                  <span style={{ color: '#cbd6df', fontSize: 11, fontWeight: 700 }}>{label}</span>
                  <span style={{ color, fontSize: 12, fontWeight: 700 }}>{n == null ? '—' : n.toFixed(0) + '/100'} · {status}</span>
                </div>
                <div role="meter" aria-label={label + ' positioning range index'} aria-valuemin={0} aria-valuemax={100} aria-valuenow={n == null ? undefined : n} aria-valuetext={status} style={{ height: 9, borderRadius: 8, overflow: 'hidden', background: '#283744' }}>
                  {n != null ? <div style={{ width: n + '%', height: '100%', borderRadius: 8, background: color }} /> : null}
                </div>
              </div>
            })}
          </div>
          {[
            ['Commercial', v.change],
            ['Non-Commercial', v.ncChange],
          ].map(([group, value]) => {
            const n = value == null || value === '' ? null : Number(value)
            const tone = n == null || !Number.isFinite(n) || n === 0 ? '#aebdcb' : n > 0 ? '#72dfac' : '#f18c97'
            const direction = n == null || !Number.isFinite(n) || n === 0 ? 'unchanged or unavailable' : n > 0 ? 'net buying' : 'net selling'
            return <div key={group} style={{ color: '#b8c9d6', fontSize: 12, marginTop: group === 'Commercial' ? 0 : 5 }}>
              {group} WoW net change: <strong title={group + ' ' + direction} style={{ color: tone, fontVariantNumeric: 'tabular-nums' }}>{fmt(value)}</strong>
            </div>
          })}
          <p style={{ color: '#a4b3c5', fontSize: 12, margin: '9px 0' }}>{v.retailExtreme && !v.commercialExtreme ? 'Non-Reportable extreme: check Commercial alignment and Non-Commercial rotation.' : v.commercialExtreme ? 'Commercial extreme: check Non-Commercial rotation and supply/demand.' : 'Monitor developing positioning; inspect before making a trade decision.'}</p>
          {onAdd ? <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}><button className="ca-btn ca-btn-primary" type="button" onClick={() => onAdd?.(v, lookback, latestDate, rotation)}>＋ Add to Plan</button><button className="ca-btn" type="button" onClick={() => navigateToCotWorkstation(v.name)}>Open COT Workstation →</button></div> : null}
        </article>)
}

// Historical, point-in-time NC movement: identify aggressive turns at extremes,
// remember their onset, and distinguish sustained movement from a one-week reversal.
function ncRotation(block, snapshotDate, lookback, desired) {
  const index = { '3y': 4, '5y': 12, '10y': 14, '15y': 13, all: 15 }[lookback]
  const weeks = (block?.rows || []).filter(r =>
    Array.isArray(r) && typeof r[0] === 'string' && r[0] <= snapshotDate &&
    Number.isFinite(r[2]?.[0])
  ).sort((a,b) => a[0].localeCompare(b[0]))
  if (weeks.length < 54 || weeks[weeks.length - 1][0] !== snapshotDate) return null
  const recent = weeks.slice(-9)
  const events = []
  for (let i = Math.max(53, weeks.length - 9); i < weeks.length; i++) {
    const now = weeks[i], prev = weeks[i - 1]
    const net = now[2][0], prior = prev[2][0], delta = net - prior
    const baseline = weeks.slice(Math.max(0, i - 157), i).map(r => r[2][0])
    const moves = baseline.slice(1).map((v,j) => Math.abs(v - baseline[j])).sort((x,y)=>x-y)
    if (moves.length < 52) continue
    const aggression = Math.round(100 * moves.filter(v => v <= Math.abs(delta)).length / moves.length)
    const ncIndex = prev[2]?.[index]
    const cIndex = prev[1]?.[index], nrIndex = prev[3]?.[index]
    const ncExtreme = Number.isFinite(ncIndex) && (desired < 0 ? ncIndex >= 80 : ncIndex <= 20)
    const commercialExtreme = Number.isFinite(cIndex) && (desired < 0 ? cIndex <= 20 : cIndex >= 80)
    const retailExtreme = Number.isFinite(nrIndex) && (desired < 0 ? nrIndex >= 80 : nrIndex <= 20)
    if (Math.sign(delta) !== desired || !(ncExtreme && (commercialExtreme || retailExtreme))) continue
    const flip = (prior > 0 && net < 0) || (prior < 0 && net > 0)
    if (aggression >= 90 || flip) events.push({ date: now[0], delta, aggression, flip, index: i })
  }
  if (!events.length) return null
  const event = events[0]
  const last = weeks.length - 1
  const since = weeks.slice(event.index)
  const netNow = weeks[last][2][0]
  const netBefore = weeks[event.index - 1][2][0]
  const netMovement = netNow - netBefore
  const alignedWeeks = since.filter((r,j) => Math.sign(r[2][0] - weeks[event.index + j - 1][2][0]) === desired).length
  const recentMove = weeks[last][2][0] - weeks[last - 1][2][0]
  const stillAligned = Math.sign(netMovement) === desired
  return { ...event, weeksAgo: last - event.index, netMovement, alignedWeeks,
    observedWeeks: since.length, recentMove,
    state: !stillAligned ? 'reversed' : recentMove * desired <= 0 ? 'paused' : since.length >= 2 && alignedWeeks >= 2 ? 'developing' : 'initial',
    direction: desired }
}

function ExtremeAttentionBoard({ historical, inspector, radarEligibleOnly, loading, errored, error }) {
  const [savedNotice, setSavedNotice] = React.useState('')
  const addPlan = (v, lookback, latestDate, rotation) => {
    try {
      const item = savePositioningPlan({ market: v.name, reportDate: latestDate, lookback, tile: { ...v }, rotation: rotation || null })
      setSavedNotice(item.market + ' saved to Positioning Tracker')
    } catch (error) { setSavedNotice('Could not save: ' + error.message) }
  }
  const [showAll, setShowAll] = React.useState(true)
  const [lookback, setLookback] = React.useState("3y")
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
  const eventsByName = React.useMemo(() => {
    const out = {}
    const markets = inspector?.markets || {}
    const keys = Object.keys(markets)
    for (const row of rows) {
      if (!(row.commercialExtreme || row.retailExtreme) || !latestDate) continue
      const desired = row.commercialExtreme ? (row.c >= 80 ? 1 : -1) : (row.nr >= 80 ? -1 : 1)
      const key = keys.find(k => k.toLowerCase() === row.name.toLowerCase())
      if (key) out[row.name] = ncRotation(markets[key], latestDate, lookback, desired)
    }
    return out
  }, [inspector, rows, latestDate, lookback])
  const isLoading = !historical && !errored
  const extremes = rows.filter(v => v.commercialExtreme || v.retailExtreme)
  // Both groups independently trigger attention. Opposing extremes get a stronger
  // visual treatment, but are not trade entries or an inferred direction.

  const displayed = [...(showAll ? rows : extremes)].sort((a, b) =>
    attentionLevel(b) - attentionLevel(a) || Number(Boolean(eventsByName[b.name])) - Number(Boolean(eventsByName[a.name])) || a.name.localeCompare(b.name))
  return <section aria-label="New 80/20 Pay Attention Board" className="ie-radar">
    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: 12, marginBottom: 18 }}>
      <div><div style={{ color: '#a6bedc', fontSize: 11, letterSpacing: 2, fontWeight: 800 }}>NEW · WEEKLY POSITIONING RADAR</div>
        <h2 style={{ margin: '6px 0', color: '#f4f7fa', fontSize: 25 }}>PAY ATTENTION <span style={{ color: '#d5bd91' }}>80 / 20</span></h2>
        <p style={{ margin: 0, color: '#a4b3c5', fontSize: 13 }}>All markets shown by default. Commercial and Non-Reportable extremes lead attention; Non-Commercial positioning helps time flips.</p>
      </div>
      <div style={{ color: '#cfd3d7', textAlign: 'right', fontSize: 13 }}>Latest published COT report <strong>{isLoading ? 'Loading…' : (latestDate || '—')}</strong><div>{isLoading ? 'Loading alerts and market percentiles…' : `${extremes.length} alerts · ${rows.length} markets with percentiles`}</div></div>
    </div>
    <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' }}>
      <label style={{ color: '#cfd3d7', fontSize: 12 }} htmlFor="attention-lookback">Inspector lookback</label>
      <select id="attention-lookback" value={lookback} onChange={e => setLookback(e.target.value)} className="ie-radar-select">
        {[["3y","3Y"],["5y","5Y"],["10y","10Y"],["15y","15Y"],["all","ALL"]].map(([id,label]) => <option key={id} value={id}>{label}</option>)}
      </select>
      <button type="button" onClick={() => setShowAll(false)} aria-pressed={!showAll} className="ie-radar-toggle">80/20 alerts ({isLoading ? '…' : extremes.length})</button>
      <button type="button" onClick={() => setShowAll(true)} aria-pressed={showAll} className="ie-radar-toggle">All positioning ({isLoading ? '…' : rows.length})</button>
    </div>
    {savedNotice ? <div role="status" style={{ padding: 12, marginBottom: 12, borderRadius: 10, background: '#193c36', color: '#b5f2d6', display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center' }}><span>✓ {savedNotice}</span><button type="button" className="ca-btn" onClick={navigateToTradePlanner}>Open Planner →</button></div> : null}
    {errored ? <p role="alert" style={{ color: '#d5bd91' }}>Historical COT data could not be loaded. {error?.message || 'Please retry the page.'} The alert count is unavailable, not zero.</p> :
    !historical ? <p style={{ color: '#c6d5e1' }}>Loading historical COT series…</p> :
      !rows.length ? <p style={{ color: '#d5bd91' }}>No eligible historical positioning series with 52+ valid weeks. Historical export or market mapping needs checking ({eligibleCount} candidates).</p> :
      !displayed.length ? <p style={{ color: '#c6d5e1' }}>No 80/20 extremes for {latestDate}. Select “All positioning” to inspect every available percentile.</p> :
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(min(100%,320px),1fr))', gap: 12 }}>
        {displayed.map(v => <article key={v.name} className="ie-radar-card" style={{ display: 'flex', flexDirection: 'column', minHeight: 345, ...(attentionLevel(v) ? {
          position: 'relative',
          border: attentionLevel(v) === 2 ? '1px solid rgba(243,191,92,.78)' : '1px solid rgba(213,181,119,.46)',
 {displayed.map(v => <PositioningTile key={v.name} tile={v} rotation={eventsByName[v.name]} lookback={lookback} reportDate={latestDate} onAdd={addPlan} />)}primary attention triggers.</p>
    <div style={{ textAlign: 'right', marginTop: 12, fontSize: 10, letterSpacing: 1.3, color: '#717880' }}>POWERED BY <a href="https://www.ztack.co.uk/" target="_blank" rel="noopener noreferrer" style={{ color: '#a6bedc', textDecoration: 'none', fontWeight: 800 }}>ZTACK</a></div>
  </section>
}

// Scanner supplies the same validated Inspector snapshot as the workstation.
export function CommercialAttentionPanel({ radarEligibleOnly = true, inspectorState, selectedDate, latestDate }) {
  const { inspector, loading, error } = inspectorState
  const doc = React.useMemo(() => inspector && selectedDate ? attentionForWeek(inspector, selectedDate) : null, [inspector, selectedDate])
  const state = { inspector, loading, error, doc }
  // Fail closed after a weekly release: stale COT must never look current.
  const last = latestDate ? new Date(latestDate + 'T00:00:00Z') : null
  const daysOld = last && Number.isFinite(last.getTime()) ? (Date.now() - last.getTime()) / 86400000 : Infinity
  if (!state.loading && state.doc && (daysOld > 12 || daysOld < -2)) return <section role="alert" style={{ border: '2px solid #e48d6a', background: '#3b2023', color: '#fff1e9', padding: 22, borderRadius: 12, margin: '16px 0' }}>
    <h2 style={{ marginTop: 0 }}>⚠ STALE COT DATA — RADAR DISABLED</h2>
    <p>Published report: <strong>{latestDate}</strong>. This is {Math.max(0, Math.floor(daysOld))} days old. Current positioning and rotation signals cannot be trusted.</p>
    <p>The COT publishing pipeline must publish the latest Weekly Inspector before this Radar is usable.</p>
  </section>
  return <ExtremeAttentionBoard historical={state.doc} inspector={state.inspector} loading={state.loading} errored={Boolean(state.error)} error={state.error} radarEligibleOnly={radarEligibleOnly} />
}
