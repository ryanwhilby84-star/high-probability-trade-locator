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
  const text = [row.commercial_regime, row.narratives?.commercial_regime, row.narratives?.commercials, row.narratives?.alignment]
    .filter(Boolean).join(' ').toLowerCase()
  const bull = /bull|accumulat|long|buy|strength/.test(text) && !/bear/.test(text)
  const bear = /bear|distribut|short|sell|weak/.test(text) && !/bull/.test(text)
  if ((edge.direction === 'Bullish' && bull) || (edge.direction === 'Bearish' && bear)) return { label: 'COT supportive', tone: 'support', detail: row.narratives?.commercials || row.commercial_regime || null }
  if ((edge.direction === 'Bullish' && bear) || (edge.direction === 'Bearish' && bull)) return { label: 'COT contradicts', tone: 'contradict', detail: row.narratives?.commercials || row.commercial_regime || null }
  return { label: 'COT neutral', tone: 'neutral', detail: row.narratives?.commercials || row.commercial_regime || null }
}

function EdgeRow({ edge, alert = false, commercialAttention, activeInstrument = false }) {
  const s15 = edge?.lookbacks?.['15Y'] || {}
  const s10 = edge?.lookbacks?.['10Y'] || {}
  const s5 = edge?.lookbacks?.['5Y'] || {}
  const dominant = edge.direction === 'Bullish' ? s15.bullish : s15.bearish
  const dirClass = edge.direction === 'Bullish' ? 'sep-bull' : edge.direction === 'Bearish' ? 'sep-bear' : ''
  const cot = cotAlignment(edge, commercialAttention)
  return <button type="button" className={`sep-row ${alert ? 'sep-row-alert' : ''}`} onClick={() => !activeInstrument && navigateToSeasonalityWorkstation(edge.instrument_id)} title={cot.detail || edge.thesis || ''}>
    <div className="sep-market"><strong>{activeInstrument ? edge.window : edge.instrument_id}</strong><span>{activeInstrument ? edge.kind?.replace(/_/g, ' ') : edge.window}</span></div>
    <div><span className={`sep-direction ${dirClass}`}>{edge.direction}</span><small>{dominant ?? '—'}/{s15.n ?? '—'} in 15Y</small></div>
    <div><strong>{pct(s15.directional_frequency)}</strong><small>15Y hit rate</small></div>
    <div><strong>{ret(s15.median_pct)}</strong><small>median move</small></div>
    <div><strong>{pct(s10.directional_frequency)} · {pct(s5.directional_frequency)}</strong><small>10Y · 5Y</small></div>
    <div><span className={`sep-grade sep-grade-${String(edge.grade || '').toLowerCase()}`}>{edge.grade}</span><small>{edge.days_until_start === 0 ? 'active now' : `starts in ${edge.days_until_start}d`}</small><small className={`sep-cot sep-cot-${cot.tone}`}>{cot.label}</small></div>
  </button>
}

function PayValidation({ edges = [] }) {
  const rows = edges.map((edge) => {
    const s15 = edge?.lookbacks?.['15Y'] || {}
    const wins = edge.direction === 'Bullish' ? Number(s15.bullish || 0) : Number(s15.bearish || 0)
    const cases = Number(s15.n || 0)
    const hitRate = Number(s15.directional_frequency || 0)
    const avg = Number(s15.mean_pct)
    const median = Number(s15.median_pct)
    const directionPositive = edge.direction === 'Bullish' ? avg > 0 : avg < 0
    const passes = cases >= 6 && hitRate > 0.6 && directionPositive
    return { edge, s15, wins, cases, hitRate, avg, median, passes }
  })

  return <div className="sep-pay">
    <div className="sep-pay-heading">
      <div><h3>Pay Validation</h3><p>Has this seasonal setup actually paid before? A pass requires at least 6 historical cases, greater than 60% directional success and a positive average move in the expected direction.</p></div>
      <span className="sep-pay-count">{rows.filter((r) => r.passes).length}/{rows.length} pass</span>
    </div>
    {rows.length ? <div className="sep-pay-list">{rows.map(({ edge, s15, wins, cases, hitRate, avg, median, passes }) => (
      <details className={`sep-pay-card ${passes ? 'sep-pay-pass' : 'sep-pay-fail'}`} key={`pay-${edge.instrument_id}-${edge.window}`}>
        <summary>
          <div className="sep-pay-window"><strong>{edge.window}</strong><span className={edge.direction === 'Bullish' ? 'sep-bull' : 'sep-bear'}>{edge.direction}</span></div>
          <div><strong>{wins}/{cases}</strong><small>paid</small></div>
          <div><strong>{pct(hitRate)}</strong><small>hit rate</small></div>
          <div><strong>{ret(avg)}</strong><small>average move</small></div>
          <div><strong>{ret(median)}</strong><small>median move</small></div>
          <span className={`sep-pay-badge ${passes ? 'is-pass' : 'is-fail'}`}>{passes ? 'PASS' : 'NO PASS'}</span>
        </summary>
        <div className="sep-pay-history">
          {(s15.returns || []).map((r) => {
            const paid = edge.direction === 'Bullish' ? Number(r.return_pct) > 0 : Number(r.return_pct) < 0
            return <div className={paid ? 'paid' : 'missed'} key={`${edge.window}-${r.year}`}><span>{r.year}</span><strong>{ret(r.return_pct)}</strong><small>{paid ? 'paid' : 'missed'}</small></div>
          })}
        </div>
      </details>
    ))}</div> : <p className="sep-empty">No seasonal windows are available to validate for this instrument.</p>}
  </div>
}

export function SeasonalEdgePanel({ commercialAttention = null, instrumentId = null }) {
  const [doc, setDoc] = React.useState(null)
  const [error, setError] = React.useState(null)
  React.useEffect(() => {
    let cancelled = false
    fetch('/data/seasonal_edge_scan_latest.json')
      .then(async (r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json() })
      .then((body) => { if (!cancelled) setDoc(body) })
      .catch((err) => { if (!cancelled) setError(err?.message || 'scan unavailable') })
    return () => { cancelled = true }
  }, [])

  if (!doc) return <section className="sep-panel"><div className="sep-header"><div><h2>Seasonal Edge Alerts</h2><p>Recurring calendar windows with unusually persistent 5Y / 10Y / 15Y behaviour.</p></div></div><p className="sep-empty">{error ? 'Seasonal edge scan has not been exported yet.' : 'Loading seasonal edge scan…'} Run <code>python scripts/build_seasonal_edge_scan.py</code>.</p></section>

  const alerts = Array.isArray(doc.alerts) ? doc.alerts.slice(0, 8) : []
  const edges = Array.isArray(doc.top_edges) ? doc.top_edges.filter((e) => !alerts.includes(e)).slice(0, 8) : []
  const instrumentResult = instrumentId ? (doc.instrument_results || []).find((r) => r.instrument_id === instrumentId) : null
  const instrumentEdges = Array.isArray(instrumentResult?.edges) ? instrumentResult.edges : []

  return <section className="sep-panel" aria-label="Seasonal edge alerts">
    <div className="sep-header"><div><h2>{instrumentId ? `${instrumentId} — Historical Seasonal Edge` : 'Seasonal Edge Alerts'}</h2><p>{instrumentId ? 'This is the actual historical lookback for the selected market: recurring months and rolling windows ranked by 15Y hit rate, median move and 5Y/10Y/15Y stability.' : 'Finds boring recurring behaviour before it arrives: strong calendar months and robust rolling windows across the seasonality universe.'}</p></div><div className="sep-meta"><strong>{doc.alert_count ?? alerts.length} live alerts</strong><span>{doc.available_instruments ?? '—'}/{doc.instrument_count ?? '—'} markets scanned · {doc.asof || '—'}</span></div></div>
    {instrumentId ? <>
      {instrumentResult?.status === 'ok' && instrumentEdges.length ? <div className="sep-block"><h3>Best current / approaching patterns for {instrumentId}</h3><div className="sep-list">{instrumentEdges.slice(0, 8).map((edge) => <EdgeRow key={`${edge.instrument_id}-${edge.window}`} edge={edge} alert={edge.days_until_start <= 14 && ['STRONG', 'EXCEPTIONAL'].includes(edge.grade)} commercialAttention={commercialAttention} activeInstrument />)}</div></div> : <p className="sep-empty">{instrumentResult?.status === 'unavailable' ? `No historical edge calculation available for ${instrumentId}: ${instrumentResult.error || 'data unavailable'}.` : `No robust seasonal edge passed the filters for ${instrumentId} in the current forward scan.`}</p>}
      <PayValidation edges={instrumentEdges.slice(0, 8)} />
      <details className="sep-more"><summary>Whole-market seasonal alerts</summary><div className="sep-list">{alerts.map((edge) => <EdgeRow key={`${edge.instrument_id}-${edge.window}`} edge={edge} alert commercialAttention={commercialAttention} />)}</div></details>
    </> : <>
      {alerts.length ? <div className="sep-block"><h3>Approaching now</h3><div className="sep-list">{alerts.map((edge) => <EdgeRow key={`${edge.instrument_id}-${edge.window}`} edge={edge} alert commercialAttention={commercialAttention} />)}</div></div> : <p className="sep-empty">No strong seasonal window starts within the next 14 days.</p>}
      {edges.length ? <details className="sep-more"><summary>Next strongest seasonal patterns</summary><div className="sep-list">{edges.map((edge) => <EdgeRow key={`${edge.instrument_id}-${edge.window}`} edge={edge} commercialAttention={commercialAttention} />)}</div></details> : null}
    </>}
    <p className="sep-foot">Seasonal edge is ranked independently from COT. Pay Validation tests the historical seasonal cases only; COT remains separate confirmation and does not contaminate the seasonal pass/fail result.</p>
  </section>
}
