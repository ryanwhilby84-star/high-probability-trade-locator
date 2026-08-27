import React from 'react'

import { fetchPublicJson } from '../utils/fetchPublicJson.js'
import { navigateToCotWorkstation, navigateToInstrument, navigateToScanner } from '../routing.js'
import './soybeanValuationResearch.css'

const MARKET = 'Soybeans'
const PRICE_URL = '/data/prices_latest.json'
const MODEL_URL = '/data/soybean_valuation_research_latest.json'

function n(v) {
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}

function fmt(v, digits = 2) {
  const x = n(v)
  return x == null ? '—' : x.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits })
}

function fmtPct(v) {
  const x = n(v)
  return x == null ? '—' : `${x > 0 ? '+' : ''}${fmt(x * 100, 1)}%`
}

function tone(deviation) {
  const d = n(deviation)
  if (d == null) return 'neutral'
  if (d <= -0.1) return 'cheap'
  if (d >= 0.1) return 'rich'
  return 'neutral'
}

function MiniChart({ priceRows, fairRows }) {
  const rows = React.useMemo(() => {
    const fairByDate = new Map((fairRows || []).map((r) => [String(r.date || r.as_of || '').slice(0, 10), n(r.fair_value)]))
    return (priceRows || []).slice(-260).map((r) => ({
      date: String(r.date || '').slice(0, 10),
      price: n(r.close),
      fair: fairByDate.get(String(r.date || '').slice(0, 10)) ?? null,
    })).filter((r) => r.price != null)
  }, [priceRows, fairRows])

  if (rows.length < 2) return <div className="svr-empty">No soybean weekly price history available.</div>

  const vals = rows.flatMap((r) => [r.price, r.fair]).filter((v) => v != null)
  const lo = Math.min(...vals)
  const hi = Math.max(...vals)
  const span = Math.max(hi - lo, 1e-9)
  const x = (i) => 28 + (i / Math.max(rows.length - 1, 1)) * 944
  const y = (v) => 300 - ((v - lo) / span) * 250
  const pricePath = rows.map((r, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)},${y(r.price).toFixed(1)}`).join(' ')
  const fairSegments = []
  let seg = []
  rows.forEach((r, i) => {
    if (r.fair == null) {
      if (seg.length > 1) fairSegments.push(seg)
      seg = []
    } else seg.push([x(i), y(r.fair)])
  })
  if (seg.length > 1) fairSegments.push(seg)

  return (
    <div className="svr-chart-wrap">
      <svg viewBox="0 0 1000 330" className="svr-chart" role="img" aria-label="Soybean market price versus fair value">
        {[0, 1, 2, 3, 4].map((k) => <line key={k} x1="28" x2="972" y1={50 + k * 62.5} y2={50 + k * 62.5} className="svr-grid" />)}
        <path d={pricePath} className="svr-price-line" />
        {fairSegments.map((s, idx) => <path key={idx} d={s.map((p, i) => `${i ? 'L' : 'M'}${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(' ')} className="svr-fair-line" />)}
      </svg>
      <div className="svr-legend"><span><i className="price" />Market price</span><span><i className="fair" />Blended fair value</span></div>
    </div>
  )
}

export function SoybeanValuationResearchPage() {
  const [priceDoc, setPriceDoc] = React.useState(null)
  const [modelDoc, setModelDoc] = React.useState(null)
  const [error, setError] = React.useState(null)

  React.useEffect(() => {
    let cancelled = false
    Promise.all([
      fetchPublicJson(PRICE_URL),
      fetchPublicJson(MODEL_URL).catch(() => null),
    ]).then(([p, m]) => {
      if (cancelled) return
      setPriceDoc(p)
      setModelDoc(m)
      setError(null)
    }).catch((e) => !cancelled && setError(e?.message || String(e)))
    return () => { cancelled = true }
  }, [])

  const record = priceDoc?.instruments?.[MARKET] || priceDoc?.instruments?.['Soybeans / ZS'] || null
  const weekly = Array.isArray(record?.weekly) ? record.weekly : []
  const last = weekly[weekly.length - 1]
  const market = n(modelDoc?.current?.market_price) ?? n(record?.price?.mid) ?? n(last?.close)
  const fair = n(modelDoc?.current?.fair_value)
  const scarcity = n(modelDoc?.current?.scarcity_fair_value)
  const crush = n(modelDoc?.current?.crush_implied_value)
  const dev = fair != null && market != null && fair !== 0 ? market / fair - 1 : n(modelDoc?.current?.deviation)
  const state = tone(dev)
  const history = Array.isArray(modelDoc?.history) ? modelDoc.history : []
  const status = modelDoc?.status || (history.length ? 'research' : 'awaiting_inputs')

  return (
    <main className="svr-root">
      <header className="svr-topbar">
        <div>
          <div className="svr-eyebrow">INSTITUTIONAL EDGE · FUNDAMENTAL RESEARCH</div>
          <h1>Soybean Valuation</h1>
          <p>Dual-anchor research: USDA stocks-to-use scarcity value + board-crush processing value.</p>
        </div>
        <div className="svr-actions">
          <button onClick={() => navigateToCotWorkstation(MARKET)}>COT</button>
          <button onClick={() => navigateToInstrument(MARKET)}>Instrument</button>
          <button onClick={navigateToScanner}>Scanner</button>
        </div>
      </header>

      {error ? <div className="svr-warning">{error}</div> : null}

      <section className="svr-hero-grid">
        <article className="svr-card svr-primary">
          <span>Market</span><strong>{fmt(market)}</strong><small>canonical soybean price</small>
        </article>
        <article className="svr-card">
          <span>Fundamental fair value</span><strong>{fmt(fair)}</strong><small>blended anchor</small>
        </article>
        <article className={`svr-card svr-state ${state}`}>
          <span>Valuation gap</span><strong>{fmtPct(dev)}</strong><small>{dev == null ? 'waiting for model inputs' : state === 'cheap' ? 'market below fair value' : state === 'rich' ? 'market above fair value' : 'near fair value'}</small>
        </article>
        <article className="svr-card">
          <span>Model status</span><strong className="svr-status-text">{String(status).replaceAll('_', ' ')}</strong><small>{modelDoc?.as_of || 'no fundamental snapshot yet'}</small>
        </article>
      </section>

      <section className="svr-panel">
        <div className="svr-panel-head"><div><span className="svr-eyebrow">VALUATION VS PRICE</span><h2>Manual lookback</h2></div><div className="svr-panel-note">Last ~5 years of weekly price; fair-value line appears as point-in-time model history is generated.</div></div>
        <MiniChart priceRows={weekly} fairRows={history} />
      </section>

      <section className="svr-anchor-grid">
        <article className="svr-panel svr-anchor"><span className="svr-eyebrow">ANCHOR 01</span><h3>Scarcity value</h3><strong>{fmt(scarcity)}</strong><p>Nonlinear inverse stocks-to-use relationship. Tight balance sheets should reprice faster than comfortable inventories.</p><dl><div><dt>Stocks / use</dt><dd>{modelDoc?.current?.stocks_to_use == null ? '—' : fmtPct(modelDoc.current.stocks_to_use)}</dd></div><div><dt>USDA vintage</dt><dd>{modelDoc?.current?.wasde_as_of || '—'}</dd></div></dl></article>
        <article className="svr-panel svr-anchor"><span className="svr-eyebrow">ANCHOR 02</span><h3>Crush-implied value</h3><strong>{fmt(crush)}</strong><p>Joint product economics from soybean meal and soybean oil, net of the processing margin allowance.</p><dl><div><dt>Meal</dt><dd>{fmt(modelDoc?.current?.meal_price)}</dd></div><div><dt>Oil</dt><dd>{fmt(modelDoc?.current?.oil_price)}</dd></div></dl></article>
      </section>

      <section className="svr-panel svr-drivers">
        <div className="svr-panel-head"><div><span className="svr-eyebrow">DRIVER AUDIT</span><h2>What is moving fair value?</h2></div></div>
        <div className="svr-driver-grid">
          {[
            ['Ending stocks', modelDoc?.current?.ending_stocks, 'USDA'],
            ['Total use', modelDoc?.current?.total_use, 'USDA'],
            ['Crush', modelDoc?.current?.crush_use, 'USDA'],
            ['Exports', modelDoc?.current?.exports, 'USDA'],
            ['BRL / USD adj.', modelDoc?.current?.fx_adjustment, 'secondary'],
            ['Biofuel adj.', modelDoc?.current?.biofuel_adjustment, 'secondary'],
          ].map(([label, value, source]) => <div key={label}><span>{label}</span><strong>{fmt(value)}</strong><small>{source}</small></div>)}
        </div>
      </section>

      {status === 'awaiting_inputs' ? <div className="svr-warning">Research UI is wired to the canonical soybean price history. Fundamental values remain deliberately blank until real point-in-time USDA + crush inputs are generated; no synthetic fair value is shown.</div> : null}
    </main>
  )
}
