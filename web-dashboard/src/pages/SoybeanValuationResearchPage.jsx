import React from 'react'
import { Line, LineChart, ResponsiveContainer, CartesianGrid, Tooltip, XAxis, YAxis, Legend } from 'recharts'

import { AppShell } from '../components/AppShell.jsx'
import { fetchPublicJson } from '../utils/fetchPublicJson.js'
import { useCanonicalCurrentPrice } from '../prices/canonicalCurrentPrice.js'
import { navigateToCotWorkstation, navigateToInstrument, navigateToScanner } from '../routing.js'
import './naturalGasValuation.css'

const MARKET = 'Soybeans'
const MODEL_URL = '/data/soybean_valuation_research_latest.json'

const num = (v) => Number.isFinite(Number(v)) ? Number(v) : null
const fmt = (v, d = 2) => num(v) == null ? '—' : num(v).toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d })
const signed = (v, d = 1) => num(v) == null ? '—' : `${num(v) > 0 ? '+' : ''}${fmt(v, d)}`

function valuationState(dev) {
  if (dev == null) return { label: 'AWAITING FUNDAMENTALS', tone: 'neutral' }
  if (dev <= -20) return { label: 'MATERIALLY UNDERVALUED', tone: 'bull' }
  if (dev <= -8) return { label: 'UNDERVALUED', tone: 'bull' }
  if (dev >= 20) return { label: 'MATERIALLY OVERVALUED', tone: 'bear' }
  if (dev >= 8) return { label: 'OVERVALUED', tone: 'bear' }
  return { label: 'NEAR FAIR VALUE', tone: 'neutral' }
}

function SummaryCard({ label, value, sub, tone = 'neutral' }) {
  return <article className={`ngv-summary-card ngv-summary-card--${tone}`}><span className="ngv-summary-label">{label}</span><strong className="ngv-summary-value">{value}</strong><span className="ngv-summary-sub">{sub}</span></article>
}

function Scale({ deviation }) {
  const p = deviation == null ? 50 : Math.max(3, Math.min(97, 50 + deviation * 1.5))
  return <section className="ngv-scale"><header className="ngv-section-head"><h2>Valuation Scale</h2><span className="ngv-scale-band">Soybean fundamental value</span></header><div className="ngv-scale-track"><div className="ngv-scale-gradient"/><div className="ngv-scale-marker" style={{ left: `${p}%` }}><span className="ngv-scale-pin"/><span className="ngv-scale-pin-label">Today {deviation == null ? '—' : `${signed(deviation)}%`}</span></div></div><div className="ngv-scale-labels"><span>Strongly<br/>Undervalued</span><span>Moderately<br/>Undervalued</span><span>Fair<br/>Value</span><span>Moderately<br/>Overvalued</span><span>Strongly<br/>Overvalued</span></div></section>
}

function Driver({ title, value, source, copy }) {
  return <article className="ngv-driver-card"><header className="ngv-driver-head"><span className="ngv-driver-icon">●</span><div><h3>{title}</h3><span className="ngv-driver-source">{source}</span></div></header><dl className="ngv-driver-metrics"><div><dt>Current</dt><dd>{value}</dd></div></dl><p className="ngv-driver-copy">{copy}</p></article>
}

function HistoryChart({ history }) {
  const data = (Array.isArray(history) ? history : []).slice(-260).map(r => ({ date: r.date || r.as_of || r.model_week, price: num(r.market_price ?? r.price ?? r.close), fair: num(r.fair_value) })).filter(r => r.price != null || r.fair != null)
  if (!data.length) return <div className="ngv-chart-empty">Historical fair-value series will appear here as the point-in-time soybean model history is generated.</div>
  return <div className="ngv-chart-wrap"><ResponsiveContainer width="100%" height={380}><LineChart data={data} margin={{ top: 12, right: 18, left: 4, bottom: 8 }}><CartesianGrid stroke="rgba(148,163,184,0.12)" strokeDasharray="3 6"/><XAxis dataKey="date" minTickGap={48} tick={{ fill:'#94a3b8', fontSize:11 }}/><YAxis domain={['auto','auto']} width={55} tick={{ fill:'#94a3b8', fontSize:11 }}/><Tooltip/><Legend/><Line type="monotone" dataKey="price" name="Soybean Price" stroke="#38bdf8" strokeWidth={2} dot={false} isAnimationActive={false}/><Line type="monotone" dataKey="fair" name="Fair Value" stroke="#fbbf24" strokeWidth={2} strokeDasharray="6 4" dot={false} isAnimationActive={false}/></LineChart></ResponsiveContainer></div>
}

export function SoybeanValuationResearchPage({ sidebarClass, onSidebarClass }) {
  const [doc, setDoc] = React.useState(null)
  const [error, setError] = React.useState(null)
  const canonical = useCanonicalCurrentPrice(MARKET)
  React.useEffect(() => { let live = true; fetchPublicJson(MODEL_URL).then(x => live && setDoc(x)).catch(e => live && setError(e?.message || String(e))); return () => { live = false } }, [])

  const c = doc?.current || {}
  const market = num(c.market_price) ?? num(canonical.price)
  const fair = num(c.fair_value)
  const deviation = fair && market != null ? (market / fair - 1) * 100 : num(c.deviation_pct) ?? (num(c.deviation) != null ? num(c.deviation) * 100 : null)
  const state = valuationState(deviation)
  const history = doc?.history || []

  return <AppShell active="soybean-valuation" sidebarClass={sidebarClass} onSidebarClass={onSidebarClass}>
    <main className="ngv-page">
      <header className="ngv-header"><div><span className="ngv-kicker">INSTITUTIONAL EDGE · SOYBEAN FUNDAMENTALS</span><h1>Soybean Valuation</h1><p>Stocks-to-use scarcity + board-crush economics, with global competitiveness and biofuel context.</p></div><div className="ngv-header-actions"><button onClick={() => navigateToCotWorkstation(MARKET)}>COT Workstation</button><button onClick={() => navigateToInstrument(MARKET)}>Instrument</button><button onClick={navigateToScanner}>Scanner</button></div></header>
      {error ? <div className="ngv-error">{error}</div> : null}
      <section className="ngv-summary-grid"><SummaryCard label="Market Price" value={fmt(market)} sub={canonical.label || 'canonical soybean price'}/><SummaryCard label="Fundamental Fair Value" value={fmt(fair)} sub="blended soybean model"/><SummaryCard label="Valuation Deviation" value={deviation == null ? '—' : `${signed(deviation,2)}%`} sub={state.label} tone={state.tone}/><SummaryCard label="Model As Of" value={doc?.as_of || c.wasde_as_of || '—'} sub={doc?.status || 'point-in-time research'}/></section>
      <Scale deviation={deviation}/>
      <section className="ngv-section"><header className="ngv-section-head"><div><span className="ngv-kicker">CORE ANCHORS</span><h2>What is soybean worth?</h2></div></header><div className="ngv-driver-grid"><Driver title="Stocks-to-Use Scarcity" value={fmt(c.scarcity_fair_value)} source="USDA / WASDE" copy={`Ending stocks ${fmt(c.ending_stocks,0)} · Total use ${fmt(c.total_use,0)}. Non-linear scarcity anchor for the US balance sheet.`}/><Driver title="Board Crush Value" value={fmt(c.crush_implied_value)} source="Soybean meal + soybean oil" copy={`Meal ${fmt(c.meal_price)} · Oil ${fmt(c.oil_price)}. Processing economics provide the commercial value anchor.`}/></div></section>
      <section className="ngv-section"><header className="ngv-section-head"><div><span className="ngv-kicker">DRIVER AUDIT</span><h2>What is moving fair value?</h2></div></header><div className="ngv-driver-grid"><Driver title="Exports" value={fmt(c.exports,0)} source="USDA" copy="Export demand and global offtake tighten or loosen the US balance sheet."/><Driver title="Crush Demand" value={fmt(c.crush_use,0)} source="USDA" copy="Domestic processing demand links raw beans to meal and oil economics."/><Driver title="BRL / USD Competitiveness" value={fmt(c.fx_adjustment,3)} source="Secondary adjustment" copy="Brazilian competitiveness changes the relative attractiveness of US export supply."/><Driver title="Biofuel / Soy Oil" value={fmt(c.biofuel_adjustment,3)} source="Secondary adjustment" copy="Renewable diesel and biofuel demand can alter soybean-oil economics and crush value."/></div></section>
      <section className="ngv-section"><header className="ngv-section-head"><div><span className="ngv-kicker">VALUATION VS PRICE</span><h2>Historical Research</h2></div><span className="ngv-scale-band">Point-in-time only</span></header><HistoryChart history={history}/></section>
    </main>
  </AppShell>
}

export default SoybeanValuationResearchPage
