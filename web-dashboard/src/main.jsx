import React from 'react'
import { createRoot } from 'react-dom/client'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, LineChart, Line, Legend } from 'recharts'
import './styles.css'

const EXPECTED = ['NASDAQ / NQ','S&P 500 / ES','Dow / YM / DJIA / S30','Gold / GC','Silver / SI','Copper / HG','Crude Oil / CL','Natural Gas / NG','Corn / ZC','Soybeans / ZS','Wheat / ZW','Coffee / KC','Cocoa / CC']
const norm = (m='') => m.toLowerCase()
const canonical = (market='') => {
  const m = norm(market)
  if (m.includes('nasdaq')) return 'NASDAQ / NQ'
  if (m.includes('s&p') || m.includes('sp 500')) return 'S&P 500 / ES'
  if (m.includes('dow') || m.includes('s30') || m.includes('djia')) return 'Dow / YM / DJIA / S30'
  const map = {'gold':'Gold / GC','silver':'Silver / SI','copper':'Copper / HG','crude oil':'Crude Oil / CL','natural gas':'Natural Gas / NG','corn':'Corn / ZC','soybeans':'Soybeans / ZS','wheat':'Wheat / ZW','coffee':'Coffee / KC','cocoa':'Cocoa / CC'}
  for (const x of Object.keys(map)) if (m.includes(x)) return map[x]
  return market
}
const rowDate = (r={}) => r.date || ''
const hasValue = (v) => !(v === null || v === undefined || v === '')
const display = (v) => (hasValue(v) ? v : 'N/A')

const explainRow = (row={}) => {
  const finalBias = display(row.confluence_bias)
  const readinessLabel = display(row.trade_readiness)
  const cotBias = display(row.cot_bias)
  const macroRegime = display(row.macro_signal)

  if (finalBias === 'N/A' || readinessLabel === 'N/A') return 'Insufficient data this week to classify overall context.'
  return `COT is ${cotBias} and macro regime is ${macroRegime}, resulting in ${finalBias} context with a ${readinessLabel} readiness label.`
}

const sanitizeInvalidNumericLiterals = (text='') => text.replace(/\b(?:NaN|Infinity|-Infinity|undefined)\b/g, 'null')

const sanitizeObject = (value, stats = { sanitized: false, replacements: 0 }) => {
  if (Array.isArray(value)) return value.map((item) => sanitizeObject(item, stats))
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, sanitizeObject(v, stats)]))
  }
  if (value === undefined || value === null) return null
  if (typeof value === 'number' && !Number.isFinite(value)) {
    stats.sanitized = true
    stats.replacements += 1
    return null
  }
  return value
}

const safeJsonParse = (text='') => {
  try {
    return { parsed: JSON.parse(text), sanitized: false, replacements: 0 }
  } catch (err) {
    const repaired = sanitizeInvalidNumericLiterals(text)
    const replacements = ((text.match(/\b(?:NaN|Infinity|-Infinity|undefined)\b/g)) || []).length
    const parsed = JSON.parse(repaired)
    return { parsed, sanitized: true, replacements, parseError: err }
  }
}

function App(){
  const [data,setData]=React.useState([]); const [date,setDate]=React.useState(''); const [market,setMarket]=React.useState('')
  const [search,setSearch]=React.useState(''); const [bias,setBias]=React.useState('All'); const [readiness,setReadiness]=React.useState('All')

  React.useEffect(()=>{
    fetch('/data/confluence_history_latest.json')
      .then(r=>r.text())
      .then((text)=>{
        const parsedResult = safeJsonParse(text)
        const stats = { sanitized: parsedResult.sanitized, replacements: parsedResult.replacements }
        const payload = sanitizeObject(parsedResult.parsed, stats)
        const rows = Array.isArray(payload?.records) ? payload.records : (Array.isArray(payload) ? payload : [])
        setData(rows)
        const ds=[...new Set(rows.map(rowDate).filter(Boolean))].sort()
        setDate(ds.at(-1)||'')
        console.info('[dashboard] dashboard data loaded', {
          rowCount: rows.length,
          sanitized: stats.sanitized,
          replacements: stats.replacements,
        })
      })
      .catch((err)=>{
        console.error('[dashboard] failed to load dashboard data', err)
        setData([])
      })
  },[])

  const dates=React.useMemo(()=>[...new Set(data.map(rowDate).filter(Boolean))].sort(),[data])
  const latestDate = dates.at(-1) || ''
  const week=React.useMemo(()=>data.filter(r=>rowDate(r)===date).map(r=>({...r,market_key:canonical(r.market)})),[data,date])
  React.useEffect(()=>{if(!market && week[0]) setMarket(week[0].market)},[week,market])

  const filtered=week.filter(r=>(!search||String(r.market||'').toLowerCase().includes(search.toLowerCase())) && (bias==='All'||(r.confluence_bias||'').includes(bias.replace(' Bias',''))) && (readiness==='All'||r.trade_readiness===readiness))
  const missing=EXPECTED.filter(e=>!week.some(w=>canonical(w.market)===e))
  const available=EXPECTED.filter(e=>week.some(w=>canonical(w.market)===e))
  const counts=(k)=>Object.entries(filtered.reduce((a,r)=>{
    const key = hasValue(r[k]) ? r[k] : 'N/A'
    a[key]=(a[key]||0)+1
    return a
  },{})).map(([name,value])=>({name,value}))
  const series=data.filter(r=>r.market===market).sort((a,b)=>rowDate(a).localeCompare(rowDate(b)))

  return <div className='app'><h1>High Probability Trade Locator</h1><p>COT + Macro Historical Context Engine</p>
  <div className='controls'><select value={date} onChange={e=>setDate(e.target.value)}>{dates.map(d=><option key={d}>{d}</option>)}</select><input placeholder='Search market' value={search} onChange={e=>setSearch(e.target.value)}/><select value={bias} onChange={e=>setBias(e.target.value)}>{['All','Long Bias','Short Bias','Headwind','Conflicted'].map(v=><option key={v}>{v}</option>)}</select><select value={readiness} onChange={e=>setReadiness(e.target.value)}>{['All','High conviction','Actionable','Cautious','Stand down'].map(v=><option key={v}>{v}</option>)}</select></div>

  <div className='panel status-panel'>
    <h3>Weekly Status</h3>
    <div className='status-grid'>
      <p><strong>Selected report date:</strong> {display(date)}</p>
      <p><strong>Latest available report date:</strong> {display(latestDate)}</p>
      <p><strong>Loaded rows:</strong> {data.length}</p>
      <p><strong>Markets expected this week:</strong> {EXPECTED.length}</p>
      <p><strong>Markets available this week:</strong> {available.length}</p>
      <p><strong>Markets missing this week:</strong> {missing.length}</p>
    </div>
    {missing.length>0 && <p className='missing-list'><strong>Missing markets:</strong> {missing.join(', ')}</p>}
  </div>

  <div className='warn'>This dashboard is context only. It does not generate trade entries or buy/sell signals.</div>

  <div className='panel glossary'>
    <h3>Glossary</h3>
    <ul>
      <li><strong>COT score</strong> = positioning strength/context from trader positioning</li>
      <li><strong>Macro score</strong> = interest-rate/risk-regime modifier</li>
      <li><strong>Final confluence score</strong> = combined context score</li>
      <li><strong>Readiness label</strong> = dashboard-level interpretation, not a buy/sell signal</li>
    </ul>
  </div>

  <div className='grid'>{filtered.map(r=><div key={r.market} className={`card ${String(r.trade_readiness).toLowerCase().replace(' ','-')}`} onClick={()=>setMarket(r.market)}><h3>{display(r.market)}</h3><p><strong>Final bias:</strong> {display(r.confluence_bias)}</p><p><strong>Final confluence score:</strong> {display(r.confluence_score)}</p><p><strong>Readiness label:</strong> {display(r.trade_readiness)}</p><p><strong>COT bias:</strong> {display(r.cot_bias)}</p><p><strong>COT score:</strong> {display(r.cot_score)}</p><p><strong>Macro regime:</strong> {display(r.macro_signal)}</p><p><strong>Macro score:</strong> {display(r.macro_score)}</p><p className='calc-line'>COT {display(r.cot_bias)} ({display(r.cot_score)}) + Macro {display(r.macro_signal)} ({display(r.macro_score)}) = Final {display(r.confluence_bias)} ({display(r.confluence_score)})</p><small>{explainRow(r)}</small></div>)}</div>
  <div className='charts'>
    <Chart title='Final Confluence Score by Market'><BarChart data={filtered}><CartesianGrid strokeDasharray='3 3'/><XAxis dataKey='market'/><YAxis/><Tooltip/><Bar dataKey='confluence_score' fill='#4fd1c5'/></BarChart></Chart>
    <Chart title='Readiness Label Count'><BarChart data={counts('trade_readiness')}><CartesianGrid strokeDasharray='3 3'/><XAxis dataKey='name'/><YAxis allowDecimals={false}/><Tooltip/><Bar dataKey='value' fill='#f6ad55'/></BarChart></Chart>
    <Chart title='Final Bias Count'><BarChart data={counts('confluence_bias')}><CartesianGrid strokeDasharray='3 3'/><XAxis dataKey='name'/><YAxis allowDecimals={false}/><Tooltip/><Bar dataKey='value' fill='#90cdf4'/></BarChart></Chart>
  </div>
  <div className='timeline'><h2>Timeline: {market || 'Select market'}</h2><Chart title='Weekly Story'><LineChart data={series}><CartesianGrid strokeDasharray='3 3'/><XAxis dataKey='date'/><YAxis domain={[0,10]}/><Tooltip/><Legend/><Line dataKey='confluence_score' stroke='#4fd1c5'/><Line dataKey='cot_score' stroke='#f6ad55'/><Line dataKey='macro_score' stroke='#90cdf4'/></LineChart></Chart></div>
  </div>
}
const Chart=({title,children})=><div className='panel'><h3>{title}</h3><ResponsiveContainer width='100%' height={280}>{children}</ResponsiveContainer></div>
createRoot(document.getElementById('root')).render(<App/>)
