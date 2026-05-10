import React from 'react'
import { createRoot } from 'react-dom/client'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import './styles.css'

const TRACKED_MARKETS = [
  'NASDAQ / NQ',
  'S&P 500 / ES',
  'Dow / YM',
  'Gold',
  'Silver',
  'Copper / HG',
  'Crude Oil / CL',
  'Natural Gas / NG',
  'Coffee',
  'Cocoa',
  'Corn',
  'Wheat',
  'Soybeans',
]

const norm = (m = '') => m.toLowerCase()
const canonical = (market = '') => {
  const m = norm(market)
  if (m.includes('nasdaq') || m.includes('/ nq')) return 'NASDAQ / NQ'
  if (m.includes('s&p') || m.includes('sp 500') || m.includes('/ es')) return 'S&P 500 / ES'
  if (m.includes('dow') || m.includes('djia') || m.includes('s30') || m.includes('/ ym')) return 'Dow / YM'
  if (m.includes('gold') || m.includes('/ gc')) return 'Gold'
  if (m.includes('silver') || m.includes('/ si')) return 'Silver'
  if (m.includes('copper') || m.includes('/ hg')) return 'Copper / HG'
  if (m.includes('crude oil') || m.includes('/ cl')) return 'Crude Oil / CL'
  if (m.includes('natural gas') || m.includes('/ ng')) return 'Natural Gas / NG'
  if (m.includes('coffee') || m.includes('/ kc')) return 'Coffee'
  if (m.includes('cocoa') || m.includes('/ cc')) return 'Cocoa'
  if (m.includes('corn') || m.includes('/ zc')) return 'Corn'
  if (m.includes('wheat') || m.includes('/ zw')) return 'Wheat'
  if (m.includes('soybeans') || m.includes('/ zs')) return 'Soybeans'
  return market
}

const rowDate = (r = {}) => r.date || r.latest_report_date || ''
const display = (v) => (v === null || v === undefined || v === '' ? 'N/A' : v)

const sanitizeInvalidNumericLiterals = (text = '') => text.replace(/\b(?:NaN|Infinity|-Infinity|undefined)\b/g, 'null')

const sanitizeObject = (value, stats = { sanitized: false, replacements: 0 }) => {
  if (Array.isArray(value)) return value.map((item) => sanitizeObject(item, stats))
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, sanitizeObject(v, stats)]))
  if (value === undefined || value === null) return null
  if (typeof value === 'number' && !Number.isFinite(value)) {
    stats.sanitized = true
    stats.replacements += 1
    return null
  }
  return value
}

const safeJsonParse = (text = '') => {
  try {
    return { parsed: JSON.parse(text), sanitized: false, replacements: 0 }
  } catch (err) {
    const repaired = sanitizeInvalidNumericLiterals(text)
    const replacements = ((text.match(/\b(?:NaN|Infinity|-Infinity|undefined)\b/g)) || []).length
    const parsed = JSON.parse(repaired)
    return { parsed, sanitized: true, replacements, parseError: err }
  }
}

function App() {
  const [data, setData] = React.useState([])
  const [date, setDate] = React.useState('')
  const [expanded, setExpanded] = React.useState({})

  React.useEffect(() => {
    fetch('/data/confluence_history_latest.json')
      .then((r) => r.text())
      .then((text) => {
        const parsedResult = safeJsonParse(text)
        const stats = { sanitized: parsedResult.sanitized, replacements: parsedResult.replacements }
        const payload = sanitizeObject(parsedResult.parsed, stats)
        const rows = Array.isArray(payload?.records) ? payload.records : (Array.isArray(payload) ? payload : [])
        setData(rows)
        const ds = [...new Set(rows.map(rowDate).filter(Boolean))].sort()
        setDate(ds.at(-1) || '')
      })
      .catch(() => setData([]))
  }, [])

  const dates = React.useMemo(() => [...new Set(data.map(rowDate).filter(Boolean))].sort(), [data])
  const week = React.useMemo(() => data.filter((r) => rowDate(r) === date).map((r) => ({ ...r, market_key: canonical(r.market) })), [data, date])

  const marketRows = React.useMemo(() => {
    const byMarket = new Map()
    week.forEach((row) => {
      if (TRACKED_MARKETS.includes(row.market_key) && !byMarket.has(row.market_key)) byMarket.set(row.market_key, row)
    })
    return TRACKED_MARKETS.map((market) => {
      const row = byMarket.get(market)
      return {
        market,
        latest_report_date: row?.latest_report_date || row?.date || date,
        cot_bias: row?.cot_bias,
        cot_score: row?.cot_score,
        cot_reason: row?.cot_reason || row?.cot_reasoning || row?.cot_context,
        macro_regime: row?.macro_regime || row?.macro_signal,
        macro_score: row?.macro_score,
        final_context: row?.final_context || row?.confluence_bias,
        technical_action_note: row?.summary || row?.technical_note || row?.trade_readiness,
        final_context_reason: row?.final_context_reason,
        raw_cftc_market_name: row?.raw_cftc_market_name,
        trader_group_used: row?.trader_group_used,
        long_value: row?.long_value,
        short_value: row?.short_value,
        net_value: row?.net_value,
        previous_week_net: row?.previous_week_net,
        one_week_net_change: row?.one_week_net_change,
        four_week_net_change: row?.four_week_net_change,
        bias_rule_used: row?.bias_rule_used,
        score_rule_used: row?.score_rule_used,
        final_calculated_cot_bias: row?.final_calculated_cot_bias,
        final_calculated_cot_score: row?.final_calculated_cot_score,
      }
    })
  }, [week, date])

  const tracked = marketRows.filter((r) => r.cot_score !== undefined && r.cot_score !== null)

  return <div className='app'>
    <h1>High Probability Trade Locator</h1>
    <p>COT-primary decision table (macro as filter only)</p>
    <div className='controls'>
      <select value={date} onChange={(e) => setDate(e.target.value)}>{dates.map((d) => <option key={d}>{d}</option>)}</select>
    </div>
    <div className='table-wrap'>
      <table className='decision-table'>
        <thead><tr>
          <th>Market</th><th>Latest report date</th><th>COT bias</th><th>COT score</th><th>COT reason</th><th>Macro regime</th><th>Macro score</th><th>Final context</th><th>Technical action note</th>
        </tr></thead>
        <tbody>
          {marketRows.map((r) => <React.Fragment key={r.market}>
            <tr>
              <td>{r.market}</td><td>{display(r.latest_report_date)}</td><td>{display(r.cot_bias)}</td><td>{display(r.cot_score)}</td><td>{display(r.cot_reason)}</td><td>{display(r.macro_regime)}</td><td>{display(r.macro_score)}</td><td>{display(r.final_context)}</td><td>{display(r.technical_action_note)}</td>
            </tr>
            <tr>
              <td colSpan={9}>
                <button onClick={() => setExpanded((s) => ({ ...s, [r.market]: !s[r.market] }))}>
                  {expanded[r.market] ? 'Hide audit details' : 'Show audit details'}
                </button>
                {expanded[r.market] && <div style={{ marginTop: '8px' }}>
                  <strong>Audit:</strong> raw_market={display(r.raw_cftc_market_name)}; trader_group={display(r.trader_group_used)}; long={display(r.long_value)}; short={display(r.short_value)}; net={display(r.net_value)}; prev_week_net={display(r.previous_week_net)}; 1w_change={display(r.one_week_net_change)}; 4w_change={display(r.four_week_net_change)}; bias_rule={display(r.bias_rule_used)}; score_rule={display(r.score_rule_used)}; final_bias={display(r.final_calculated_cot_bias)}; final_score={display(r.final_calculated_cot_score)}; final_context_reason={display(r.final_context_reason)}
                </div>}
              </td>
            </tr>
          </React.Fragment>)}
        </tbody>
      </table>
    </div>
    <div className='charts'>
      <Chart title='COT Score by Tracked Market'><BarChart data={tracked}><CartesianGrid strokeDasharray='3 3' /><XAxis dataKey='market' /><YAxis domain={[0, 10]} /><Tooltip /><Bar dataKey='cot_score' fill='#f6ad55' /></BarChart></Chart>
    </div>
  </div>
}

const Chart = ({ title, children }) => <div className='panel'><h3>{title}</h3><ResponsiveContainer width='100%' height={280}>{children}</ResponsiveContainer></div>

createRoot(document.getElementById('root')).render(<App />)
