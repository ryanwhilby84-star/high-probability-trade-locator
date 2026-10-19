import React from 'react'
import { navigateToSeasonalityWorkstation } from '../routing.js'
import './seasonalAttentionBoard.css'

const percent = n => Number.isFinite(Number(n)) ? `${Number(n).toFixed(1)}%` : '—'
const formatDate = s => typeof s === 'string' ? s : '—'
const valid = w => w && ['BULLISH','BEARISH'].includes(w.direction) &&
  Number(w.sampleYears) >= 10 && Number.isFinite(w.medianReturnPct) &&
  Number.isFinite(w.frequency) && w.frequency >= 0.7
const fromArchive = archive => Object.entries(archive?.markets || {}).flatMap(([market, entry]) =>
  (entry?.status === 'ok' && entry?.integrity?.status !== 'FAIL' ? entry.daily_seasonal_windows || [] : [])
    .filter(w => w && ['BUY', 'SELL'].includes(w.direction) && Number(w.sample_years) >= 10)
    .map(w => ({
      market, direction: w.direction === 'BUY' ? 'BULLISH' : 'BEARISH',
      start: w.start, end: w.end, sampleYears: Number(w.sample_years),
      medianReturnPct: Number(w.median_return_pct), frequency: Number(w.hit_rate) > 1 ? Number(w.hit_rate) / 100 : Number(w.hit_rate)
    })).filter(valid))

export function SeasonalAttentionBoard({ markets = [] }) {
  const [results, setResults] = React.useState([])
  const [status, setStatus] = React.useState('loading')
  React.useEffect(() => {
    const controller = new AbortController()
    const ids = [...new Set(markets.map(m => typeof m === 'string' ? m : m?.market).filter(Boolean))].slice(0, 30)
    if (!ids.length) { setStatus('empty'); return () => controller.abort() }
    setStatus('loading')
    fetch('/data/seasonality_workstation_latest.json', { signal: controller.signal, cache: 'no-store' })
      .then(response => response.ok ? response.json() : null)
      .then(payload => {
        if (controller.signal.aborted) return
        const allowed = new Set(ids)
        const ranked = fromArchive(payload)
          .filter(w => allowed.has(w.market))
          .sort((a,b) => b.frequency - a.frequency || b.medianReturnPct - a.medianReturnPct)
        setResults(ranked.slice(0,8))
        setStatus(ranked.length ? 'ready' : 'empty')
      })
      .catch(() => { if (!controller.signal.aborted) setStatus('empty') })
    return () => controller.abort()
  }, [markets])
  return <section className="sab" aria-label="Strong seasonal windows">
    <header className="sab-head"><div><h2>Strong Seasonal Windows</h2>
      <p>15-year historical research · minimum 10 verified years · 70% directional frequency</p></div>
      <span className="sab-count">{status === 'ready' ? `${results.length} windows` : 'Research board'}</span>
    </header>
    {status === 'loading' ? <p className="sab-empty">Checking verified seasonal windows…</p> :
      !results.length ? <p className="sab-empty">No qualifying verified daily windows in the current published seasonality archive.</p> :
      <div className="sab-grid">{results.map((w,i) => {
        const bull = w.direction === 'BULLISH'
        const rate = 100 * w.frequency
        return <button type="button" key={w.market+'-'+w.start+'-'+i} className={`sab-card ${bull?'sab-bull':'sab-bear'}`} onClick={() => navigateToSeasonalityWorkstation(w.market)}>
          <span className="sab-top"><strong>{w.market}</strong><span>{bull?'↗ BUY':'↘ SELL'}</span></span>
          <span className="sab-dates">{formatDate(w.start)} → {formatDate(w.end)}</span>
          <span className="sab-metrics"><span><small>Historical frequency</small><strong>{percent(rate)}</strong></span>
            <span><small>Median return</small><strong>{percent(w.medianReturnPct)}</strong></span>
            <span><small>Sample</small><strong>{w.sampleYears} years</strong></span></span>
          <span className="sab-track"><span style={{width:`${Math.max(0,Math.min(100,rate))}%`}} /></span>
          <span className="sab-link">Open seasonal research →</span>
        </button>
      })}</div>}
  </section>
}
