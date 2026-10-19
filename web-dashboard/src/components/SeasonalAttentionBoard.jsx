import React from 'react'
import { navigateToSeasonalityWorkstation } from '../routing.js'
import './seasonalAttentionBoard.css'

const percent = n => Number.isFinite(Number(n)) ? `${Number(n).toFixed(1)}%` : '—'
const formatDate = s => typeof s === 'string' ? s.slice(5) : '—'
const valid = w => w && ['BULLISH','BEARISH'].includes(w.direction) &&
  Number.isFinite(Number(w.sampleYears)) && Number(w.sampleYears) >= 10 &&
  Number.isFinite(Number(w.medianReturnPct)) &&
  Number.isFinite(Number(w.bullishFrequency)) && Number.isFinite(Number(w.bearishFrequency))

export function SeasonalAttentionBoard({ markets = [] }) {
  const [results, setResults] = React.useState([])
  const [status, setStatus] = React.useState('loading')
  React.useEffect(() => {
    const controller = new AbortController()
    const ids = [...new Set(markets.map(m => typeof m === 'string' ? m : m?.market).filter(Boolean))].slice(0, 30)
    if (!ids.length) { setStatus('empty'); return () => controller.abort() }
    setStatus('loading')
    Promise.all(ids.map(async id => {
      try {
        const response = await fetch(`/api/seasonality-workstation/${encodeURIComponent(id)}?lookback=15Y`, { signal: controller.signal })
        if (!response.ok) return []
        const payload = await response.json()
        if (payload?.status !== 'ok') return []
        // Only use audited window outputs from the backend; never derive returns from forecast curves.
        const windows = payload?.seasonal_windows?.windows
        if (!Array.isArray(windows)) return []
        return windows.filter(valid).map(w => ({ ...w, market: id }))
      } catch { return [] }
    })).then(groups => {
      if (controller.signal.aborted) return
      const ranked = groups.flat().filter(w => Math.max(w.bullishFrequency, w.bearishFrequency) >= .7)
        .sort((a,b) => Math.max(b.bullishFrequency,b.bearishFrequency) - Math.max(a.bullishFrequency,a.bearishFrequency))
      setResults(ranked.slice(0, 8))
      setStatus(ranked.length ? 'ready' : 'empty')
    })
    return () => controller.abort()
  }, [markets])
  return <section className="sab" aria-label="Strong seasonal windows">
    <header className="sab-head"><div><h2>Strong Seasonal Windows</h2>
      <p>15-year historical research · minimum 10 verified years · 70% directional frequency</p></div>
      <span className="sab-count">{status === 'ready' ? `${results.length} windows` : 'Research board'}</span>
    </header>
    {status === 'loading' ? <p className="sab-empty">Checking verified seasonal windows…</p> :
      !results.length ? <p className="sab-empty">No verified seasonal windows available from the current API. Results will appear when audited daily-window data is published.</p> :
      <div className="sab-grid">{results.map((w,i) => {
        const bull = w.direction === 'BULLISH'
        const rate = 100 * (bull ? w.bullishFrequency : w.bearishFrequency)
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
