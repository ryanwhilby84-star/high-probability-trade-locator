import React from 'react'
import { scanSeasonalWindows } from './seasonalWindows.js'

export function SeasonalWindowsScanner({ dailyPrices, instrumentId }) {
  const [direction, setDirection] = React.useState('ALL')
  const [minFrequency, setMinFrequency] = React.useState(0.7)
  const windows = React.useMemo(() => scanSeasonalWindows(dailyPrices), [dailyPrices])
  const filtered = windows.filter(w => (direction === 'ALL' || w.direction === direction) &&
    Math.max(w.bullishFrequency, w.bearishFrequency) >= minFrequency).slice(0, 30)
  return <section className="sws-main" aria-label="Seasonal windows scanner">
    <h2>Seasonal Windows · {instrumentId || 'Instrument'}</h2>
    <p className="sws-muted">Historical daily-close outcomes · completed years only · 15-year maximum lookback. Research, not trade signals.</p>
    <div className="sws-controls">
      <label>Direction <select value={direction} onChange={e=>setDirection(e.target.value)}>
        <option value="ALL">Both</option><option value="BULLISH">Bullish</option><option value="BEARISH">Bearish</option>
      </select></label>
      <label>Minimum historical frequency <select value={minFrequency} onChange={e=>setMinFrequency(Number(e.target.value))}>
        <option value={0.6}>60%</option><option value={0.7}>70%</option><option value={0.8}>80%</option><option value={0.9}>90%</option>
      </select></label>
    </div>
    {!Array.isArray(dailyPrices) || !dailyPrices.length ? <p className="sws-muted">Verified daily price history is required. No seasonal results available.</p> :
      !filtered.length ? <p className="sws-muted">No historical windows meet the selected filters and sample threshold.</p> :
      <div style={{overflowX:'auto'}}><table style={{width:'100%',textAlign:'left',borderCollapse:'collapse'}}>
        <thead><tr>{['Window','Days','Bias','Median return','Mean return','Historical frequency','Sample years'].map(x=><th key={x} style={{padding:'0.6rem'}}>{x}</th>)}</tr></thead>
        <tbody>{filtered.map(w=><tr key={w.calendarDay+'-'+w.durationDays}>
          <td>{w.start} → {w.end}</td><td>{w.durationDays}</td><td>{w.direction}</td>
          <td>{w.medianReturnPct.toFixed(2)}%</td><td>{w.meanReturnPct.toFixed(2)}%</td>
          <td>{(100*(w.direction==='BULLISH'?w.bullishFrequency:w.bearishFrequency)).toFixed(0)}%</td>
          <td>{w.sampleYears}</td>
        </tr>)}</tbody>
      </table></div>}
  </section>
}
