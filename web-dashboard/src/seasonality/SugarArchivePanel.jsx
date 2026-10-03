import React from 'react'
import { evaluateSugarWindow, findSugarWindows, prepareSugarArchive } from './sugarArchiveWindows.js'
import './sugarArchive.css'

const pct = (v) => v == null ? '—' : `${v > 0 ? '+' : ''}${v.toFixed(2)}%`
const localMonthDay = () => { const d = new Date(); return `${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}` }
const plusDays = (md, days) => new Date(Date.parse(`2001-${md}T00:00:00Z`) + days * 86400000).toISOString().slice(5, 10)

export function SugarArchivePanel({ marketId }) {
  const isSugar = marketId === 'Sugar'
  const [archive, setArchive] = React.useState(null)
  const [error, setError] = React.useState('')
  const [start, setStart] = React.useState(localMonthDay)
  const [end, setEnd] = React.useState(() => plusDays(localMonthDay(), 28))
  const [strict, setStrict] = React.useState(true)
  const [direction, setDirection] = React.useState('long')
  React.useEffect(() => {
    if (!isSugar) return undefined
    const controller = new AbortController()
    setError(''); setArchive(null)
    fetch('/data/sugar_archive_daily.json', { signal: controller.signal })
      .then((r) => { if (!r.ok) throw new Error(`Archive request failed (${r.status}).`); return r.json() })
      .then((doc) => { if (!controller.signal.aborted) setArchive(prepareSugarArchive(doc)) })
      .catch((err) => { if (err.name !== 'AbortError') setError(err.message) })
    return () => controller.abort()
  }, [isSugar])
  const options = React.useMemo(() => ({ strict, direction }), [strict, direction])
  const result = React.useMemo(() => archive ? evaluateSugarWindow(archive, start, end, options) : null, [archive, start, end, options])
  const candidates = React.useMemo(() => archive ? findSugarWindows(archive, start, options).slice(0, 12) : [], [archive, start, options])
  if (!isSugar) return null
  return (
    <section className="sugar-archive" aria-label="Sugar archive seasonal window finder">
      <h2>Sugar seasonal window finder</h2>
      <p className="sugar-archive-note">Historical archive · 1961–2001 full calendar years · source ends September 2002 · contract-switch gaps excluded.</p>
      <p className="sugar-archive-note">This is an older research sample. Keep it alongside your recent 15-year study. Larger samples do not automatically mean a stronger current edge.</p>
      {error ? <p role="alert">{error}</p> : !archive ? <p role="status">Loading sugar archive…</p> : <>
        <div className="sugar-archive-controls">
          <label>Entry date<input type="date" min="2001-01-01" max="2001-12-31" value={`2001-${start}`} onChange={(e) => { if (e.target.value) setStart(e.target.value.slice(5)) }} /></label>
          <label>Exit date<input type="date" min="2001-01-01" max="2001-12-31" value={`2001-${end}`} onChange={(e) => { if (e.target.value) setEnd(e.target.value.slice(5)) }} /></label>
          <label>Direction<select value={direction} onChange={(e) => setDirection(e.target.value)}><option value="long">Long sugar</option><option value="short">Short sugar</option></select></label>
          <label>Observations<select value={strict ? 'strict' : 'all'} onChange={(e) => setStrict(e.target.value === 'strict')}><option value="strict">Quality-filtered windows</option><option value="all">All source observations</option></select></label>
        </div>
        <p className="sugar-archive-note">Dates repeat each year; the displayed year 2001 is only a date-picker reference. Exit dates before entry wrap into the next year. Boundaries use the first session on or after each date.</p>
        {result?.error ? <p role="alert">{result.error}</p> : result ? <>
          <div className="sugar-archive-stats">
            <div><span>Window</span><strong>{start} → {end}</strong></div>
            <div><span>Profitable years</span><strong>{result.hits}/{result.n} · {result.hitRate == null ? '—' : `${result.hitRate.toFixed(1)}%`}</strong></div>
            <div><span>Median return</span><strong>{pct(result.median)}</strong></div>
            <div><span>Average return</span><strong>{pct(result.mean)}</strong></div>
            <div><span>Worst year</span><strong>{pct(result.worst)}</strong></div>
            <div><span>Excluded years</span><strong>{result.exclusions.length}</strong></div>
          </div>
          <p className="sugar-archive-note">All observations: {result.all.hits}/{result.all.n} profitable, median {pct(result.all.median)}. Quality-filtered: {result.filtered.hits}/{result.filtered.n}, median {pct(result.filtered.median)}. Filter rejects entire year-windows with zero volume at either endpoint of a daily interval or daily moves above 10%; it can exclude genuine moves.</p>
          <details><summary>Year-by-year outcomes and exclusions</summary><div className="sugar-archive-table-wrap"><table><thead><tr><th>Year</th><th>Entry</th><th>Exit</th><th>Directional return</th><th>Flagged intervals</th></tr></thead><tbody>{result.outcomes.map((r) => <tr key={r.year}><td>{r.year}</td><td>{r.entry}</td><td>{r.exit}</td><td>{pct(r.returnPct * (direction === 'short' ? -1 : 1))}</td><td>{r.flagged}</td></tr>)}</tbody></table></div><p>{result.exclusions.map((r) => `${r.year}: ${r.reason.replaceAll('_', ' ')}`).join('; ') || 'No excluded years.'}</p></details>
        </> : null}
        <h3>Window search from {start}</h3>
        <p className="sugar-archive-note">Entry dates every 7 days over the next 8 weeks; durations 7–84 calendar days. Ranked by profitable-year frequency, then median return; minimum 15 usable years. These overlapping windows were selected from the same history and are not independently validated signals.</p>
        <div className="sugar-archive-table-wrap"><table><thead><tr><th>Entry → exit</th><th>Days</th><th>Profitable years</th><th>Rate</th><th>Median</th><th>Average</th><th>Inspect</th></tr></thead><tbody>{candidates.map((r) => <tr key={`${r.start}-${r.end}`}><td>{r.start} → {r.end}</td><td>{r.calendarDays}</td><td>{r.hits}/{r.n}</td><td>{r.hitRate.toFixed(1)}%</td><td>{pct(r.median)}</td><td>{pct(r.mean)}</td><td><button type="button" onClick={() => { setStart(r.start); setEnd(r.end) }} aria-label={`Inspect ${r.start} to ${r.end}`}>View</button></td></tr>)}</tbody></table>{!candidates.length ? <p>No windows meet the minimum sample.</p> : null}</div>
      </>}
    </section>
  )
}
