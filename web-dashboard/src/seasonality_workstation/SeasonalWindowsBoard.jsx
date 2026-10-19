import React from 'react'

const pct = (v) => v == null || !Number.isFinite(Number(v)) ? '—' : `${(Number(v) <= 1 ? Number(v) * 100 : Number(v)).toFixed(1)}%`
const dateLabel = (week) => {
  if (!Number.isFinite(Number(week))) return '—'
  const d = new Date(Date.UTC(2026, 0, 4 + (Number(week) - 1) * 7))
  return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', timeZone: 'UTC' })
}
const currentWeek = () => {
  const d = new Date()
  const day = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()))
  const weekday = day.getUTCDay() || 7
  day.setUTCDate(day.getUTCDate() + 4 - weekday)
  const yearStart = new Date(Date.UTC(day.getUTCFullYear(), 0, 1))
  return Math.ceil((((day - yearStart) / 86400000) + 1) / 7)
}
export function SeasonalWindowsBoard({ marketId, windows = [], dailyWindows = [] }) {
  const [selected, setSelected] = React.useState(null)
  const [showAll, setShowAll] = React.useState(false)
  const dailyRows = [...dailyWindows].sort((a,b) => b.hit_rate - a.hit_rate || b.median_return_pct - a.median_return_pct)
  const currentMd = new Date().toISOString().slice(5,10)
  const currentDay = (md) => {
    const [m,d] = md.split('-').map(Number)
    return Math.round((Date.UTC(2000,m-1,d) - Date.UTC(2000,0,1))/86400000)
  }
  const nowDay = currentDay(currentMd === '02-29' ? '02-28' : currentMd)
  const isActive = (w) => {
    const a=currentDay(w.start), b=a+w.calendar_days
    return nowDay>=a && nowDay<=b || nowDay+366>=a && nowDay+366<=b
  }
  const isUpcoming = (w) => !isActive(w) && ((currentDay(w.start)-nowDay+366)%366)<=35
  const visibleDaily = showAll ? dailyRows : dailyRows.filter(w => isActive(w) || isUpcoming(w))

  const now = currentWeek()
  const rows = [...windows].filter(w => w?.window?.start_week && w?.window?.end_week).sort((a,b) => (b.hit_rate || 0) - (a.hit_rate || 0))
  const active = rows.filter(w => w.window.start_week <= now && w.window.end_week >= now)
  const upcoming = rows.filter(w => w.window.start_week > now).sort((a,b) => a.window.start_week - b.window.start_week)
  const highlighted = [...active, ...upcoming].slice(0, 8)
  return <section className="sws-pane" aria-label="Seasonal Windows board">
    <h2>Seasonal Windows · {marketId}</h2>
    <h3>Daily seasonal window opportunities · 15-year research</h3>
    <p className="sws-muted">Every calendar entry day tested against 1–12 week exits. Historical daily closes, minimum 10 usable years and 70% profitable years. In-sample research, not a validated trading signal.</p>
    <div className="sws-controls"><strong>{dailyRows.filter(isActive).length} active</strong><strong>{dailyRows.filter(isUpcoming).length} opening within 35 days</strong><button type="button" onClick={() => setShowAll(v=>!v)}>{showAll ? 'Current / upcoming only' : 'Show all ranked windows'}</button></div>
    <div style={{overflowX:'auto'}}><table style={{width:'100%',textAlign:'left'}}><thead><tr><th>Direction</th><th>Entry → exit</th><th>Historical record</th><th>Median</th><th>Average</th><th>Recent 5Y</th><th>Worst year</th><th>Details</th></tr></thead><tbody>{visibleDaily.map((w,i)=><tr key={`${w.start}-${w.end}-${w.direction}-${i}`}><td>{w.direction}</td><td>{w.start} → {w.end}</td><td>{w.wins}/{w.sample_years} · {pct(w.hit_rate)}</td><td>{w.median_return_pct}%</td><td>{w.average_return_pct}%</td><td>{w.recent_5y_wins}/{w.recent_5y_n}</td><td>{w.worst_return_pct}%</td><td><button type="button" onClick={()=>setSelected(selected===w?null:w)}>Inspect</button></td></tr>)}</tbody></table></div>
    {!visibleDaily.length && <p className="sws-muted">No qualifying active/upcoming daily windows in the current archive. Select “Show all ranked windows” to inspect historical candidates.</p>}
    {selected?.outcomes && <div className="sws-pane"><h3>{selected.direction} {selected.start} → {selected.end}</h3><p>Year-by-year directional returns · historical sample only</p><div style={{display:'flex',flexWrap:'wrap',gap:12}}>{selected.outcomes.map(o=><span key={o.year}>{o.year}: {o.return_pct}%</span>)}</div></div>}
    <p className="sws-muted">Historical ISO-week turning windows. Current archive evidence only; these are not yet the day-by-day entry/exit scans.</p>
    <div className="sws-controls"><strong>{active.length} active</strong><strong>{upcoming.length} upcoming</strong><strong>{rows.length} verified turning windows</strong></div>
    <div style={{ position: 'relative', padding: '18px 0', margin: '12px 0' }}>
      <div style={{ position: 'absolute', left: `${(now - 1) / 52 * 100}%`, top: 0, bottom: 0, width: 2, background: '#f3c36b', zIndex: 2 }} title="Current ISO week" />
      {highlighted.map((w,i) => {
        const a = w.window.start_week, b = w.window.end_week
        return <button type="button" key={`${a}-${b}-${i}`} onClick={() => setSelected(w)} title={`${w.kind} · ${pct(w.hit_rate)}`} style={{ display: 'block', position: 'relative', marginBottom: 8, marginLeft: `${(a - 1) / 52 * 100}%`, width: `${Math.max(2,(b - a + 1) / 52 * 100)}%`, minHeight: 24, borderRadius: 5, border: '1px solid currentColor', background: w.kind === 'BULLISH_TURN' ? '#174f3b' : '#643038', color: '#fff', cursor: 'pointer', fontSize: 11 }}>{pct(w.hit_rate)}</button>
      })}
      <div className="sws-muted">Jan ───────── Apr ───────── Jul ───────── Oct ───── Dec · Gold line: current week</div>
    </div>
    <div style={{ overflowX: 'auto' }}><table style={{ width: '100%', textAlign: 'left' }}><thead><tr><th>Bias</th><th>Dates (approx.)</th><th>Historical record</th><th>Follow-through</th><th>Confidence</th><th>Details</th></tr></thead><tbody>{rows.map((w,i) => <tr key={i}><td>{w.kind === 'BULLISH_TURN' ? 'BUY' : 'SELL'}</td><td>{dateLabel(w.window.start_week)} – {dateLabel(w.window.end_week)}</td><td>{pct(w.hit_rate)} · {w.sample_years || '—'} years</td><td>{w.average_follow_return_pct == null ? '—' : `${Number(w.average_follow_return_pct).toFixed(2)}%`}</td><td>{w.confidence || '—'}</td><td><button type="button" onClick={() => setSelected(selected === w ? null : w)}>Inspect</button></td></tr>)}</tbody></table></div>
    {selected ? <div className="sws-pane"><h3>{selected.kind === 'BULLISH_TURN' ? 'BUY' : 'SELL'} · {dateLabel(selected.window.start_week)} – {dateLabel(selected.window.end_week)}</h3><p>Hit rate {pct(selected.hit_rate)} · {selected.sample_years} observed years · average {selected.follow_horizon_weeks}-week follow-through {selected.average_follow_return_pct}% · confidence {selected.confidence}</p><p className="sws-muted">Dates approximate from ISO weeks. Daily entry/exit returns and per-year drawdowns are not present in this turning-window record; no fabricated statistics are displayed.</p></div> : null}
    {!rows.length ? <p className="sws-muted">No validated seasonal turning windows available for this instrument.</p> : null}
  </section>
}
