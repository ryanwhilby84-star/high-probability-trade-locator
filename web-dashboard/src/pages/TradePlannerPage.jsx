import React from 'react'
import { AppShell } from '../components/AppShell.jsx'
import { allInstrumentIds } from '../instrumentRegistry.js'
import { getPositioningPlans, updatePositioningPlan, deletePositioningPlan, positioningTimeline } from '../data/positioningPlans.js'
import { loadWeeklyInspector } from '../data/weeklyInspectorStore.js'
import { PlannerScreenshots } from '../components/PlannerScreenshots.jsx'
import { PositioningTile } from '../components/CommercialAttentionPanel.jsx'
import '../components/positioningPlanner.css'

const STORAGE = 'institutional-edge.trade-planner.v1'
const initial = { market: 'Soybeans', side: 'short', entry: '', stop: '', target: '', account: '10000', riskPct: '0.5', notes: '' }
function readPlans() {
  try { const data = JSON.parse(localStorage.getItem(STORAGE) || '[]'); return Array.isArray(data) ? data : [] } catch { return [] }
}
const money = n => Number.isFinite(n) ? n.toLocaleString('en-GB', { maximumFractionDigits: 2 }) : '—'

export function TradePlannerPage({ sidebarClass, onSidebarClass }) {
  const [positionPlans, setPositionPlans] = React.useState(getPositioningPlans)
  const [inspector, setInspector] = React.useState(null)
  const [expanded, setExpanded] = React.useState(null)
  React.useEffect(() => {
    const refresh = () => setPositionPlans(getPositioningPlans())
    window.addEventListener('positioning-plans-changed', refresh)
    loadWeeklyInspector().then(setInspector).catch(() => {})
    return () => window.removeEventListener('positioning-plans-changed', refresh)
  }, [])
  const [form, setForm] = React.useState(initial)
  const [plans, setPlans] = React.useState(readPlans)
  const update = (key, value) => setForm(f => ({ ...f, [key]: value }))
  const entry = Number(form.entry), stop = Number(form.stop), target = Number(form.target)
  const account = Number(form.account), riskPct = Number(form.riskPct)
  const valuesPresent = [form.entry, form.stop, form.target, form.account, form.riskPct].every(v => String(v).trim() !== '')
  const validDirection = form.side === 'long' ? stop < entry && target > entry : stop > entry && target < entry
  const valid = valuesPresent && [entry, stop, target, account, riskPct].every(Number.isFinite) && entry > 0 && stop > 0 && target > 0 && account > 0 && riskPct > 0 && riskPct <= 5 && validDirection
  const risk = account * riskPct / 100
  const stopDistance = Math.abs(entry - stop)
  const rewardDistance = Math.abs(target - entry)
  const rr = valid ? rewardDistance / stopDistance : null
  const save = () => {
    if (!valid) return
    const item = { ...form, id: String(Date.now()), created: new Date().toISOString(), rr, risk }
    const next = [item, ...plans]
    try { localStorage.setItem(STORAGE, JSON.stringify(next)); setPlans(next) } catch { /* storage unavailable */ }
  }
  const remove = id => {
    const next = plans.filter(p => p.id !== id)
    try { localStorage.setItem(STORAGE, JSON.stringify(next)); setPlans(next) } catch { /* storage unavailable */ }
  }
  const field = (label, key, type = 'number') => <label style={{ display: 'grid', gap: 5, color: '#cbd6df', fontSize: 12 }}>
    {label}<input className="ws-input" type={type} step={type === 'number' ? 'any' : undefined} value={form[key]} onChange={e => update(key, e.target.value)} />
  </label>
  return <AppShell title="Positioning Planner" subtitle="Immutable institutional snapshots · research · weekly rotations" sidebarClass={sidebarClass} onSidebarClass={onSidebarClass}>
    <section style={{ maxWidth: 1050, display: 'grid', gap: 18 }}>
      <div style={{ border: '1px solid #385568', borderRadius: 18, padding: 20, background: 'linear-gradient(140deg,#152839,#141c2a 65%,#102b2c)' }}>
        <div style={{ color: '#a8c5df', fontSize: 11, letterSpacing: 2, fontWeight: 800 }}>RESEARCH WORKSPACE</div>
        <h2 style={{ color: '#f4f7fa', marginBottom: 6 }}>Positioning Tracker <span style={{ fontSize: 14, color: '#8ecfc2' }}>· {positionPlans.length} saved</span></h2>
        <p style={{ color: '#b7c9d8', fontSize: 13 }}>Capture a Radar tile, preserve its original COT evidence, then track institutional positioning each week. Notes and research are editable; the original snapshot is not.</p>
        {!positionPlans.length ? <p style={{ color: '#a6bdce' }}>No positioning snapshots yet. Open Market Radar and select ＋ Add to Plan on any tile.</p> : positionPlans.map(p => {
          const marketKey = Object.keys(inspector?.markets || {}).find(k => k.toLowerCase() === p.market.toLowerCase())
          const timeline = positioningTimeline(inspector?.markets?.[marketKey], p)
          const last = timeline.at(-1)
          const tile = p.snapshot?.tile || {}
          const isOpen = expanded === p.id
          return <article key={p.id} className="ie-plan-arrive" style={{ border: '1px solid #39536b', borderRadius: 14, padding: 16, background: '#192638', marginTop: 12, boxShadow: isOpen ? '0 12px 38px #07131e88' : 'none', transition: 'box-shadow .25s ease' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap', alignItems: 'center' }}>
              <div><strong style={{ color: '#f5f8fd', fontSize: 18 }}>{p.market}</strong><div style={{ color: '#a9bed0', fontSize: 12 }}>COT {p.reportDate} · {p.lookback.toUpperCase()} · saved {new Date(p.createdAt).toLocaleString('en-GB')}</div></div>
              <div style={{ display: 'flex', gap: 8 }}><select className="ws-select" aria-label={'Plan status for ' + p.market} value={p.status} onChange={e => { updatePositioningPlan(p.id, { status: e.target.value }); setPositionPlans(getPositioningPlans()) }}>{['Watching','Waiting for Entry','Active','Archived'].map(v => <option key={v}>{v}</option>)}</select><button type="button" className="ws-btn ws-btn-primary" aria-expanded={isOpen} onClick={() => setExpanded(isOpen ? null : p.id)}>{isOpen ? 'Close' : 'Open plan →'}</button></div>
            </div>
            <div style={{ marginTop: 14, maxWidth: 440 }}><PositioningTile tile={tile} rotation={p.snapshot?.rotation || null} lookback={p.lookback} reportDate={p.reportDate} /></div>
            {isOpen ? <div style={{ marginTop: 18, display: 'grid', gap: 14 }}>
              <div style={{ padding: 14, border: '1px solid #31536a', borderRadius: 12, background: '#102332' }}><strong style={{ color: '#d7e9f6' }}>Weekly positioning behaviour</strong><div style={{ color: '#9fb8c9', fontSize: 12, marginTop: 5 }}>Original snapshot stays fixed; weekly readings are sourced from the validated Inspector dataset.</div>
                {timeline.length ? <div style={{ display: 'grid', gap: 8, marginTop: 12 }}>{timeline.map(t => <div key={t.date} style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', gap: 8, paddingBottom: 8, borderBottom: '1px solid #2a4152', color: '#d8e7f2', fontSize: 12 }}><span>{t.date} · {t.event}</span><span>NC net {t.net?.toLocaleString() ?? '—'} · Δ {t.change?.toLocaleString() ?? '—'} · C {t.c?.toFixed?.(0) ?? '—'} / NC {t.nc?.toFixed?.(0) ?? '—'} / NR {t.nr?.toFixed?.(0) ?? '—'}</span></div>)}</div> : <p style={{ color: '#d2b784', fontSize: 12 }}>Waiting for matching Inspector history. The original snapshot remains saved.</p>}
                {last ? <p style={{ color: '#9edbc7', fontSize: 12 }}>Latest tracked COT: {last.date} · {last.event}</p> : null}
              </div>
              <PlannerScreenshots planId={p.id} />
              <label style={{ color: '#d7e5f0', fontSize: 13 }}>Strategic thesis / supply-demand zones / entry conditions<textarea className="ws-input" rows={5} value={p.thesis || ''} onChange={e => { updatePositioningPlan(p.id, { thesis: e.target.value }); setPositionPlans(getPositioningPlans()) }} style={{ display: 'block', width: '100%', marginTop: 8 }} /></label>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12 }}><span style={{ color: '#9db5c5', fontSize: 11 }}>Stored in this browser. Export/backup and cloud sync are needed before relying on this across devices.</span><button className="ws-btn" type="button" onClick={() => { if (window.confirm('Remove this saved positioning plan?')) { deletePositioningPlan(p.id); setPositionPlans(getPositioningPlans()) } }}>Remove plan</button></div>
            </div> : null}
          </article>
        })}
      </div>
      <div style={{ border: '1px solid #34475a', borderRadius: 12, padding: 20, background: '#17212e' }}>
        <h2 style={{ color: '#f0f4fa', marginTop: 0 }}>Execution calculator</h2>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(190px,1fr))', gap: 14 }}>
          <label style={{ display: 'grid', gap: 5, color: '#cbd6df', fontSize: 12 }}>Market
            <select className="ws-select" value={form.market} onChange={e => update('market',e.target.value)}>
              {Array.from(new Set([form.market, ...allInstrumentIds()])).map(m => <option key={m} value={m}>{m}</option>)}
            </select>
          </label>
          <label style={{ display: 'grid', gap: 5, color: '#cbd6df', fontSize: 12 }}>Direction
            <select className="ws-select" value={form.side} onChange={e => update('side',e.target.value)}><option value="long">Long</option><option value="short">Short</option></select>
          </label>
          {field('Entry', 'entry')}{field('Stop loss', 'stop')}{field('Target', 'target')}
          {field('Account balance', 'account')}{field('Risk %', 'riskPct')}
        </div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 20, margin: '18px 0', color: '#f0f4fa' }}>
          <span>Cash risk: <strong>{money(risk)}</strong></span>
          <span>Reward/risk: <strong>{valid ? rr.toFixed(2) + 'R' : '—'}</strong></span>
          <span>Stop distance: <strong>{valid ? money(stopDistance) : '—'}</strong></span>
        </div>
        {!valid && valuesPresent ? <p style={{ color: '#e5b777' }}>Check the entry, stop and target: they must be on the correct sides for your trade direction.</p> : null}
        <label style={{ display: 'grid', gap: 5, color: '#cbd6df', fontSize: 12 }}>Thesis / zone / COT notes
          <textarea className="ws-input" rows={3} value={form.notes} onChange={e => update('notes',e.target.value)} />
        </label>
        <button className="ws-btn ws-btn-primary" type="button" disabled={!valid} onClick={save} style={{ marginTop: 14 }}>Save plan locally</button>
        <p style={{ fontSize: 12, color: '#aebdcb' }}>Planning only. No orders are sent. Position size needs instrument-specific contract value, lot minimum and currency conversion; this calculator does not guess them.</p>
      </div>
      <div style={{ border: '1px solid #34475a', borderRadius: 12, padding: 20, background: '#17212e' }}>
        <h2 style={{ color: '#f0f4fa', marginTop: 0 }}>Saved plans ({plans.length})</h2>
        {!plans.length ? <p style={{ color: '#aebdcb' }}>No plans yet.</p> : plans.map(p => <div key={p.id} style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', gap: 10, borderTop: '1px solid #34475a', padding: '12px 0', color: '#e4ebf4' }}>
          <div><strong>{p.market} · {p.side.toUpperCase()}</strong><div style={{ fontSize: 12, color: '#aebdcb' }}>Entry {p.entry} · Stop {p.stop} · Target {p.target} · {Number(p.rr).toFixed(2)}R · Risk {money(Number(p.risk))}</div>{p.notes ? <div style={{ fontSize: 12, marginTop: 5 }}>{p.notes}</div> : null}</div>
          <button type="button" className="ws-btn" onClick={() => remove(p.id)}>Remove</button>
        </div>)}
      </div>
    </section>
  </AppShell>
}
