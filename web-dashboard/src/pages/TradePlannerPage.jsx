import React from 'react'
import { AppShell } from '../components/AppShell.jsx'
import { allInstrumentIds } from '../instrumentRegistry.js'

const STORAGE = 'institutional-edge.trade-planner.v1'
const initial = { market: 'Soybeans', side: 'short', entry: '', stop: '', target: '', account: '10000', riskPct: '0.5', notes: '' }
function readPlans() {
  try { const data = JSON.parse(localStorage.getItem(STORAGE) || '[]'); return Array.isArray(data) ? data : [] } catch { return [] }
}
const money = n => Number.isFinite(n) ? n.toLocaleString('en-GB', { maximumFractionDigits: 2 }) : '—'

export function TradePlannerPage({ sidebarClass, onSidebarClass }) {
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
  return <AppShell title="Trade Planner" subtitle="Plan the location, invalidation, target and risk before placing an order" sidebarClass={sidebarClass} onSidebarClass={onSidebarClass}>
    <section style={{ maxWidth: 1050, display: 'grid', gap: 18 }}>
      <div style={{ border: '1px solid #34475a', borderRadius: 12, padding: 20, background: '#17212e' }}>
        <h2 style={{ color: '#f0f4fa', marginTop: 0 }}>New trade plan</h2>
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
