import React from 'react'
import { TRADE_DIRECTIONS, TRADE_STATUSES } from '../journal/journalPrefill.js'
import { saveJournalEntry } from '../journal/journalApi.js'

const numOrNull = (v) => {
  if (v === '' || v === null || v === undefined) return null
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

export function LogTradeIdeaModal({ open, prefill, onClose, onSaved }) {
  const [form, setForm] = React.useState(prefill || {})
  const [secret, setSecret] = React.useState('')
  const [saving, setSaving] = React.useState(false)
  const [error, setError] = React.useState(null)
  const [okMsg, setOkMsg] = React.useState(null)

  React.useEffect(() => {
    if (open && prefill) setForm({ ...prefill })
  }, [open, prefill])

  if (!open) return null

  const set = (key) => (e) => setForm((f) => ({ ...f, [key]: e.target.value }))

  const submit = async (e) => {
    e.preventDefault()
    setSaving(true)
    setError(null)
    setOkMsg(null)
    try {
      const payload = {
        market: form.market,
        symbol: form.symbol,
        direction: form.direction,
        status: form.status,
        entry_price: numOrNull(form.entry_price),
        stop_loss: numOrNull(form.stop_loss),
        target_1: numOrNull(form.target_1),
        target_2: numOrNull(form.target_2),
        risk_amount: numOrNull(form.risk_amount),
        timeframe: form.timeframe,
        setup_type: form.setup_type,
        thesis: form.thesis,
        notes: form.notes,
        cot_bias: form.cot_bias,
        cot_score: numOrNull(form.cot_score),
        macro_bias: form.macro_bias,
        weather_bias: form.weather_bias,
        catalyst_risk: form.catalyst_risk,
        dashboard_snapshot: form.dashboard_snapshot || {},
      }
      const res = await saveJournalEntry(payload, { secret: secret.trim() })
      setOkMsg(`Logged ${res.entry?.trade_id || 'trade'} (planning only — no orders sent).`)
      onSaved?.(res.entry)
    } catch (err) {
      setError(
        err?.message ||
          'Could not reach journal server. Run: python -m hptl.journal.run_webhook_server',
      )
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="tj-modal-backdrop" role="dialog" aria-modal="true" aria-labelledby="tj-modal-title">
      <div className="tj-modal">
        <div className="tj-modal-head">
          <h3 id="tj-modal-title">Log trade idea</h3>
          <p className="tj-disclaimer">Planning log only — HPTL does not place orders or connect to brokers.</p>
          <button type="button" className="ws-btn tj-close" onClick={onClose} aria-label="Close">
            ×
          </button>
        </div>
        <form className="tj-form" onSubmit={submit}>
          <div className="tj-grid">
            <label>
              Market
              <input className="ws-input" value={form.market || ''} onChange={set('market')} required />
            </label>
            <label>
              Symbol
              <input className="ws-input" value={form.symbol || ''} onChange={set('symbol')} placeholder="e.g. WHEATUSD" />
            </label>
            <label>
              Direction
              <select className="ws-select" value={form.direction || 'long'} onChange={set('direction')}>
                {TRADE_DIRECTIONS.map((d) => (
                  <option key={d} value={d}>
                    {d}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Status
              <select className="ws-select" value={form.status || 'idea'} onChange={set('status')}>
                {TRADE_STATUSES.map((s) => (
                  <option key={s} value={s}>
                    {s}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Entry
              <input className="ws-input" type="number" step="any" value={form.entry_price} onChange={set('entry_price')} />
            </label>
            <label>
              Stop
              <input className="ws-input" type="number" step="any" value={form.stop_loss} onChange={set('stop_loss')} />
            </label>
            <label>
              Target 1
              <input className="ws-input" type="number" step="any" value={form.target_1} onChange={set('target_1')} />
            </label>
            <label>
              Target 2
              <input className="ws-input" type="number" step="any" value={form.target_2} onChange={set('target_2')} />
            </label>
            <label>
              Risk ($)
              <input className="ws-input" type="number" step="any" value={form.risk_amount} onChange={set('risk_amount')} />
            </label>
            <label>
              Timeframe
              <input className="ws-input" value={form.timeframe || ''} onChange={set('timeframe')} />
            </label>
          </div>
          <label>
            Setup type
            <input className="ws-input" value={form.setup_type || ''} onChange={set('setup_type')} />
          </label>
          <label>
            Thesis
            <textarea className="ws-input tj-textarea" rows={2} value={form.thesis || ''} onChange={set('thesis')} />
          </label>
          <label>
            Notes
            <textarea className="ws-input tj-textarea" rows={2} value={form.notes || ''} onChange={set('notes')} />
          </label>
          <div className="tj-context">
            <span>COT: {form.cot_bias || '—'} ({form.cot_score ?? '—'})</span>
            <span>Macro: {form.macro_bias || '—'}</span>
            <span>Weather: {form.weather_bias || '—'}</span>
            <span>Catalyst: {form.catalyst_risk || '—'}</span>
            <span>Week: {form.dashboard_snapshot?.cot_calendar_week || '—'}</span>
          </div>
          <label>
            Webhook secret (local server)
            <input
              className="ws-input"
              type="password"
              value={secret}
              onChange={(e) => setSecret(e.target.value)}
              placeholder="TRADINGVIEW_WEBHOOK_SECRET"
              autoComplete="off"
            />
          </label>
          {error ? (
            <p className="tj-error" role="alert">
              {error}
            </p>
          ) : null}
          {okMsg ? <p className="tj-ok">{okMsg}</p> : null}
          <div className="tj-actions">
            <button type="button" className="ws-btn" onClick={onClose}>
              Cancel
            </button>
            <button type="submit" className="ws-btn ws-btn-primary" disabled={saving}>
              {saving ? 'Saving…' : 'Save to journal'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
