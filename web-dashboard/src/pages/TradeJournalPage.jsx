import React from 'react'
import { AppShell } from '../components/AppShell.jsx'
import { useTradeJournalData } from '../hooks/useTradeJournalData.js'
import { navigateToScanner } from '../routing.js'
import { TRADE_STATUSES } from '../journal/journalPrefill.js'

const OPEN_STATUSES = new Set(['idea', 'planned', 'order_set', 'triggered'])
const PLANNED = new Set(['planned', 'order_set'])
const INVALIDATED = new Set(['invalidated'])
const CLOSED = new Set(['closed'])

function fmtPrice(v) {
  if (v === null || v === undefined || v === '') return '—'
  const n = Number(v)
  return Number.isFinite(n) ? n.toFixed(4).replace(/\.?0+$/, '') : String(v)
}

function TradeTable({ title, rows, onSelect }) {
  if (!rows.length) {
    return (
      <div className="tj-section">
        <h4 className="tj-section-title">{title}</h4>
        <p className="mcat-empty">None</p>
      </div>
    )
  }
  return (
    <div className="tj-section">
      <h4 className="tj-section-title">{title}</h4>
      <table className="mcat-table">
        <thead>
          <tr>
            <th>Market</th>
            <th>Dir</th>
            <th>Status</th>
            <th>Entry</th>
            <th>Stop</th>
            <th>Targets</th>
            <th>Updated</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((t) => (
            <tr key={t.trade_id} className="tj-row-click" onClick={() => onSelect(t)} tabIndex={0} role="button">
              <td>{t.market}</td>
              <td>{t.direction}</td>
              <td>
                <span className={`tj-status tj-status-${t.status}`}>{t.status}</span>
              </td>
              <td className="mcat-mono">{fmtPrice(t.entry_price)}</td>
              <td className="mcat-mono">{fmtPrice(t.stop_loss)}</td>
              <td className="mcat-mono">
                {fmtPrice(t.target_1)} / {fmtPrice(t.target_2)}
              </td>
              <td className="mcat-mono">{(t.updated_at || t.created_at || '').slice(0, 16)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function TradeDetail({ trade, onClose }) {
  if (!trade) return null
  return (
    <aside className="tj-detail">
      <div className="tj-detail-head">
        <h3>{trade.market}</h3>
        <button type="button" className="ws-btn" onClick={onClose}>
          Close
        </button>
      </div>
      <p className="tj-disclaimer">Planning log — not an executed trade.</p>
      <dl className="tj-dl">
        <dt>ID</dt>
        <dd className="mcat-mono">{trade.trade_id}</dd>
        <dt>Symbol</dt>
        <dd>{trade.symbol || '—'}</dd>
        <dt>Direction</dt>
        <dd>{trade.direction}</dd>
        <dt>Status</dt>
        <dd>{trade.status}</dd>
        <dt>Entry / Stop</dt>
        <dd>
          {fmtPrice(trade.entry_price)} / {fmtPrice(trade.stop_loss)}
        </dd>
        <dt>Targets</dt>
        <dd>
          {fmtPrice(trade.target_1)} / {fmtPrice(trade.target_2)}
        </dd>
        <dt>Setup</dt>
        <dd>{trade.setup_type || '—'}</dd>
        <dt>Thesis</dt>
        <dd>{trade.thesis || '—'}</dd>
        <dt>Notes</dt>
        <dd>{trade.notes || '—'}</dd>
        <dt>COT</dt>
        <dd>
          {trade.cot_bias || '—'} ({trade.cot_score ?? '—'})
        </dd>
        <dt>Macro / Weather / Catalyst</dt>
        <dd>
          {trade.macro_bias || '—'} · {trade.weather_bias || '—'} · {trade.catalyst_risk || '—'}
        </dd>
        <dt>Source</dt>
        <dd>{trade.source || '—'}</dd>
        <dt>Created / Updated</dt>
        <dd>
          {(trade.created_at || '').slice(0, 19)} / {(trade.updated_at || '').slice(0, 19)}
        </dd>
      </dl>
    </aside>
  )
}

export function TradeJournalPage({ sidebarClass, onSidebarClass }) {
  const { entries, loading, error, reload, doc } = useTradeJournalData()
  const [statusFilter, setStatusFilter] = React.useState('all')
  const [marketFilter, setMarketFilter] = React.useState('')
  const [selected, setSelected] = React.useState(null)

  const markets = React.useMemo(() => {
    const s = new Set(entries.map((e) => e.market).filter(Boolean))
    return [...s].sort()
  }, [entries])

  const filtered = React.useMemo(() => {
    return entries.filter((e) => {
      if (marketFilter && e.market !== marketFilter) return false
      if (statusFilter === 'all') return true
      return String(e.status) === statusFilter
    })
  }, [entries, statusFilter, marketFilter])

  const open = filtered.filter((e) => OPEN_STATUSES.has(e.status))
  const planned = filtered.filter((e) => PLANNED.has(e.status))
  const invalidated = filtered.filter((e) => INVALIDATED.has(e.status))
  const closed = filtered.filter((e) => CLOSED.has(e.status))

  return (
    <AppShell
      title="Trade Journal"
      subtitle="Planning log only — no broker execution"
      date=""
      dates={[]}
      onDateChange={() => {}}
      latestCotReportDate=""
      sidebarClass={sidebarClass}
      onSidebarClass={onSidebarClass}
      topActions={
        <button type="button" className="ws-btn" onClick={navigateToScanner}>
          ← Scanner
        </button>
      }
    >
      <p className="tj-disclaimer">{doc?.disclaimer || 'Logging only. HPTL does not place orders.'}</p>
      {loading ? <p className="mcat-meta-line">Loading journal…</p> : null}
      {error ? <p className="tj-error">{error} — run validate or start journal server, then refresh export.</p> : null}
      <div className="tj-filters">
        <label>
          Status
          <select className="ws-select" value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
            <option value="all">All</option>
            {TRADE_STATUSES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </label>
        <label>
          Market
          <select className="ws-select" value={marketFilter} onChange={(e) => setMarketFilter(e.target.value)}>
            <option value="">All</option>
            {markets.map((m) => (
              <option key={m} value={m}>
                {m}
              </option>
            ))}
          </select>
        </label>
        <button type="button" className="ws-btn" onClick={reload}>
          Reload JSON
        </button>
      </div>
      <div className="tj-layout">
        <div className="tj-main">
          <TradeTable title="Open / active" rows={open} onSelect={setSelected} />
          <TradeTable title="Planned / orders set" rows={planned} onSelect={setSelected} />
          <TradeTable title="Invalidated" rows={invalidated} onSelect={setSelected} />
          <TradeTable title="Closed" rows={closed} onSelect={setSelected} />
        </div>
        <TradeDetail trade={selected} onClose={() => setSelected(null)} />
      </div>
    </AppShell>
  )
}
