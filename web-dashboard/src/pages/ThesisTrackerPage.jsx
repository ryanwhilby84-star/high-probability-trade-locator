import React from 'react'
import { AppShell } from '../components/AppShell.jsx'
import { useThesisTrackerData } from '../hooks/useThesisTrackerData.js'
import { navigateToInstrument, navigateToScanner } from '../routing.js'
import { ALL_STATUSES, normStatus, statusMeta, computeConviction, convictionSeries } from '../thesisTracker/thesisModel.js'
import { getDecision } from '../thesisTracker/thesisNarrative.js'
import { getOpportunity, actionMeta, ACTION_META } from '../thesisTracker/opportunityModel.js'
import { ThesisSummaryCard } from '../thesisTracker/ThesisSummaryCard.jsx'
import { WhyBreakdown } from '../thesisTracker/WhyBreakdown.jsx'

function ActionBadge({ thesis }) {
  const meta = actionMeta(thesis)
  return (
    <span className={`toe-act-badge toe-act-badge--${meta.tone}`} title={getOpportunity(thesis).headline}>
      {meta.label}
    </span>
  )
}

function OpportunityRow({ thesis, selected, onSelect }) {
  const opp = getOpportunity(thesis)
  const shortName = String(thesis.market || '').split('/')[0].trim().toUpperCase()
  return (
    <tr
      className={`toe-row${selected ? ' toe-row--active' : ''}`}
      onClick={() => onSelect(thesis)}
      tabIndex={0}
      role="button"
      onKeyDown={(e) => e.key === 'Enter' && onSelect(thesis)}
    >
      <td className="toe-row-market">
        {shortName}
        {thesis._local ? <span className="tt-local-dot" title="Local">•</span> : null}
      </td>
      <td className="toe-row-align mcat-mono">{opp.alignment?.label || '—'}</td>
      <td className="toe-row-action">
        <ActionBadge thesis={thesis} />
      </td>
    </tr>
  )
}

function SnapshotTable({ snapshots }) {
  if (!snapshots?.length) return <p className="tt-muted">No weekly snapshots captured.</p>
  const fmtNum = (v) => (typeof v === 'number' && Number.isFinite(v) ? v.toLocaleString() : '—')
  const fmtScore = (v) => (typeof v === 'number' && Number.isFinite(v) ? v.toFixed(1) : '—')
  return (
    <div className="tt-table-wrap">
      <table className="mcat-table tt-snap-table">
        <thead>
          <tr>
            <th>Week</th>
            <th>COT</th>
            <th>Score</th>
            <th>Net</th>
            <th>Retail net</th>
            <th>Zone</th>
            <th>Macro</th>
            <th>Conviction</th>
          </tr>
        </thead>
        <tbody>
          {snapshots.map((s) => (
            <tr key={s.week}>
              <td className="mcat-mono">{s.week}</td>
              <td>{s.cot_bias || '—'}</td>
              <td className="mcat-mono">{fmtScore(s.cot_score)}</td>
              <td className="mcat-mono">{fmtNum(s.net_value)}</td>
              <td className="mcat-mono">{fmtNum(s.retail_net)}</td>
              <td>{s.zone_focus || '—'}</td>
              <td className="mcat-mono">{fmtScore(s.macro_score)}</td>
              <td className="mcat-mono">{computeConviction(s).score ?? '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function EvolutionLog({ log }) {
  if (!log?.length) return <p className="tt-muted">No evolution entries yet.</p>
  const ordered = [...log].sort((a, b) => String(b.created_at || '').localeCompare(String(a.created_at || '')))
  return (
    <ul className="tt-log">
      {ordered.map((e, i) => (
        <li key={i} className={`tt-log-item${e.auto ? ' tt-log-item--auto' : ''}`}>
          <div className="tt-log-meta">
            {e.week ? <span className="mcat-mono">{e.week}</span> : null}
            <span className={`tt-log-kind tt-log-kind--${e.auto ? 'auto' : 'manual'}`}>{e.auto ? 'auto' : 'note'}</span>
          </div>
          <div className="tt-log-text">{e.text}</div>
        </li>
      ))}
    </ul>
  )
}

function DetailPanel({ thesis, actions, onClose }) {
  const [note, setNote] = React.useState('')
  if (!thesis) {
    return (
      <aside className="toe-detail toe-detail--empty">
        <p className="tt-muted">Select an instrument to see alignment, action, and evidence.</p>
      </aside>
    )
  }
  const dec = getDecision(thesis)

  return (
    <aside className="toe-detail">
      <div className="toe-detail-top">
        <button type="button" className="ws-btn" onClick={onClose}>
          Close
        </button>
      </div>

      <section className="toe-detail-section">
        <h3 className="toe-section-tag">Section 1 — Thesis summary</h3>
        <ThesisSummaryCard thesis={thesis} />
      </section>

      <section className="toe-detail-section">
        <h3 className="toe-section-tag">Section 2 — Why this score exists</h3>
        <WhyBreakdown thesis={thesis} />
      </section>

      <div className="toe-detail-controls">
        <label className="tt-control">
          Status
          <select
            className="ws-select"
            value={normStatus(thesis.status)}
            onChange={(e) => actions.setStatus(thesis.thesis_id, e.target.value)}
          >
            {ALL_STATUSES.map((s) => (
              <option key={s} value={s}>
                {statusMeta(s).label}
              </option>
            ))}
          </select>
        </label>
        <button type="button" className="ws-btn" onClick={() => navigateToInstrument(thesis.market)}>
          Open instrument
        </button>
        <button type="button" className="ws-btn ws-btn-danger" onClick={() => actions.remove(thesis.thesis_id)}>
          Remove
        </button>
      </div>

      <details className="toe-advanced">
        <summary>Section 3 — Advanced detail (COT, participation, conviction, narrative)</summary>
        <div className="toe-advanced-body">
          <p className="tt-headline">{dec.headline}</p>
          <div className="tt-story">
            {(dec.story || []).map((s, i) => (
              <p key={i}>{s}</p>
            ))}
          </div>
          <p className="tt-interp">
            <strong>Interpretation:</strong> {dec.interpretation}
          </p>
          <div className="tt-conv-line">
            <span className="tt-muted">Composite conviction</span>
            <strong>{thesis.conviction_current ?? '—'}/100</strong>
          </div>
          <h5>Weekly evolution log</h5>
          <EvolutionLog log={thesis.evolution_log} />
          <div className="tt-note-row">
            <input
              className="ws-select tt-note-input"
              placeholder="Add a note (stored locally)…"
              value={note}
              onChange={(e) => setNote(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && note.trim()) {
                  actions.addNote(thesis.thesis_id, note)
                  setNote('')
                }
              }}
            />
            <button
              type="button"
              className="ws-btn"
              disabled={!note.trim()}
              onClick={() => {
                actions.addNote(thesis.thesis_id, note)
                setNote('')
              }}
            >
              Add note
            </button>
          </div>
          <h5>Weekly snapshots</h5>
          <SnapshotTable snapshots={thesis.snapshots} />
        </div>
      </details>
    </aside>
  )
}

export function ThesisTrackerPage({ sidebarClass, onSidebarClass }) {
  const { theses, loading, error, reload, doc, actions } = useThesisTrackerData()
  const [actionFilter, setActionFilter] = React.useState('all')
  const [showArchived, setShowArchived] = React.useState(false)
  const [selectedId, setSelectedId] = React.useState(null)

  const filtered = React.useMemo(() => {
    return theses.filter((t) => {
      if (!showArchived && t.archived) return false
      if (actionFilter === 'all') return true
      const key = getOpportunity(t).action_key
      return key === actionFilter
    })
  }, [theses, actionFilter, showArchived])

  const selected = React.useMemo(() => theses.find((t) => t.thesis_id === selectedId) || null, [theses, selectedId])

  const actionCounts = React.useMemo(() => {
    const c = { all: 0, high_attention: 0, pay_attention: 0, watch: 0, no_edge: 0 }
    theses.forEach((t) => {
      if (!showArchived && t.archived) return
      c.all += 1
      const k = getOpportunity(t).action_key
      if (k in c) c[k] += 1
    })
    return c
  }, [theses, showArchived])

  return (
    <AppShell
      title="Opportunity Engine"
      subtitle="Institutional alignment and action — no COT literacy required"
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
      <p className="tj-disclaimer">
        {doc?.disclaimer ||
          'Five-pillar alignment: institutions, retail, location, valuation (52w percentile), and seasonality (calendar month). Action thresholds unchanged.'}
      </p>
      {loading ? <p className="mcat-meta-line">Loading opportunities…</p> : null}
      {error ? <p className="tj-error">{error}</p> : null}

      <div className="toe-filter-bar">
        <button
          type="button"
          className={`toe-filter${actionFilter === 'all' ? ' active' : ''}`}
          onClick={() => setActionFilter('all')}
        >
          All <span className="toe-filter-n">{actionCounts.all}</span>
        </button>
        {Object.entries(ACTION_META)
          .filter(([k]) => k !== 'closed')
          .sort((a, b) => a[1].sort - b[1].sort)
          .map(([key, meta]) => (
            <button
              key={key}
              type="button"
              className={`toe-filter toe-filter--${meta.tone}${actionFilter === key ? ' active' : ''}`}
              onClick={() => setActionFilter(actionFilter === key ? 'all' : key)}
            >
              {meta.label} <span className="toe-filter-n">{actionCounts[key] ?? 0}</span>
            </button>
          ))}
        <label className="tt-check">
          <input type="checkbox" checked={showArchived} onChange={(e) => setShowArchived(e.target.checked)} />
          Show archived
        </label>
        <button type="button" className="ws-btn" onClick={reload}>
          Reload JSON
        </button>
      </div>

      <div className="toe-layout">
        <div className="toe-main">
          {selected ? (
            <div className="toe-mobile-summary">
              <ThesisSummaryCard thesis={selected} compact />
            </div>
          ) : null}
          {filtered.length ? (
            <div className="toe-table-wrap">
              <table className="mcat-table toe-table">
                <thead>
                  <tr>
                    <th>Instrument</th>
                    <th>Alignment</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((t) => (
                    <OpportunityRow
                      key={t.thesis_id}
                      thesis={t}
                      selected={t.thesis_id === selectedId}
                      onSelect={(x) => setSelectedId(x.thesis_id)}
                    />
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="tt-empty">
              <p>No opportunities in this filter.</p>
              <p className="tt-muted">
                Seed with <code>python -m hptl.thesis_tracker.run_thesis_seed --reset --weeks 13</code>
              </p>
            </div>
          )}
        </div>
        <DetailPanel thesis={selected} actions={actions} onClose={() => setSelectedId(null)} />
      </div>
    </AppShell>
  )
}
