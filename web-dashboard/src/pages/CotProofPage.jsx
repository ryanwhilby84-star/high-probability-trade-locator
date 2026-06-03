import React from 'react'
import { AppShell } from '../components/AppShell.jsx'
import { loadCotProofLatest, proofInstrumentsList, statusTone } from '../cotProofData.js'
import { navigateToInstrument, navigateToScanner } from '../routing.js'

const fmt = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return n.toLocaleString(undefined, { maximumFractionDigits: 0 })
}

const matchIcon = (m) => (m === true ? '✓' : m === false ? '✗' : '—')

function GroupDetailTable({ group }) {
  if (!group) return null
  const rows = [
    { label: 'Long', key: 'long' },
    { label: 'Short', key: 'short' },
    { label: 'Net', key: 'net' },
  ]
  return (
    <table className="wo-cot-hist-table cot-integrity-table cot-proof-detail-table">
      <thead>
        <tr>
          <th>Field</th>
          <th>Legacy panel</th>
          <th>Raw CFTC</th>
          <th>Match</th>
          <th>Confluence</th>
          <th>Match</th>
          <th>Diff (panel−raw)</th>
        </tr>
      </thead>
      <tbody>
        {rows.map(({ label, key }) => {
          const leg = group.legacy_panel?.[key] || {}
          const raw = group.raw_cftc?.[key]
          const conf = group.confluence_headline?.[key] || {}
          return (
            <tr key={key}>
              <th scope="row">{label}</th>
              <td>{fmt(leg.dashboard_value)}</td>
              <td>{fmt(raw)}</td>
              <td className={`cot-proof-match-${matchIcon(leg.match)}`}>{matchIcon(leg.match)}</td>
              <td>{fmt(conf.dashboard_value)}</td>
              <td className={`cot-proof-match-${matchIcon(conf.match)}`}>{matchIcon(conf.match)}</td>
              <td>{leg.difference != null ? fmt(leg.difference) : '—'}</td>
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}

function InstrumentProofRow({ row }) {
  const [open, setOpen] = React.useState(false)
  const cols = row.column_status || {}
  return (
    <>
      <tr
        className={`cot-proof-row cot-proof-row-${statusTone(row.overall_status)}`}
        onClick={() => setOpen((v) => !v)}
        style={{ cursor: 'pointer' }}
      >
        <th scope="row">
          <button type="button" className="cot-proof-expand" aria-expanded={open}>
            {open ? '▼' : '▶'}
          </button>{' '}
          <button
            type="button"
            className="cot-proof-link"
            onClick={(e) => {
              e.stopPropagation()
              navigateToInstrument(row.instrument_id)
            }}
          >
            {row.instrument_id}
          </button>
        </th>
        <td>{row.report_date || '—'}</td>
        <td>{row.cftc_code || '—'}</td>
        <td className="cot-proof-market-name">{row.market_name || '—'}</td>
        <td>
          <span className={`cot-proof-badge cot-proof-badge-${statusTone(cols.NC)}`}>{cols.NC || '—'}</span>
        </td>
        <td>
          <span className={`cot-proof-badge cot-proof-badge-${statusTone(cols.Commercial)}`}>
            {cols.Commercial || '—'}
          </span>
        </td>
        <td>
          <span className={`cot-proof-badge cot-proof-badge-${statusTone(cols['Non-Reportable'])}`}>
            {cols['Non-Reportable'] || '—'}
          </span>
        </td>
        <td>
          <span className={`cot-proof-badge cot-proof-badge-${statusTone(row.overall_status)}`}>
            {row.overall_status}
          </span>
        </td>
      </tr>
      {open ? (
        <tr className="cot-proof-detail-row">
          <td colSpan={8}>
            <div className="cot-proof-detail">
              <p className="wo-cot-meta-line">
                <strong>Mapping:</strong> {row.mapping_status} · <strong>Report type:</strong>{' '}
                {row.report_type || '—'} · <strong>Source:</strong> {row.source_file || '—'} row{' '}
                {row.source_row ?? '—'}
                {row.confluence_present === false ? (
                  <span className="cot-proof-warn"> · No confluence row for this date</span>
                ) : null}
                {row.confluence_trader_group_used ? (
                  <span className="cot-proof-warn">
                    {' '}
                    · Confluence trader group: {row.confluence_trader_group_used}
                  </span>
                ) : null}
              </p>
              {row.mismatch_reasons?.length ? (
                <ul className="cot-proof-mismatch-list">
                  {row.mismatch_reasons.map((reason) => (
                    <li key={reason}>{reason}</li>
                  ))}
                </ul>
              ) : (
                <p className="cot-integrity-pass">All dashboard fields match raw Legacy CFTC for this week.</p>
              )}
              {Object.values(row.groups || {}).map((g) => (
                <div key={g.group_id} className="cot-proof-group-block">
                  <h4 className="wo-cot-section-title">
                    {g.label} — <span className={`cot-proof-badge cot-proof-badge-${statusTone(g.status)}`}>{g.status}</span>
                  </h4>
                  <GroupDetailTable group={g} />
                </div>
              ))}
            </div>
          </td>
        </tr>
      ) : null}
    </>
  )
}

export function CotProofPage({ sidebarClass, onSidebarClass }) {
  const [doc, setDoc] = React.useState(null)
  const [error, setError] = React.useState(null)
  const [loading, setLoading] = React.useState(true)
  const [filter, setFilter] = React.useState('all')

  React.useEffect(() => {
    let cancelled = false
    loadCotProofLatest()
      .then((d) => {
        if (!cancelled) {
          setDoc(d)
          setError(null)
        }
      })
      .catch((e) => {
        if (!cancelled) setError(e?.message || 'Failed to load proof data')
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [])

  const rows = React.useMemo(() => proofInstrumentsList(doc), [doc])
  const filtered = React.useMemo(() => {
    if (filter === 'all') return rows
    return rows.filter((r) => r.overall_status === filter)
  }, [rows, filter])

  const summary = doc?.summary || {}
  const gate = doc?.gate || {}

  return (
    <AppShell
      title="COT Proof"
      subtitle="Legacy dashboard vs raw CFTC — gate before scoring"
      sidebarClass={sidebarClass}
      onSidebarClass={onSidebarClass}
      topActions={
        <button type="button" className="ws-btn" onClick={navigateToScanner}>
          ← Scanner
        </button>
      }
    >
      <section className="cot-proof-page">
        {loading ? <p className="ws-topbar-meta">Loading cot_proof_latest.json…</p> : null}
        {error ? (
          <p className="ws-error-banner">
            {error}. Run: <code>python -m hptl.cot.run_cot_proof</code>
          </p>
        ) : null}

        {doc && !error ? (
          <>
            <div className={`cot-proof-gate cot-proof-gate-${gate.trusted ? 'pass' : 'fail'}`}>
              <strong>{gate.trusted ? 'TRUSTED' : 'BLOCKED'}</strong>
              <p>{gate.message}</p>
              <p className="wo-cot-meta-line">
                Latest COT week: {doc.latest_report_date || '—'} · Generated: {doc.generated_at || '—'}
              </p>
            </div>

            <div className="cot-proof-summary-grid">
              <div className="cot-proof-stat">
                <span className="lbl">Checked</span>
                <span className="val">{summary.total_instruments_checked ?? '—'}</span>
              </div>
              <div className="cot-proof-stat cot-proof-stat-pass">
                <span className="lbl">PASS</span>
                <span className="val">{summary.pass_count ?? 0}</span>
              </div>
              <div className="cot-proof-stat cot-proof-stat-fail">
                <span className="lbl">FAIL</span>
                <span className="val">{summary.fail_count ?? 0}</span>
              </div>
              <div className="cot-proof-stat cot-proof-stat-review">
                <span className="lbl">NEEDS REVIEW</span>
                <span className="val">{summary.needs_review_count ?? 0}</span>
              </div>
            </div>

            {summary.failed_instruments?.length ? (
              <div className="cot-proof-fail-box">
                <h3>Failed instruments</h3>
                <p>{summary.failed_instruments.join(' · ')}</p>
                {summary.mismatch_reasons_sample?.length ? (
                  <>
                    <h4>Sample mismatch reasons</h4>
                    <ul className="cot-proof-mismatch-list">
                      {summary.mismatch_reasons_sample.slice(0, 15).map((r) => (
                        <li key={r}>{r}</li>
                      ))}
                    </ul>
                  </>
                ) : null}
              </div>
            ) : null}

            <div className="cot-proof-toolbar">
              <label>
                Filter
                <select className="ws-select" value={filter} onChange={(e) => setFilter(e.target.value)}>
                  <option value="all">All ({rows.length})</option>
                  <option value="PASS">PASS only</option>
                  <option value="FAIL">FAIL only</option>
                  <option value="NEEDS_REVIEW">NEEDS REVIEW only</option>
                </select>
              </label>
              <span className="ws-topbar-meta">
                Legacy panel = <code>legacy_cot_latest.json</code> · Confluence = headline export · Raw = deacot zip
              </span>
            </div>

            <div className="wo-cot-hist-table-wrap">
              <table className="wo-cot-hist-table cot-proof-table">
                <thead>
                  <tr>
                    <th>Instrument</th>
                    <th>Date</th>
                    <th>CFTC Code</th>
                    <th>Market Name</th>
                    <th>NC</th>
                    <th>Commercial</th>
                    <th>Non-Reportable</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((row) => (
                    <InstrumentProofRow key={row.instrument_id} row={row} />
                  ))}
                </tbody>
              </table>
            </div>
          </>
        ) : null}
      </section>
    </AppShell>
  )
}
