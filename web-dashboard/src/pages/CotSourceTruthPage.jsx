import React from 'react'
import { AppShell } from '../components/AppShell.jsx'
import { loadCotSourceTruthLatest, sourceTruthInstruments, statusTone } from '../cotSourceTruthData.js'
import { navigateToInstrument, navigateToScanner } from '../routing.js'

const fmt = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return n.toLocaleString(undefined, { maximumFractionDigits: 0 })
}

const matchLabel = (ok) => (ok === true ? 'PASS' : ok === false ? 'FAIL' : '—')

function MetricCompareTable({ title, comparison, officialValues }) {
  if (!comparison) return null
  return (
    <div className="cot-proof-group-block">
      <h4 className="wo-cot-section-title">{title}</h4>
      <table className="wo-cot-hist-table cot-integrity-table cot-proof-detail-table">
        <thead>
          <tr>
            <th>Field</th>
            <th>Dashboard rendered</th>
            <th>Official CFTC raw</th>
            <th>Match</th>
            <th>Diff</th>
          </tr>
        </thead>
        <tbody>
          {['long', 'short', 'net'].map((field) => {
            const m = comparison[field] || {}
            return (
              <tr key={field}>
                <th scope="row">{field}</th>
                <td>{fmt(m.dashboard_value)}</td>
                <td>{fmt(m.official_raw_value)}</td>
                <td>
                  <span className={`cot-proof-badge cot-proof-badge-${m.match ? 'pass' : 'fail'}`}>
                    {matchLabel(m.match)}
                  </span>
                </td>
                <td>{m.difference != null ? fmt(m.difference) : '—'}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
      {officialValues?.spread != null ? (
        <p className="wo-cot-hint wo-cot-hint-tight">Official NC spread (not on dashboard): {fmt(officialValues.spread)}</p>
      ) : null}
    </div>
  )
}

function InstrumentRow({ row }) {
  const [open, setOpen] = React.useState(false)
  return (
    <>
      <tr
        className={`cot-proof-row cot-proof-row-${statusTone(row.status)}`}
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
              navigateToInstrument(row.instrument)
            }}
          >
            {row.instrument}
          </button>
        </th>
        <td>{row.report_date}</td>
        <td>{row.selected_cftc_code || '—'}</td>
        <td className="cot-proof-market-name">{row.selected_market_name || '—'}</td>
        <td>
          <span className={`cot-proof-badge cot-proof-badge-${row.nc_match ? 'pass' : 'fail'}`}>
            {matchLabel(row.nc_match)}
          </span>
        </td>
        <td>
          <span className={`cot-proof-badge cot-proof-badge-${row.nonreportable_match ? 'pass' : 'fail'}`}>
            {matchLabel(row.nonreportable_match)}
          </span>
        </td>
        <td>
          <span className={`cot-proof-badge cot-proof-badge-${statusTone(row.status)}`}>{row.status}</span>
        </td>
        <td className="cot-proof-market-name">{row.failure_reasons?.[0] || '—'}</td>
      </tr>
      {open ? (
        <tr className="cot-proof-detail-row">
          <td colSpan={8}>
            <div className="cot-proof-detail">
              <p className="wo-cot-meta-line">
                <strong>Exchange:</strong> {row.exchange || '—'} · <strong>Official:</strong>{' '}
                {row.official_raw_source_file} (row {row.official_raw_row_index}) ·{' '}
                <a href={row.official_raw_source_url} target="_blank" rel="noreferrer">
                  CFTC zip
                </a>
              </p>
              <p className="wo-cot-meta-line">
                <strong>Dashboard source (rendered, not truth):</strong> {row.dashboard_source_file}
              </p>
              {row.failure_reasons?.length ? (
                <ul className="cot-proof-mismatch-list">
                  {row.failure_reasons.map((r) => (
                    <li key={r}>{r}</li>
                  ))}
                </ul>
              ) : (
                <p className="cot-integrity-pass">Dashboard matches independent official CFTC parse.</p>
              )}
              <MetricCompareTable
                title="Non-Commercials"
                comparison={row.comparisons?.noncommercials}
                officialValues={row.official_raw_values?.noncommercials}
              />
              <MetricCompareTable
                title="Non-Reportables"
                comparison={row.comparisons?.nonreportables}
                officialValues={row.official_raw_values?.nonreportables}
              />
              {row.candidate_row_count > 1 ? (
                <details className="cot-source-truth-candidates">
                  <summary>{row.candidate_row_count} candidate official rows on this date</summary>
                  <pre style={{ fontSize: '0.68rem', overflow: 'auto' }}>
                    {JSON.stringify(row.candidate_rows, null, 2)}
                  </pre>
                </details>
              ) : null}
            </div>
          </td>
        </tr>
      ) : null}
    </>
  )
}

export function CotSourceTruthPage({ sidebarClass, onSidebarClass }) {
  const [doc, setDoc] = React.useState(null)
  const [error, setError] = React.useState(null)
  const [loading, setLoading] = React.useState(true)
  const [filter, setFilter] = React.useState('all')
  const [showFocus, setShowFocus] = React.useState(false)

  React.useEffect(() => {
    let cancelled = false
    loadCotSourceTruthLatest()
      .then((d) => {
        if (!cancelled) {
          setDoc(d)
          setError(null)
        }
      })
      .catch((e) => {
        if (!cancelled) setError(e?.message || 'Failed to load source truth audit')
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [])

  const rows = React.useMemo(() => sourceTruthInstruments(doc), [doc])
  const filtered = React.useMemo(() => {
    if (filter === 'all') return rows
    return rows.filter((r) => r.status === filter)
  }, [rows, filter])

  const summary = doc?.summary || {}
  const focus = doc?.special_focus || {}
  const official = doc?.official_source || {}

  return (
    <AppShell
      title="COT Source Truth"
      subtitle="Official CFTC Legacy Futures Only vs dashboard rendered"
      sidebarClass={sidebarClass}
      onSidebarClass={onSidebarClass}
      topActions={
        <button type="button" className="ws-btn" onClick={navigateToScanner}>
          ← Scanner
        </button>
      }
    >
      <section className="cot-proof-page">
        {loading ? <p className="ws-topbar-meta">Loading source truth audit…</p> : null}
        {error ? (
          <p className="ws-error-banner">
            {error}. Run: <code>python -m hptl.cot.run_cot_source_truth_audit</code>
          </p>
        ) : null}

        {doc && !error ? (
          <>
            <div className={`cot-proof-gate cot-proof-gate-${summary.all_pass ? 'pass' : 'fail'}`}>
              <strong>{summary.all_pass ? 'ALL PASS' : 'NOT TRUSTED'}</strong>
              <p>
                Independent chain: fresh CFTC deacot download → parse → compare to confluence dashboard
                records (positioning trail).
              </p>
              <p className="wo-cot-meta-line">
                Official: {official.source_file} · {doc.latest_report_date} ·{' '}
                <a href={official.source_url} target="_blank" rel="noreferrer">
                  {official.source_url}
                </a>
              </p>
            </div>

            <div className="cot-proof-summary-grid">
              <div className="cot-proof-stat">
                <span className="lbl">Checked</span>
                <span className="val">{summary.total_instruments_checked}</span>
              </div>
              <div className="cot-proof-stat cot-proof-stat-pass">
                <span className="lbl">PASS</span>
                <span className="val">{summary.pass_count}</span>
              </div>
              <div className="cot-proof-stat cot-proof-stat-fail">
                <span className="lbl">FAIL</span>
                <span className="val">{summary.fail_count}</span>
              </div>
              <div className="cot-proof-stat cot-proof-stat-review">
                <span className="lbl">NEEDS REVIEW</span>
                <span className="val">{summary.needs_manual_review_count}</span>
              </div>
            </div>

            <div className="cot-proof-toolbar">
              <label>
                Filter
                <select className="ws-select" value={filter} onChange={(e) => setFilter(e.target.value)}>
                  <option value="all">All</option>
                  <option value="PASS">PASS</option>
                  <option value="FAIL">FAIL</option>
                  <option value="NEEDS_MANUAL_REVIEW">NEEDS REVIEW</option>
                </select>
              </label>
              <button type="button" className="ws-btn" onClick={() => setShowFocus((v) => !v)}>
                {showFocus ? 'Hide' : 'Show'} CL / NQ official rows
              </button>
            </div>

            {showFocus ? (
              <div className="cot-proof-fail-box">
                <h3>Crude-related official rows ({doc.latest_report_date})</h3>
                <p className="wo-cot-hint">{focus.cl_recommendation}</p>
                <ul className="cot-proof-mismatch-list" style={{ color: '#e2e8f0' }}>
                  {(focus.crude_oil_cl_all_rows_2026_05_26 || []).map((r) => (
                    <li key={r.cftc_code + r.market_name}>
                      <code>{r.cftc_code}</code> {r.market_name} · {r.exchange} · NC {fmt(r.noncommercial_long)}/
                      {fmt(r.noncommercial_short)} · NR {fmt(r.nonreportable_long)}/{fmt(r.nonreportable_short)} · OI{' '}
                      {fmt(r.open_interest)}
                      {r.htpl_canonical_for_cl ? ' [HTPL CL]' : ''}
                    </li>
                  ))}
                </ul>
                <h3>NQ-related official rows</h3>
                <ul className="cot-proof-mismatch-list" style={{ color: '#e2e8f0' }}>
                  {(focus.nasdaq_nq_all_rows_2026_05_26 || []).map((r) => (
                    <li key={r.cftc_code + r.market_name}>
                      <code>{r.cftc_code}</code> {r.market_name} · NC {fmt(r.noncommercial_long)}/
                      {fmt(r.noncommercial_short)}
                      {r.htpl_canonical_for_nq ? ' [HTPL NQ]' : ''}
                    </li>
                  ))}
                </ul>
              </div>
            ) : null}

            <div className="wo-cot-hist-table-wrap">
              <table className="wo-cot-hist-table cot-proof-table">
                <thead>
                  <tr>
                    <th>Instrument</th>
                    <th>Date</th>
                    <th>CFTC Code</th>
                    <th>Market Name</th>
                    <th>NC Match</th>
                    <th>Non-Reportable Match</th>
                    <th>Status</th>
                    <th>Failure Reason</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((row) => (
                    <InstrumentRow key={row.instrument} row={row} />
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
