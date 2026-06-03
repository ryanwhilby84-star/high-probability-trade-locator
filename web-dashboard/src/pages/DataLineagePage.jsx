import React from 'react'
import { AppShell } from '../components/AppShell.jsx'
import { loadDataLineageLatest, lineageInstruments, statusTone, LAYER_ORDER } from '../dataLineageData.js'
import { navigateToInstrument, navigateToScanner } from '../routing.js'

const fmt = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return n.toLocaleString(undefined, { maximumFractionDigits: 1 })
}

function ValuesBlock({ values }) {
  if (!values) return null
  return (
    <ul className="lineage-values-list">
      <li>NC L/S/N: {fmt(values.nc_long)} / {fmt(values.nc_short)} / {fmt(values.nc_net)}</li>
      <li>NR L/S/N: {fmt(values.nr_long)} / {fmt(values.nr_short)} / {fmt(values.nr_net)}</li>
      <li>COT score: {fmt(values.cot_score)}</li>
    </ul>
  )
}

function InstrumentLineageRow({ row }) {
  const [open, setOpen] = React.useState(false)
  const layers = row.layers || {}
  const dash = layers.dashboard?.values || {}
  const truth = layers.source_truth?.values || {}
  const ncMatch =
    truth.nc_long === dash.nc_long && truth.nc_short === dash.nc_short && truth.nc_net === dash.nc_net
  const nrMatch =
    truth.nr_long === dash.nr_long && truth.nr_short === dash.nr_short && truth.nr_net === dash.nr_net

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
              navigateToInstrument(row.instrument)
            }}
          >
            {row.instrument}
          </button>
        </th>
        <td>{row.report_date}</td>
        <td>{row.mapping?.cftc_code || '—'}</td>
        <td className="cot-proof-market-name">{row.mapping?.market_name || '—'}</td>
        <td>
          <span className={`cot-proof-badge cot-proof-badge-${ncMatch ? 'pass' : 'fail'}`}>
            {ncMatch ? 'PASS' : 'FAIL'}
          </span>
        </td>
        <td>
          <span className={`cot-proof-badge cot-proof-badge-${nrMatch ? 'pass' : 'fail'}`}>
            {nrMatch ? 'PASS' : 'FAIL'}
          </span>
        </td>
        <td>
          <span className={`cot-proof-badge cot-proof-badge-${statusTone(row.overall_status)}`}>
            {row.overall_status}
          </span>
        </td>
        <td className="cot-proof-market-name">
          {row.first_divergence_layer ? `at ${row.first_divergence_layer}` : '—'}
          {row.failure_reasons?.[0] ? ` · ${row.failure_reasons[0]}` : ''}
        </td>
      </tr>
      {open ? (
        <tr className="cot-proof-detail-row">
          <td colSpan={8}>
            <div className="cot-proof-detail lineage-detail">
              <p className="wo-cot-meta-line">
                <strong>Chain:</strong> Instrument → Raw CFTC ({row.raw_source?.official_raw_source_file} row{' '}
                {row.raw_source?.official_raw_row_index}) → Mapping {row.mapping?.cftc_code} → Source Truth →
                Dashboard → Scanner → Thesis → Scoring
              </p>
              {row.failure_reasons?.length ? (
                <ul className="cot-proof-mismatch-list">
                  {row.failure_reasons.map((r) => (
                    <li key={r}>{r}</li>
                  ))}
                </ul>
              ) : (
                <p className="cot-integrity-pass">All layers use identical values for this report date.</p>
              )}
              <div className="lineage-layer-grid">
                {LAYER_ORDER.map((lid) => {
                  const L = layers[lid]
                  if (!L) return null
                  return (
                    <div key={lid} className="lineage-layer-card">
                      <h4>{lid}</h4>
                      <p className="wo-cot-hint-tight">
                        <code>{L.file_name}</code>
                        <br />
                        {L.dataset_name}
                        <br />
                        {L.row_source}
                        <br />
                        loaded: {L.mtime_utc || L.generated_at || '—'}
                      </p>
                      <ValuesBlock values={L.values} />
                    </div>
                  )
                })}
              </div>
              <table className="wo-cot-hist-table cot-integrity-table">
                <thead>
                  <tr>
                    <th>Check</th>
                    <th>Match</th>
                    <th>Differences</th>
                  </tr>
                </thead>
                <tbody>
                  {(row.chain_checks || []).map((c) => (
                    <tr key={`${c.from_layer}-${c.to_layer}`}>
                      <td>
                        {c.from_layer} → {c.to_layer}
                      </td>
                      <td>{c.match ? 'PASS' : 'FAIL'}</td>
                      <td>
                        {(c.differences || []).map((d) => (
                          <div key={d.field}>
                            {d.field}: expected {fmt(d.expected)} actual {fmt(d.actual)}
                          </div>
                        ))}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </td>
        </tr>
      ) : null}
    </>
  )
}

export function DataLineagePage({ sidebarClass, onSidebarClass }) {
  const [doc, setDoc] = React.useState(null)
  const [error, setError] = React.useState(null)
  const [loading, setLoading] = React.useState(true)
  const [filter, setFilter] = React.useState('all')

  React.useEffect(() => {
    let cancelled = false
    loadDataLineageLatest()
      .then((d) => {
        if (!cancelled) {
          setDoc(d)
          setError(null)
        }
      })
      .catch((e) => {
        if (!cancelled) setError(e?.message || 'Failed to load lineage')
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [])

  const rows = React.useMemo(() => lineageInstruments(doc), [doc])
  const filtered = React.useMemo(() => {
    if (filter === 'all') return rows
    return rows.filter((r) => r.overall_status === filter)
  }, [rows, filter])

  const summary = doc?.summary || {}

  return (
    <AppShell
      title="Data Lineage"
      subtitle="Source Truth → Dashboard → Scanner → Thesis → Scoring"
      sidebarClass={sidebarClass}
      onSidebarClass={onSidebarClass}
      topActions={
        <button type="button" className="ws-btn" onClick={navigateToScanner}>
          ← Scanner
        </button>
      }
    >
      <section className="cot-proof-page lineage-page">
        {loading ? <p className="ws-topbar-meta">Loading lineage audit…</p> : null}
        {error ? (
          <p className="ws-error-banner">
            {error}. Run: <code>python -m hptl.cot.run_data_lineage_audit</code>
          </p>
        ) : null}

        {doc && !error ? (
          <>
            <div className={`cot-proof-gate cot-proof-gate-${summary.all_layers_identical ? 'pass' : 'fail'}`}>
              <strong>{summary.all_layers_identical ? 'LINEAGE PASS' : 'LINEAGE FAIL'}</strong>
              <p>
                PASS requires every layer to carry the same NC/NR/score values for {doc.latest_report_date}. Source
                Truth PASS alone is not sufficient.
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
            </div>

            <div className="cot-proof-toolbar">
              <label>
                Filter
                <select className="ws-select" value={filter} onChange={(e) => setFilter(e.target.value)}>
                  <option value="all">All</option>
                  <option value="PASS">PASS only</option>
                  <option value="FAIL">FAIL only</option>
                </select>
              </label>
            </div>

            <div className="wo-cot-hist-table-wrap">
              <table className="wo-cot-hist-table cot-proof-table">
                <thead>
                  <tr>
                    <th>Instrument</th>
                    <th>Date</th>
                    <th>CFTC Code</th>
                    <th>Market Name</th>
                    <th>NC lineage</th>
                    <th>NR lineage</th>
                    <th>Status</th>
                    <th>First divergence</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((row) => (
                    <InstrumentLineageRow key={row.instrument} row={row} />
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
