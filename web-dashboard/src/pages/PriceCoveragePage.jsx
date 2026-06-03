import React from 'react'
import { AppShell } from '../components/AppShell.jsx'
import { usePriceCoverageData } from '../hooks/usePriceCoverageData.js'
import { navigateToScanner } from '../routing.js'

function SourceEvidence({ src }) {
  if (!src) return null
  return (
    <div className="pcov-src">
      <div className="pcov-src-head">
        <strong>{src.source}</strong>
        <span className={`pcov-badge pcov-badge--${src.coverage_status}`}>{src.coverage_status}</span>
      </div>
      <dl className="pcov-dl">
        <dt>Symbol</dt>
        <dd className="mcat-mono">{src.symbol || '—'}</dd>
        <dt>Endpoint</dt>
        <dd className="mcat-mono pcov-endpoint">{src.endpoint || '—'}</dd>
        {src.function ? (
          <>
            <dt>Function</dt>
            <dd className="mcat-mono">{src.function}</dd>
          </>
        ) : null}
        <dt>Last OK</dt>
        <dd className="mcat-mono">{(src.last_successful_response || '—').slice(0, 19).replace('T', ' ')}</dd>
      </dl>
    </div>
  )
}

function InstrumentEvidenceTable({ rows, filter }) {
  const list = (rows || []).filter((r) => !filter || filter(r))
  if (!list.length) return <p className="mcat-empty">None</p>
  return (
    <table className="mcat-table pcov-table">
      <thead>
        <tr>
          <th>Instrument</th>
          <th>Overall</th>
          <th>OANDA</th>
          <th>Alpha Vantage</th>
        </tr>
      </thead>
      <tbody>
        {list.map((r) => {
          const oanda = (r.sources || []).find((s) => s.source === 'oanda')
          const alpha = (r.sources || []).find((s) => s.source === 'alpha_vantage')
          return (
            <tr key={r.htpl_instrument_id}>
              <td>
                <strong>{r.friendly_name}</strong>
                <div className="pcov-sub">{r.htpl_instrument_id}</div>
              </td>
              <td>
                <span className={`pcov-badge pcov-badge--${r.coverage_status}`}>{r.coverage_status}</span>
              </td>
              <td className="mcat-mono">{oanda?.symbol || '—'}</td>
              <td className="mcat-mono">{alpha?.symbol || alpha?.function || '—'}</td>
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}

export function PriceCoveragePage({ sidebarClass, onSidebarClass }) {
  const { data, loading, error } = usePriceCoverageData()
  const [expanded, setExpanded] = React.useState(null)

  const s = data?.summary ?? {}
  const instruments = Array.isArray(data?.instruments) ? data.instruments : []
  const oandaSupported = Array.isArray(data?.oanda_supported) ? data.oanda_supported : []
  const alphaSupported = Array.isArray(data?.alpha_supported) ? data.alpha_supported : []
  const bothSupported = Array.isArray(data?.supported_by_both) ? data.supported_by_both : []
  const unsupportedList = Array.isArray(data?.unsupported) ? data.unsupported : []

  const oandaIds = new Set(oandaSupported)
  const alphaIds = new Set(alphaSupported)
  const bothIds = new Set(bothSupported)
  const missingIds = new Set(unsupportedList)

  const oandaOnly = instruments.filter((r) => oandaIds.has(r.htpl_instrument_id) && !bothIds.has(r.htpl_instrument_id))
  const alphaOnly = instruments.filter((r) => alphaIds.has(r.htpl_instrument_id) && !bothIds.has(r.htpl_instrument_id))

  return (
    <AppShell
      title="Price Coverage Audit"
      subtitle="Live OANDA + Alpha Vantage vs HTPL registry"
      date=""
      dates={[]}
      onDateChange={() => {}}
      sidebarClass={sidebarClass}
      onSidebarClass={onSidebarClass}
      topActions={
        <button type="button" className="ws-btn" onClick={navigateToScanner}>
          Scanner
        </button>
      }
    >
      <div className="pcov-page">
        {loading && <p className="ws-loading-inline">Loading audit…</p>}
        {!loading && error && (
          <div className="pcov-alert">
            <p>
              <strong>Audit not loaded.</strong> {error}
            </p>
            <p className="pcov-hint">
              Run: <code className="mcat-mono">python -m hptl.prices.run_price_coverage_audit</code>
            </p>
          </div>
        )}
        {!loading && data && (
          <>
            <div className="pcov-stats">
              <div className="pcov-stat pcov-stat-oanda">
                <span className="pcov-stat-n">{s?.oanda_supported_count ?? 0}</span>
                <span className="pcov-stat-l">OANDA supported</span>
              </div>
              <div className="pcov-stat pcov-stat-alpha">
                <span className="pcov-stat-n">{s?.alpha_supported_count ?? 0}</span>
                <span className="pcov-stat-l">Alpha Vantage supported</span>
              </div>
              <div className="pcov-stat pcov-stat-both">
                <span className="pcov-stat-n">{s?.supported_by_both_count ?? 0}</span>
                <span className="pcov-stat-l">Both sources</span>
              </div>
              <div className="pcov-stat pcov-stat-miss">
                <span className="pcov-stat-n">{s?.unsupported_count ?? 0}</span>
                <span className="pcov-stat-l">Missing (neither)</span>
              </div>
            </div>

            <section className="pcov-section">
              <h3>OANDA supported</h3>
              <ul className="pcov-bullets">
                {oandaSupported.map((id) => {
                  const row = instruments.find((r) => r.htpl_instrument_id === id)
                  return (
                    <li key={id}>
                      {row?.friendly_name || id}
                    </li>
                  )
                })}
              </ul>
            </section>

            <section className="pcov-section">
              <h3>Alpha Vantage supported</h3>
              <ul className="pcov-bullets">
                {alphaSupported.map((id) => {
                  const row = instruments.find((r) => r.htpl_instrument_id === id)
                  return (
                    <li key={id}>
                      {row?.friendly_name || id}
                    </li>
                  )
                })}
              </ul>
            </section>

            <section className="pcov-section">
              <h3>Supported by both</h3>
              <InstrumentEvidenceTable
                rows={instruments.filter((r) => bothIds.has(r.htpl_instrument_id))}
              />
            </section>

            <section className="pcov-section">
              <h3>OANDA only ({oandaOnly.length})</h3>
              <InstrumentEvidenceTable rows={oandaOnly} />
            </section>

            <section className="pcov-section">
              <h3>Alpha Vantage only ({alphaOnly.length})</h3>
              <InstrumentEvidenceTable rows={alphaOnly} />
            </section>

            <section className="pcov-section">
              <h3>Missing instruments</h3>
              <InstrumentEvidenceTable rows={instruments.filter((r) => missingIds.has(r.htpl_instrument_id))} />
            </section>

            <section className="pcov-section">
              <h3>Per-instrument evidence</h3>
              <p className="pcov-hint">Click a row to expand source, symbol, endpoint, and last successful response.</p>
              <div className="pcov-evidence-list">
                {instruments.map((r) => (
                  <div key={r.htpl_instrument_id || r.friendly_name} className="pcov-evidence-item">
                    <button
                      type="button"
                      className="pcov-evidence-btn"
                      onClick={() =>
                        setExpanded(expanded === r.htpl_instrument_id ? null : r.htpl_instrument_id)
                      }
                    >
                      <span>{r.friendly_name || r.htpl_instrument_id || '—'}</span>
                      <span className={`pcov-badge pcov-badge--${r.coverage_status}`}>{r.coverage_status}</span>
                    </button>
                    {expanded === r.htpl_instrument_id && (
                      <div className="pcov-evidence-detail">
                        {(Array.isArray(r.sources) ? r.sources : []).map((src) => (
                          <SourceEvidence key={src?.source || src?.endpoint} src={src} />
                        ))}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </section>
          </>
        )}
      </div>
    </AppShell>
  )
}
