import React from 'react'
import { AppShell } from '../components/AppShell.jsx'
import { useOandaCoverageData } from '../hooks/useOandaCoverageData.js'
import { navigateToScanner } from '../routing.js'

function MappingTable({ rows, showReason }) {
  if (!rows?.length) {
    return <p className="mcat-empty">None</p>
  }
  return (
    <table className="mcat-table oanda-cov-table">
      <thead>
        <tr>
          <th>HTPL instrument</th>
          <th>OANDA symbol</th>
          <th>Mapping</th>
          {showReason ? <th>Reason</th> : null}
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={r.htpl_instrument_id}>
            <td>
              <strong>{r.friendly_name}</strong>
              <div className="oanda-cov-sub">{r.htpl_instrument_id}</div>
            </td>
            <td className="mcat-mono">{r.resolved_oanda_symbol || r.registry_oanda_symbol || '—'}</td>
            <td className="mcat-mono">{r.mapping_source}</td>
            {showReason ? <td>{r.unsupported_reason || '—'}</td> : null}
          </tr>
        ))}
      </tbody>
    </table>
  )
}

export function OandaCoveragePage({ sidebarClass, onSidebarClass }) {
  const { data, loading, error } = useOandaCoverageData()

  const summary = data?.summary
  const supported = data?.supported || []
  const unsupported = data?.unsupported || []

  return (
    <AppShell
      title="OANDA coverage"
      subtitle="Live v20 instruments vs HTPL registry"
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
      <div className="oanda-cov-page">
        {loading && <p className="ws-loading-inline">Loading audit…</p>}
        {!loading && error && (
          <div className="oanda-cov-alert">
            <p>
              <strong>Audit not loaded.</strong> {error}
            </p>
            <p className="oanda-cov-hint">
              Run: <code className="mcat-mono">python -m hptl.oanda.run_oanda_coverage_audit</code> with{' '}
              <code className="mcat-mono">OANDA_API_KEY</code> in <code className="mcat-mono">.env</code>
            </p>
          </div>
        )}
        {!loading && data && (
          <>
            <div className="oanda-cov-stats">
              <div className="oanda-cov-stat oanda-cov-stat-ok">
                <span className="oanda-cov-stat-n">{summary?.supported_count ?? 0}</span>
                <span className="oanda-cov-stat-l">Supported</span>
              </div>
              <div className="oanda-cov-stat oanda-cov-stat-no">
                <span className="oanda-cov-stat-n">{summary?.unsupported_count ?? 0}</span>
                <span className="oanda-cov-stat-l">Unsupported</span>
              </div>
              <div className="oanda-cov-stat">
                <span className="oanda-cov-stat-n">{data.oanda_instruments_on_account ?? '—'}</span>
                <span className="oanda-cov-stat-l">OANDA instruments (account)</span>
              </div>
            </div>
            <p className="oanda-cov-meta">
              Account <span className="mcat-mono">{data.oanda_account_id}</span> · Host{' '}
              <span className="mcat-mono">{data.oanda_api_host}</span> · Generated{' '}
              {(data.generated_at || '').slice(0, 19).replace('T', ' ')} UTC
            </p>

            <section className="oanda-cov-section">
              <h3>Supported</h3>
              <ul className="oanda-cov-bullets">
                {supported.map((r) => (
                  <li key={r.htpl_instrument_id}>
                    {r.friendly_name}
                    <span className="mcat-mono oanda-cov-sym"> → {r.resolved_oanda_symbol}</span>
                  </li>
                ))}
              </ul>
            </section>

            <section className="oanda-cov-section">
              <h3>Unsupported</h3>
              <ul className="oanda-cov-bullets oanda-cov-bullets-unsupported">
                {unsupported.map((r) => (
                  <li key={r.htpl_instrument_id}>
                    {r.friendly_name}
                    <span className="oanda-cov-reason"> ({r.unsupported_reason})</span>
                  </li>
                ))}
              </ul>
            </section>

            <section className="oanda-cov-section">
              <h3>Exact mappings (supported)</h3>
              <MappingTable rows={supported} showReason={false} />
            </section>

            <section className="oanda-cov-section">
              <h3>Exact mappings (unsupported)</h3>
              <MappingTable rows={unsupported} showReason />
            </section>
          </>
        )}
      </div>
    </AppShell>
  )
}
