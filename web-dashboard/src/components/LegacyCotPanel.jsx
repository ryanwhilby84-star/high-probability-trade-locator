import React from 'react'
import { LEGACY_COT_TABS } from '../legacyCotData.js'
import { useLegacyCot } from '../hooks/useLegacyCot.js'

const fmt = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return n.toLocaleString(undefined, { maximumFractionDigits: 0 })
}

const fmtPct = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return `${n.toFixed(1)}%`
}

const fmtDelta = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  const sign = n > 0 ? '+' : ''
  return `${sign}${n.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
}

function GroupHistoryTable({ weeks, title }) {
  const displayWeeks = [...(weeks || [])].reverse().slice(0, 13)
  if (!displayWeeks.length) {
    return <p className="cot-integrity-empty">No Legacy COT weeks for this group.</p>
  }
  return (
    <div className="cot-integrity-block">
      <h4 className="wo-cot-section-title">{title}</h4>
      <p className="wo-cot-hint wo-cot-hint-tight">
        Raw Legacy Futures Only — no TFF, no leveraged-money, no derived scores.
      </p>
      <div className="wo-cot-hist-table-wrap">
        <table className="wo-cot-hist-table cot-integrity-table">
          <thead>
            <tr>
              <th>Report date</th>
              <th>Long</th>
              <th>Short</th>
              <th>Net</th>
              <th>Δ long (1w)</th>
              <th>Δ short (1w)</th>
              <th>Δ net (1w)</th>
              <th>% long (OI)</th>
              <th>% short (OI)</th>
              <th>Open interest</th>
            </tr>
          </thead>
          <tbody>
            {displayWeeks.map((w) => (
              <tr key={w.report_date}>
                <th scope="row">{w.report_date}</th>
                <td>{fmt(w.long)}</td>
                <td>{fmt(w.short)}</td>
                <td className="wo-cot-net-col">{fmt(w.net)}</td>
                <td>{fmtDelta(w.long_week_change)}</td>
                <td>{fmtDelta(w.short_week_change)}</td>
                <td>{fmtDelta(w.net_week_change)}</td>
                <td>{fmtPct(w.percent_long)}</td>
                <td>{fmtPct(w.percent_short)}</td>
                <td>{fmt(w.open_interest)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function CombinedTable({ weeks }) {
  const displayWeeks = [...(weeks || [])].reverse().slice(0, 13)
  if (!displayWeeks.length) return <p className="cot-integrity-empty">No combined weeks.</p>
  return (
    <div className="wo-cot-hist-table-wrap">
      <table className="wo-cot-hist-table cot-integrity-table">
        <thead>
          <tr>
            <th>Report date</th>
            <th>Non-comm. net</th>
            <th>Commercial net</th>
            <th>Non-rept. net</th>
            <th>Open interest</th>
          </tr>
        </thead>
        <tbody>
          {displayWeeks.map((w) => (
            <tr key={w.report_date}>
              <th scope="row">{w.report_date}</th>
              <td>{fmt(w.noncommercials_net)}</td>
              <td>{fmt(w.commercials_net)}</td>
              <td>{fmt(w.nonreportables_net)}</td>
              <td>{fmt(w.open_interest)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function AuditTable({ audit, meta }) {
  const checks = audit?.checks || []
  if (!checks.length) return <p className="cot-integrity-empty">No audit checks.</p>
  return (
    <div className="cot-integrity-block">
      <p className="wo-cot-meta-line">
        <strong>Contract:</strong> {meta?.selected_market_name || '—'} ({meta?.selected_cftc_code || '—'})
        {' · '}
        <strong>Report type:</strong> legacy_futures_only
        {' · '}
        <strong>Week:</strong> {audit?.report_date || '—'}
        {' · '}
        <strong>Mapping:</strong> {meta?.mapping_status || '—'}
      </p>
      {meta?.mapping_status === 'PASS' ? (
        <p className="cot-integrity-pass">Dashboard values equal raw Legacy CFTC fields (audit pass).</p>
      ) : (
        <p className="cot-integrity-fail">
          Instrument excluded from scoring until mapping status is PASS.
        </p>
      )}
      <div className="wo-cot-hist-table-wrap">
        <table className="wo-cot-hist-table cot-integrity-table cot-integrity-audit">
          <thead>
            <tr>
              <th>Group</th>
              <th>Field</th>
              <th>Dashboard value</th>
              <th>Raw CFTC value</th>
              <th>Match</th>
              <th>Diff</th>
              <th>Source file</th>
              <th>Row</th>
              <th>CFTC code</th>
              <th>Market name</th>
              <th>Report type</th>
            </tr>
          </thead>
          <tbody>
            {checks.map((c, i) => (
              <tr key={`${c.group}-${c.field}-${i}`}>
                <td>{c.group}</td>
                <td>{c.field}</td>
                <td>{fmt(c.dashboard_value)}</td>
                <td>{fmt(c.raw_cftc_value)}</td>
                <td>{c.match === true ? '✓' : c.match === false ? '✗' : '—'}</td>
                <td>{fmt(c.difference)}</td>
                <td>{c.source_file || '—'}</td>
                <td>{c.raw_source_row ?? '—'}</td>
                <td>{c.cftc_market_code || '—'}</td>
                <td>{c.market_name || '—'}</td>
                <td>{c.report_type || '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export function LegacyCotPanel({ instrumentId }) {
  const { instrumentData, instrumentAudit, scoringEligible, loading, error } = useLegacyCot(instrumentId)
  const [activeTab, setActiveTab] = React.useState('noncommercials')

  if (loading) {
    return (
      <section className="cot-integrity-panel ws-placeholder-panel">
        <p className="ws-topbar-meta">Loading Legacy COT…</p>
      </section>
    )
  }

  if (error) {
    return (
      <section className="cot-integrity-panel">
        <p className="ws-error-banner">{error}</p>
      </section>
    )
  }

  if (!instrumentData?.groups) {
    return (
      <section className="cot-integrity-panel ws-placeholder-panel">
        <h3 className="wo-cot-title">Legacy COT positioning</h3>
        <p className="wo-cot-hint">
          No Legacy COT data for {instrumentId}. Run:{' '}
          <code>python -m hptl.cot.run_legacy_cot</code>
        </p>
      </section>
    )
  }

  const groups = instrumentData.groups
  const meta = {
    selected_cftc_code: instrumentData.selected_cftc_code,
    selected_market_name: instrumentData.selected_market_name,
    mapping_status: instrumentData.mapping_status,
  }

  return (
    <section className="cot-integrity-panel">
      <div className="wo-cot-header wo-cot-header-compact">
        <h3 className="wo-cot-title">Legacy COT positioning</h3>
        <p className="wo-cot-sub">
          {instrumentData.selected_market_name} · code {instrumentData.selected_cftc_code} ·{' '}
          {instrumentData.selected_report_type}
        </p>
        <p className="wo-cot-hint wo-cot-hint-tight">
          Single source of truth: Legacy Futures Only. Non-Commercial / Commercial / Non-Reportable.
          {scoringEligible ? ' · Eligible for scoring when layer audit passes globally.' : ' · Not scoring-eligible (mapping not PASS).'}
        </p>
      </div>

      <div className="cot-integrity-tabs" role="tablist">
        {LEGACY_COT_TABS.map((tab) => (
          <button
            key={tab.id}
            type="button"
            role="tab"
            aria-selected={activeTab === tab.id}
            className={`cot-integrity-tab${activeTab === tab.id ? ' cot-integrity-tab-active' : ''}`}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      <div className="cot-integrity-tab-panel" role="tabpanel">
        {activeTab === 'noncommercials' && (
          <GroupHistoryTable weeks={groups.noncommercials?.weeks} title="Non-Commercials" />
        )}
        {activeTab === 'commercials' && (
          <GroupHistoryTable weeks={groups.commercials?.weeks} title="Commercials" />
        )}
        {activeTab === 'nonreportables' && (
          <GroupHistoryTable weeks={groups.nonreportables?.weeks} title="Non-Reportables" />
        )}
        {activeTab === 'combined' && (
          <>
            <h4 className="wo-cot-section-title">Combined (separate cohorts)</h4>
            <CombinedTable weeks={groups.combined?.weeks} />
          </>
        )}
        {activeTab === 'audit' && <AuditTable audit={instrumentAudit} meta={meta} />}
      </div>
    </section>
  )
}
