import React from 'react'
import { COT_INTEGRITY_TABS } from '../cotGroupsData.js'
import { useCotGroups } from '../hooks/useCotGroups.js'

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

function GroupHistoryTable({ weeks, title, subtitle }) {
  const displayWeeks = [...(weeks || [])].reverse().slice(0, 13)
  if (!displayWeeks.length) {
    return <p className="cot-integrity-empty">No COT weeks available for this group.</p>
  }
  return (
    <div className="cot-integrity-block">
      <h4 className="wo-cot-section-title">{title}</h4>
      {subtitle ? <p className="wo-cot-hint wo-cot-hint-tight">{subtitle}</p> : null}
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
              <th>% long</th>
              <th>% short</th>
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
                <td>{fmt(w.total_open_interest)}</td>
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
  if (!displayWeeks.length) {
    return <p className="cot-integrity-empty">No combined weeks available.</p>
  }
  return (
    <div className="wo-cot-hist-table-wrap">
      <table className="wo-cot-hist-table cot-integrity-table">
        <thead>
          <tr>
            <th>Report date</th>
            <th>Inst. net</th>
            <th>Comm. net</th>
            <th>Retail net</th>
            <th>Open interest</th>
          </tr>
        </thead>
        <tbody>
          {displayWeeks.map((w) => (
            <tr key={w.report_date}>
              <th scope="row">{w.report_date}</th>
              <td>{fmt(w.institutions_net)}</td>
              <td>{fmt(w.commercials_net)}</td>
              <td>{fmt(w.retail_proxy_net)}</td>
              <td>{fmt(w.total_open_interest)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="wo-cot-hint wo-cot-hint-tight" style={{ marginTop: 8 }}>
        Combined view lists each cohort separately — groups are not merged into a single score.
      </p>
    </div>
  )
}

function AuditProofTable({ audit, contractMeta }) {
  const checks = audit?.checks || []
  if (!checks.length) {
    return <p className="cot-integrity-empty">No audit checks for this instrument.</p>
  }
  return (
    <div className="cot-integrity-block">
      <p className="wo-cot-meta-line">
        <strong>Contract:</strong> {contractMeta?.cftc_market_name || '—'}{' '}
        <span className="ws-topbar-meta">({contractMeta?.cftc_market_code || '—'})</span>
        {' · '}
        <strong>Report type:</strong> {contractMeta?.report_type || audit?.report_type || '—'}
        {' · '}
        <strong>Audit week:</strong> {audit?.report_date || '—'}
      </p>
      {audit?.hptl_headline_audit_pass === true ? (
        <p className="cot-integrity-pass">HPTL headline positioning matches raw managed-money CFTC fields.</p>
      ) : audit?.hptl_headline_audit_pass === false ? (
        <p className="cot-integrity-fail">
          HPTL headline positioning does not match raw managed-money fields — use Institutions tab for audited
          values.
        </p>
      ) : null}
      <div className="wo-cot-hist-table-wrap">
        <table className="wo-cot-hist-table cot-integrity-table cot-integrity-audit">
          <thead>
            <tr>
              <th>Group</th>
              <th>Field</th>
              <th>HPTL value</th>
              <th>Raw CFTC</th>
              <th>Match</th>
              <th>Diff</th>
              <th>Source file</th>
              <th>Row</th>
              <th>CFTC code</th>
              <th>Category</th>
            </tr>
          </thead>
          <tbody>
            {checks.map((c, i) => (
              <tr key={`${c.group}-${c.field}-${c.audit_kind || 'raw'}-${i}`}>
                <td>{c.group}</td>
                <td>{c.field}</td>
                <td>{fmt(c.hptl_value ?? c.integrity_value)}</td>
                <td>{fmt(c.raw_cftc_value)}</td>
                <td>{c.match === true ? '✓' : c.match === false ? '✗' : '—'}</td>
                <td>{fmt(c.difference)}</td>
                <td>{c.source_file || '—'}</td>
                <td>{c.raw_source_row ?? '—'}</td>
                <td>{c.cftc_market_code || '—'}</td>
                <td>{c.trader_category || '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export function CotIntegrityPanel({ instrumentId }) {
  const { instrumentGroups, instrumentAudit, loading, error } = useCotGroups(instrumentId)
  const [activeTab, setActiveTab] = React.useState('institutions')

  if (loading) {
    return (
      <section className="cot-integrity-panel ws-placeholder-panel">
        <p className="ws-topbar-meta">Loading audited COT groups…</p>
      </section>
    )
  }

  if (error) {
    return (
      <section className="cot-integrity-panel ws-placeholder-panel">
        <p className="ws-error-banner">{error}</p>
      </section>
    )
  }

  if (!instrumentGroups?.groups) {
    return (
      <section className="cot-integrity-panel ws-placeholder-panel">
        <h3 className="wo-cot-title">COT integrity layer</h3>
        <p className="wo-cot-hint">
          No audited group data for {instrumentId}. Run:{' '}
          <code>python -m hptl.cot.run_cot_groups_integrity</code>
        </p>
      </section>
    )
  }

  const groups = instrumentGroups.groups
  const contractMeta = {
    cftc_market_code: instrumentGroups.cftc_market_code,
    cftc_market_name: instrumentGroups.cftc_market_name,
    report_type: instrumentGroups.report_type,
  }

  return (
    <section className="cot-integrity-panel">
      <div className="wo-cot-header wo-cot-header-compact">
        <h3 className="wo-cot-title">COT integrity layer</h3>
        <p className="wo-cot-sub">
          {instrumentGroups.cftc_market_name || instrumentId} · {instrumentGroups.report_type} · code{' '}
          {instrumentGroups.cftc_market_code || '—'}
        </p>
        <p className="wo-cot-hint wo-cot-hint-tight">
          Institutions = managed money / leveraged funds only. Spreaders and other reportables are excluded from
          main tabs.
        </p>
      </div>

      <div className="cot-integrity-tabs" role="tablist">
        {COT_INTEGRITY_TABS.map((tab) => (
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
        {activeTab === 'institutions' && (
          <GroupHistoryTable
            weeks={groups.institutions?.weeks}
            title={groups.institutions?.tab_label || 'Institutions'}
            subtitle={groups.institutions?.trader_group_name}
          />
        )}
        {activeTab === 'commercials' && (
          <GroupHistoryTable
            weeks={groups.commercials?.weeks}
            title={groups.commercials?.tab_label || 'Commercials'}
            subtitle={groups.commercials?.trader_group_name}
          />
        )}
        {activeTab === 'retail_proxy' && (
          <GroupHistoryTable
            weeks={groups.retail_proxy?.weeks}
            title={groups.retail_proxy?.tab_label || 'Retail Proxy'}
            subtitle={groups.retail_proxy?.trader_group_name}
          />
        )}
        {activeTab === 'combined' && (
          <>
            <h4 className="wo-cot-section-title">Combined (separate cohorts)</h4>
            <CombinedTable weeks={groups.combined?.weeks} />
          </>
        )}
        {activeTab === 'audit' && <AuditProofTable audit={instrumentAudit} contractMeta={contractMeta} />}
      </div>
    </section>
  )
}
