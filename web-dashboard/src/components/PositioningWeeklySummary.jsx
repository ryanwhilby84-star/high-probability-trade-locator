import React from 'react'
import { POSITIONING_SHEET_TABS } from '../cot/groupPositioningView.js'
import { buildRawRowsForGroup } from '../cot/rawCotPositioning.js'

const fmtDelta = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  const sign = n > 0 ? '+' : ''
  return `${sign}${n.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
}

function groupWeekChanges(instrumentData, groupId, asOfDate) {
  const rows = buildRawRowsForGroup(instrumentData, groupId, asOfDate)
  if (!rows.length) return null
  const latest = rows[rows.length - 1]
  return {
    long: latest.weekly_change_long,
    short: latest.weekly_change_short,
    net: latest.weekly_change_net,
    reportDate: latest.report_date,
  }
}

/** Compact weekly deltas for all three COT cohorts — raw numbers only. */
export function PositioningWeeklySummary({ instrumentData, asOfDate }) {
  const groups = React.useMemo(
    () =>
      POSITIONING_SHEET_TABS.map((tab) => ({
        ...tab,
        changes: groupWeekChanges(instrumentData, tab.id, asOfDate),
      })),
    [instrumentData, asOfDate],
  )

  const reportDate = groups.find((g) => g.changes?.reportDate)?.changes?.reportDate

  if (!groups.some((g) => g.changes)) {
    return (
      <p className="wo-cot-hint positioning-weekly-empty">
        No weekly change data in legacy COT archive for this instrument.
      </p>
    )
  }

  return (
    <section className="positioning-weekly-summary" aria-label="Weekly positioning changes">
      <div className="positioning-weekly-head">
        <h4 className="wo-cot-section-title">Weekly summary</h4>
        {reportDate ? <span className="positioning-weekly-asof">Week ending {reportDate}</span> : null}
      </div>
      <div className="positioning-weekly-grid">
        {groups.map((g) => (
          <div key={g.id} className={`positioning-weekly-col positioning-weekly-col--${g.id}`}>
            <span className="positioning-weekly-label">{g.label}</span>
            <div className="positioning-weekly-metrics">
              <span>
                <span className="positioning-weekly-k">Long</span>
                <span className="positioning-weekly-v">{fmtDelta(g.changes?.long)}</span>
              </span>
              <span>
                <span className="positioning-weekly-k">Short</span>
                <span className="positioning-weekly-v">{fmtDelta(g.changes?.short)}</span>
              </span>
              <span>
                <span className="positioning-weekly-k">Net</span>
                <span className="positioning-weekly-v">{fmtDelta(g.changes?.net)}</span>
              </span>
            </div>
          </div>
        ))}
      </div>
    </section>
  )
}
