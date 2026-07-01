import React from 'react'
import {
  buildRawCotHeatRanges,
  longLevelHeat,
  netLevelHeat,
  shortLevelHeat,
  signedDeltaHeat,
  totalOiLevelHeat,
} from '../cot/cotHeatmap.js'

const fmtNum = (v) => {
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

/** Full legacy COT archive spreadsheet — newest row first. */
export function CotRawDataTable({ rows, groupLabel }) {
  const sorted = React.useMemo(
    () => [...(rows || [])].sort((a, b) => String(b.report_date).localeCompare(String(a.report_date))),
    [rows],
  )

  const heat = React.useMemo(() => buildRawCotHeatRanges(sorted), [sorted])

  if (!sorted.length) {
    return (
      <p className="wo-cot-hint" style={{ marginTop: '8px' }}>
        No {groupLabel || 'COT'} weekly rows in legacy_cot_latest.json for this instrument.
      </p>
    )
  }

  return (
    <div className="cot-raw-data-spreadsheet">
      <div className="wo-cot-table-wrap cot-raw-data-table-scroll">
        <table className="wo-cot-table wo-cot-table-participation cot-raw-table">
          <thead>
            <tr>
              <th>Date</th>
              <th>Long</th>
              <th>Short</th>
              <th title="Change in longs vs prior COT week">Long Wk</th>
              <th title="Change in shorts vs prior COT week">Short Wk</th>
              <th>Total OI</th>
              <th>% Long</th>
              <th>% Short</th>
              <th>Net</th>
              <th title="Net change vs prior COT week">Net Wk</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((r) => (
              <tr key={r.report_date} className="wo-cot-data-row">
                <td className="wo-cot-date">{r.report_date}</td>
                <td className={longLevelHeat(r.long, heat.long.min, heat.long.max).className}>{fmtNum(r.long)}</td>
                <td className={shortLevelHeat(r.short, heat.short.min, heat.short.max).className}>{fmtNum(r.short)}</td>
                <td className={signedDeltaHeat(r.weekly_change_long, heat.longDeltas).className}>
                  {fmtDelta(r.weekly_change_long)}
                </td>
                <td className={signedDeltaHeat(r.weekly_change_short, heat.shortDeltas, true).className}>
                  {fmtDelta(r.weekly_change_short)}
                </td>
                <td className={totalOiLevelHeat(r.open_interest, heat.oi.min, heat.oi.max).className}>
                  {fmtNum(r.open_interest)}
                </td>
                <td>{fmtPct(r.percent_long)}</td>
                <td>{fmtPct(r.percent_short)}</td>
                <td className={`wo-cot-net-col ${netLevelHeat(r.net, heat.net.min, heat.net.max).className}`}>
                  {fmtNum(r.net)}
                </td>
                <td className={signedDeltaHeat(r.weekly_change_net, heat.netDeltas).className}>
                  {fmtDelta(r.weekly_change_net)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="wo-cot-hint wo-cot-hint-tight" style={{ marginTop: '8px' }}>
        {sorted.length} weekly reports · source legacy_cot_latest.json · newest at top
      </p>
    </div>
  )
}
