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
  const cellStyle = { color: "#f1f5f9", background: "transparent", fontWeight: 500 }
  const deltaStyle = (v) => ({ ...cellStyle, color: Number(v) > 0 ? "#57e3ad" : Number(v) < 0 ? "#ff929a" : "#f1f5f9" })

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
        <table className="wo-cot-table wo-cot-table-participation cot-raw-table" style={{ background: "#111c27", color: "#f1f5f9" }}>
          <th style={{ color: "#f8fafc", background: "#263747" }}ead>
            <tr>
              <th style={{ color: "#f8fafc", background: "#263747" }}>Date</th>
              <th style={{ color: "#f8fafc", background: "#263747" }}>Long</th>
              <th style={{ color: "#f8fafc", background: "#263747" }}>Short</th>
              <th style={{ color: "#f8fafc", background: "#263747" }} title="Change in longs vs prior COT week">Long Wk</th>
              <th style={{ color: "#f8fafc", background: "#263747" }} title="Change in shorts vs prior COT week">Short Wk</th>
              <th style={{ color: "#f8fafc", background: "#263747" }}>Total OI</th>
              <th style={{ color: "#f8fafc", background: "#263747" }}>% Long</th>
              <th style={{ color: "#f8fafc", background: "#263747" }}>% Short</th>
              <th style={{ color: "#f8fafc", background: "#263747" }}>Net</th>
              <th style={{ color: "#f8fafc", background: "#263747" }} title="Net change vs prior COT week">Net Wk</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((r) => (
              <tr key={r.report_date} className="wo-cot-data-row" style={{ background: sorted.indexOf(r) % 2 ? "#1b2937" : "#111c27" }}>
                <td className="wo-cot-date" style={cellStyle}>{r.report_date}</td>
                <td style={cellStyle}>{fmtNum(r.long)}</td>
                <td style={cellStyle}>{fmtNum(r.short)}</td>
                <td style={deltaStyle(r.weekly_change_long)}>
                  {fmtDelta(r.weekly_change_long)}
                </td>
                <td style={deltaStyle(r.weekly_change_short)}>
                  {fmtDelta(r.weekly_change_short)}
                </td>
                <td style={cellStyle}>
                  {fmtNum(r.open_interest)}
                </td>
                <td style={cellStyle}>{fmtPct(r.percent_long)}</td>
                <td style={cellStyle}>{fmtPct(r.percent_short)}</td>
                <td className="wo-cot-net-col" style={cellStyle}>
                  {fmtNum(r.net)}
                </td>
                <td style={deltaStyle(r.weekly_change_net)}>
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
