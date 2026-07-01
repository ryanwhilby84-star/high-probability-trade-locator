import React from 'react'

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

const fmtExtreme = (v) => {
  if (v === true) return 'Yes'
  if (v === false || v == null) return '—'
  return String(v)
}

export function CommercialPositioningTable({ rows }) {
  if (!rows?.length) {
    return <p className="cot-chart-empty">No commercial COT rows.</p>
  }

  return (
    <div className="cot-raw-table-wrap">
      <table className="cot-raw-table">
        <thead>
          <tr>
            <th>report_date</th>
            <th>commercial_long</th>
            <th>commercial_short</th>
            <th>commercial_net</th>
            <th>weekly_change_net</th>
            <th>percentile</th>
            <th>extreme_flag</th>
          </tr>
        </thead>
        <tbody>
          {[...rows].reverse().map((row) => (
            <tr key={row.report_date}>
              <td>{row.report_date}</td>
              <td>{fmt(row.commercial_long)}</td>
              <td>{fmt(row.commercial_short)}</td>
              <td>{fmt(row.commercial_net)}</td>
              <td>{fmt(row.weekly_change_net)}</td>
              <td>{fmtPct(row.percentile)}</td>
              <td>{fmtExtreme(row.extreme_flag)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
