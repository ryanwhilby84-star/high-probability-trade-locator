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

export function CotPositioningRawTable({ series, groupTitle }) {
  if (!series?.length) {
    return <p className="cot-chart-empty">No weekly rows to display.</p>
  }
  return (
    <div className="cot-raw-table-wrap">
      <table className="cot-raw-table">
        <caption className="cot-raw-caption">{groupTitle} — weekly COT fields</caption>
        <thead>
          <tr>
            <th>Week</th>
            <th>Long</th>
            <th>Short</th>
            <th>Long Δ</th>
            <th>Short Δ</th>
            <th>Net</th>
            <th>Net Δ</th>
            <th>% long</th>
            <th>% short</th>
          </tr>
        </thead>
        <tbody>
          {[...series].reverse().map((row, index, newestFirst) => { 
            const previous = newestFirst[index + 1]
            const change = (key) => Number.isFinite(Number(row[key])) && row[key] != null && previous?.[key] != null && Number.isFinite(Number(previous[key])) ? Number(row[key]) - Number(previous[key]) : null
            return (
            <tr key={row.cotDate || row.label}>
              <td>{row.label}</td>
              <td>{fmt(row.long)}</td>
              <td>{fmt(row.short)}</td>
              <td>{fmt(change('long'))}</td>
              <td>{fmt(change('short'))}</td>
              <td>{fmt(row.net)}</td>
              <td>{fmt(change('net'))}</td>
              <td>{fmtPct(row.pctLong)}</td>
              <td>{fmtPct(row.pctShort)}</td>
            </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
