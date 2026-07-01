import React from 'react'

const fmt = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  if (Number.isInteger(n)) return n.toLocaleString()
  return n.toFixed(1)
}

const fmtDiv = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  const sign = n > 0 ? '+' : ''
  return `${sign}${n.toFixed(1)}`
}

export function CommercialStrengthResearchTable({ rows }) {
  if (!rows?.length) {
    return <p className="cot-chart-empty">No commercial strength research rows.</p>
  }

  return (
    <div className="cot-raw-table-wrap">
      <table className="cot-raw-table comm-research-table">
        <thead>
          <tr>
            <th>Currency</th>
            <th>Spec Strength</th>
            <th>Commercial Strength</th>
            <th>Divergence</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.currency}>
              <th scope="row">{row.currency}</th>
              <td>{fmt(row.spec_strength)}</td>
              <td>{fmt(row.commercial_strength)}</td>
              <td>{fmtDiv(row.divergence)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
