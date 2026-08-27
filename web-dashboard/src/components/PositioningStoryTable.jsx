import React from 'react'

const fmt = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  if (Number.isInteger(n)) return n.toLocaleString()
  return n.toFixed(1)
}

const fmtDelta = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  const sign = n > 0 ? '+' : ''
  return `${sign}${n.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
}

export function PositioningStoryTable({ rows }) {
  if (!rows?.length) {
    return <p className="cot-chart-empty">No positioning story rows.</p>
  }

  return (
    <div className="cot-raw-table-wrap comm-research-table-wrap">
      <table className="cot-raw-table comm-research-table positioning-story-table">
        <thead>
          <tr>
            <th>Currency</th>
            <th>Story state</th>
            <th>Story score</th>
            <th>Commercial current</th>
            <th>Commercial 13W change</th>
            <th>Non-commercial current</th>
            <th>Non-commercial 13W change</th>
            <th>Explanation</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.currency}>
              <th scope="row">{row.currency}</th>
              <td>{row.story_state}</td>
              <td>{fmt(row.story_score)}</td>
              <td>{fmt(row.commercial_current_score)}</td>
              <td>{fmtDelta(row.commercial_change_13w)}</td>
              <td>{fmt(row.noncommercial_current_score)}</td>
              <td>{fmtDelta(row.noncommercial_change_13w)}</td>
              <td className="positioning-story-explanation">{row.explanation || '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
