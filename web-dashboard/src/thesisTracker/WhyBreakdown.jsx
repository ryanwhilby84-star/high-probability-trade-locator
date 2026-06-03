import React from 'react'
import { getOpportunity } from './opportunityModel.js'

export function WhyBreakdown({ thesis }) {
  const why = getOpportunity(thesis).why || []
  return (
    <section className="toe-why" aria-label="Why this score exists">
      <h4 className="toe-section-title">Why this score exists</h4>
      <ul className="toe-checks">
        {why.map((row) => (
          <li key={row.pillar} className={row.pass ? 'pass' : row.wired === false ? 'pending' : 'fail'}>
            <span className="toe-check-label">{row.label}</span>
            <span className="toe-check-verdict">
              {!row.wired ? 'PENDING' : row.pass ? 'PASS' : 'FAIL'}
            </span>
          </li>
        ))}
      </ul>
      <ul className="toe-why-lines">
        {why.map((row) => (
          <li key={`${row.pillar}-detail`}>
            <strong>{row.label}:</strong> {row.detail}
          </li>
        ))}
      </ul>
    </section>
  )
}
