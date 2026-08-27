import React from 'react'
import { navigateToInstrument } from '../routing.js'

function stateToneClass(stateId) {
  const id = String(stateId || '')
  if (id.includes('CROWDED') || id.includes('EXT')) return 'market-state-extreme'
  if (id.includes('LIQ') || id.includes('DIST')) return 'market-state-caution'
  if (id.includes('ACCUM') || id.includes('EXP') || id.includes('ALIGN')) return 'market-state-setup'
  return 'market-state-neutral'
}

export function MarketStatesPanel({ attentionList, loading, error, calendarWeek, limit = 12 }) {
  const items = (attentionList || []).slice(0, limit)

  return (
    <section className="market-states-panel" aria-label="Markets deserving attention">
      <div className="market-states-header">
        <div>
          <h2 className="market-states-title">What deserves my attention this week?</h2>
          <p className="market-states-lede">
            Unusual positioning conditions across commercials, specs, and non-reportables — evidence first, no composite
            rankings.
          </p>
        </div>
        {calendarWeek ? <span className="market-states-week">Week {calendarWeek}</span> : null}
      </div>

      {loading ? <p className="market-states-meta">Loading positioning states…</p> : null}
      {error ? <p className="market-states-error">{error}</p> : null}

      {!loading && !items.length ? (
        <p className="market-states-meta">No unusual cross-cohort patterns flagged for visible markets this week.</p>
      ) : null}

      {items.length ? (
        <div className="market-states-table-wrap">
          <table className="market-states-table">
            <thead>
              <tr>
                <th>Market</th>
                <th>State</th>
                <th>Reason</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <tr key={item.market}>
                  <td>
                    <button
                      type="button"
                      className="market-states-market-btn"
                      onClick={() => navigateToInstrument(item.market)}
                    >
                      {item.market}
                    </button>
                  </td>
                  <td>
                    <span className={`market-state-pill ${stateToneClass(item.stateId)}`}>{item.state}</span>
                  </td>
                  <td className="market-states-reason">{item.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </section>
  )
}
