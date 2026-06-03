import React from 'react'
import { navigateToInstrument } from '../routing.js'

const CONFIDENCE_LABELS = {
  full_data: 'Full data',
  partial_data: 'Partial data',
  macro_only: 'Macro only',
  proxy_only: 'Proxy only',
  broken_incomplete: 'Broken / incomplete',
}

function confidenceClass(badge) {
  if (!badge) return 'confidence-unknown'
  return `confidence-${String(badge).replace(/_/g, '-')}`
}

export function PriorityMarketsPanel({ board, calendarWeek, onSelectMarket }) {
  const items = board?.priority_markets || []
  if (!items.length) {
    return (
      <section className="priority-markets-panel priority-markets-empty" aria-label="This week's priority markets">
        <h2 className="priority-markets-title">This week&apos;s priority markets</h2>
        <p className="priority-markets-meta">
          No high-urgency setups flagged for week {calendarWeek || '—'}. Review DEVELOPING / WATCHLIST rows below or
          rebuild confluence export.
        </p>
      </section>
    )
  }

  return (
    <section className="priority-markets-panel" aria-label="This week's priority markets">
      <div className="priority-markets-header">
        <h2 className="priority-markets-title">This week&apos;s priority markets</h2>
        {calendarWeek ? <span className="priority-markets-week">Week {calendarWeek}</span> : null}
      </div>
      <ul className="priority-markets-list">
        {items.map((item) => (
          <li key={item.market}>
            <button
              type="button"
              className="priority-market-card"
              onClick={() => (onSelectMarket ? onSelectMarket(item.market) : navigateToInstrument(item.market))}
            >
              <span className="priority-market-icon" aria-hidden>
                {item.icon || '👀'}
              </span>
              <span className="priority-market-body">
                <span className="priority-market-name">
                  {item.market}
                  <span className={`priority-tier-pill tier-${item.priority_tier}`}>{item.priority_label}</span>
                  {item.data_confidence_badge ? (
                    <span
                      className={`priority-confidence-badge ${confidenceClass(item.data_confidence_badge)}`}
                      title={item.eligibility_label || item.inclusion_reason || ''}
                    >
                      {CONFIDENCE_LABELS[item.data_confidence_badge] || item.data_confidence_badge}
                    </span>
                  ) : null}
                </span>
                <span className="priority-market-headline">
                  {item.priority_headline || item.dominant_narrative}
                </span>
                {item.scoring ? (
                  <span className="priority-market-scoring" title="Transparent score breakdown">
                    Score {item.scoring.final_attention_score} (COT {item.scoring.cot_score_component} · macro{' '}
                    {item.scoring.macro_score_component} · Δpos {item.scoring.positioning_change_component} · anomaly{' '}
                    {item.scoring.anomaly_component}; −{item.scoring.penalty_for_missing_data} missing-data)
                  </span>
                ) : null}
                {item.tactical_readable ? (
                  <span className="priority-market-tactical">{item.tactical_readable}</span>
                ) : null}
              </span>
            </button>
          </li>
        ))}
      </ul>
    </section>
  )
}
