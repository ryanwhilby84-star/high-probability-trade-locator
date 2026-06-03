import React from 'react'

const CONVICTION_CLASS = {
  'HIGH CONVICTION': 'rs-conviction-high',
  'MEDIUM CONVICTION': 'rs-conviction-medium',
  'LOW CONVICTION': 'rs-conviction-low',
  'WATCHLIST ONLY': 'rs-conviction-watch',
}

function scoreBar(score) {
  const pct = Math.max(0, Math.min(100, ((Number(score) + 100) / 200) * 100))
  const neg = Number(score) < 0
  return (
    <span className="rs-score-bar" aria-hidden>
      <span className={`rs-score-fill ${neg ? 'negative' : 'positive'}`} style={{ width: `${pct}%` }} />
    </span>
  )
}

export function RelativeStrengthPanel({ relativeStrength, calendarWeek }) {
  if (!relativeStrength?.currency_leaderboard?.length) {
    return null
  }

  const lb = relativeStrength.currency_leaderboard
  const pairs = relativeStrength.pair_opportunities || []
  const heat = relativeStrength.heatmap || {}
  const rankingNote =
    relativeStrength.ranking_rules?.display_board ||
    'Ranked by |raw differential| (base leg − quote leg).'

  return (
    <section className="relative-strength-panel" aria-label="FX relative institutional strength">
      <header className="rs-header">
        <div>
          <h2 className="rs-title">Relative strength — LOOK HERE</h2>
          <p className="rs-subtitle">
            Currency legs first, pair bias from differential. Not trade signals — institutional pressure only.
            {calendarWeek ? ` Week ${calendarWeek}.` : ''}
          </p>
        </div>
      </header>

      <div className="rs-grid">
        <div className="rs-block rs-currencies">
          <h3 className="rs-block-title">Currency leaderboard</h3>
          <ol className="rs-currency-list">
            {lb.map((row) => (
              <li key={row.currency} className="rs-currency-row">
                <span className="rs-rank">{row.rank}</span>
                <span className="rs-ccy">{row.currency}</span>
                <span className={`rs-score ${row.final_score >= 0 ? 'pos' : 'neg'}`}>
                  {row.final_score > 0 ? '+' : ''}
                  {row.final_score}
                </span>
                {scoreBar(row.final_score)}
                <span className="rs-conf" title={`Confidence ${(row.confidence_modifier * 100).toFixed(0)}%`}>
                  {(row.confidence_modifier * 100).toFixed(0)}%
                </span>
              </li>
            ))}
          </ol>
          <p className="rs-footnote">{relativeStrength.limitations?.[0]}</p>
        </div>

        <div className="rs-block rs-pairs">
          <h3 className="rs-block-title">Top G10 pair differentials (by |raw|)</h3>
          <p className="rs-ranking-note">{rankingNote}</p>
          <ul className="rs-pair-list">
            {pairs.map((p) => (
              <li key={p.pair} className={`rs-pair-row ${CONVICTION_CLASS[p.conviction] || ''}`}>
                <span className="rs-pair-name">
                  <span className="rs-arrow" aria-hidden>
                    {p.direction_arrow}
                  </span>
                  {p.pair}
                  {p.in_registry === false ? (
                    <span className="rs-registry-flag" title="G10 theoretical — not in OANDA registry">
                      G10 only
                    </span>
                  ) : null}
                </span>
                <span className={`rs-conviction-pill ${CONVICTION_CLASS[p.conviction] || ''}`}>{p.conviction}</span>
                <span
                  className="rs-diff"
                  title={`Raw Δ ${p.raw_differential_score ?? p.differential}; adjusted ${p.adjusted_opportunity_score}; conf ${(p.confidence_score * 100).toFixed(0)}%`}
                >
                  raw {p.raw_differential_score > 0 ? '+' : ''}
                  {p.raw_differential_score ?? p.differential}
                </span>
                <span className="rs-adj" title="Adjusted opportunity score">
                  adj {p.adjusted_opportunity_score}
                </span>
                <span className="rs-bias">{p.directional_bias}</span>
                <span className="rs-momentum">{p.momentum}</span>
                {p.crowding_warning ? <span className="rs-crowding">⚠ {p.crowding_warning}</span> : null}
              </li>
            ))}
          </ul>
        </div>

        <div className="rs-block rs-heat">
          <h3 className="rs-block-title">Heatmap</h3>
          <div className="rs-heat-cols">
            <div>
              <h4>Strongest</h4>
              <ul>
                {(heat.strongest_currencies || []).map((c) => (
                  <li key={c.currency}>
                    {c.currency} <strong>+{c.final_score}</strong>
                  </li>
                ))}
              </ul>
            </div>
            <div>
              <h4>Weakest</h4>
              <ul>
                {(heat.weakest_currencies || []).map((c) => (
                  <li key={c.currency}>
                    {c.currency} <strong>{c.final_score}</strong>
                  </li>
                ))}
              </ul>
            </div>
            <div>
              <h4>Commodities (COT)</h4>
              <ul>
                {(heat.strongest_commodities || []).map((c) => (
                  <li key={c.instrument_id}>
                    {c.instrument_id} <strong>{c.score}</strong>
                  </li>
                ))}
              </ul>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
