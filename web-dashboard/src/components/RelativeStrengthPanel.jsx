import React from 'react'

function fmtSigned(n) {
  const v = Number(n)
  if (!Number.isFinite(v)) return '—'
  return `${v > 0 ? '+' : ''}${v}`
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
  const rsBlock = relativeStrength?.relative_strength
  const lb =
    rsBlock?.leaderboard ||
    (relativeStrength?.currency_leaderboard || []).map((row) => ({
      ...row,
      rank: row.rank,
    }))

  const pairs = rsBlock?.pair_differentials || []
  const heat = rsBlock?.heatmap || relativeStrength?.heatmap || {}
  const usdAnchor = relativeStrength?.usd_anchor

  if (!lb.length) return null

  const weekMismatch =
    calendarWeek &&
    relativeStrength?.calendar_week &&
    relativeStrength.calendar_week !== calendarWeek

  return (
    <section className="relative-strength-panel" aria-label="FX relative strength">
      <header className="rs-header">
        <div>
          <h2 className="rs-title">Relative strength</h2>
          <p className="rs-subtitle">
            Ranked by COT-only relative strength. Flow and anomaly are shown as context, not primary rank.
            {calendarWeek ? ` Week ${calendarWeek}.` : ''}
            {weekMismatch ? (
              <span className="rs-week-mismatch"> (Export week {relativeStrength.calendar_week}.)</span>
            ) : null}
          </p>
        </div>
      </header>

      {usdAnchor ? (
        <div className="rs-usd-anchor" role="note">
          <strong>USD leg mode:</strong>{' '}
          {usdAnchor.primary_mode_label || usdAnchor.primary_mode || '—'}
          <span className="rs-usd-anchor-meta">
            {' '}
            · confidence {usdAnchor.confidence || '—'}
            {usdAnchor.synthetic_g10 ? (
              <>
                {' '}
                · G10 synthetic {fmtSigned(usdAnchor.synthetic_g10.final_score)} (COT RS{' '}
                {fmtSigned(
                  usdAnchor.synthetic_g10.cot_relative_strength_score ??
                    usdAnchor.synthetic_g10.raw_rs
                )}
                )
              </>
            ) : null}
            {usdAnchor.direct_dxy ? (
              <>
                {' '}
                · Direct DXY {fmtSigned(usdAnchor.direct_dxy.final_score)} (COT RS{' '}
                {fmtSigned(
                  usdAnchor.direct_dxy.cot_relative_strength_score ?? usdAnchor.direct_dxy.raw_rs
                )}
                )
              </>
            ) : null}
          </span>
        </div>
      ) : null}

      <div className="rs-grid">
        <div className="rs-block rs-currencies">
          <h3 className="rs-block-title">Currency relative strength</h3>
          <div className="rs-rs-head" aria-hidden>
            <span>#</span>
            <span>CCY</span>
            <span className="rs-cot-head-label">COT score</span>
            <span>Flow</span>
            <span>Anomaly</span>
            <span>Adj RS</span>
            <span className="rs-currency-head-bar">Bar</span>
          </div>
          <ol className="rs-currency-list">
            {lb.map((row) => {
              const cotRs =
                row.cot_relative_strength_score ??
                row.cot_component ??
                0
              const adjustedRs = row.adjusted_rs ?? row.raw_rs ?? 0
              return (
                <li key={row.currency} className="rs-currency-row rs-rs-row">
                  <span className="rs-rank">{row.rank}</span>
                  <span className="rs-ccy">
                    {row.currency}
                    {row.currency === 'USD' && row.usd_mode_label ? (
                      <span className="rs-usd-mode-tag" title={row.usd_mode_label}>
                        {row.data_source === 'direct_dxy_cot' ? 'DXY' : 'Synth'}
                      </span>
                    ) : null}
                  </span>
                  <span
                    className={`rs-cot-headline ${cotRs >= 0 ? 'pos' : 'neg'}`}
                    title="COT-only relative strength (headline rank)"
                  >
                    {fmtSigned(cotRs)}
                  </span>
                  <span className="rs-rs-comp">{fmtSigned(row.flow_component)}</span>
                  <span className="rs-rs-comp">{fmtSigned(row.anomaly_component)}</span>
                  <span
                    className="rs-rs-comp rs-adjusted-rs"
                    title="Adjusted RS = COT + flow + anomaly (zero-centered)"
                  >
                    {fmtSigned(adjustedRs)}
                  </span>
                  {scoreBar(cotRs)}
                </li>
              )
            })}
          </ol>
          <p className="rs-footnote">
            {rsBlock?.ranking_rule ||
              'Ranked by COT-only relative strength. adjusted_rs sums ≈ 0 across G10 (mean-centered positioning).'}
          </p>
        </div>

        <div className="rs-block rs-pairs">
          <h3 className="rs-block-title">RS pair differentials (adjusted legs)</h3>
          <div className="rs-pair-head rs-pair-head--rs" aria-hidden>
            <span>Pair</span>
            <span>Adj RS Δ</span>
            <span>Bias</span>
          </div>
          <ul className="rs-pair-list rs-pair-list--rs">
            {pairs.map((p) => (
              <li key={p.pair} className="rs-pair-row rs-pair-row--rs">
                <span className="rs-pair-name">
                  <span className="rs-arrow" aria-hidden>
                    {p.direction_arrow}
                  </span>
                  {p.pair}
                  {p.in_registry === false ? (
                    <span className="rs-registry-flag">G10 only</span>
                  ) : null}
                </span>
                <span className="rs-diff">{fmtSigned(p.raw_rs_differential)}</span>
                <span className="rs-bias">{p.directional_bias}</span>
              </li>
            ))}
          </ul>
        </div>

        <div className="rs-block rs-heat">
          <h3 className="rs-block-title">RS heatmap</h3>
          <div className="rs-heat-cols">
            <div>
              <h4>Strongest COT RS</h4>
              <ul>
                {(heat.strongest || heat.strongest_currencies || []).map((c) => (
                  <li key={c.currency}>
                    {c.currency}{' '}
                    <strong>
                      {fmtSigned(c.cot_relative_strength_score ?? c.raw_rs ?? c.final_score)}
                    </strong>
                  </li>
                ))}
              </ul>
            </div>
            <div>
              <h4>Weakest COT RS</h4>
              <ul>
                {(heat.weakest || heat.weakest_currencies || []).map((c) => (
                  <li key={c.currency}>
                    {c.currency}{' '}
                    <strong>
                      {fmtSigned(c.cot_relative_strength_score ?? c.raw_rs ?? c.final_score)}
                    </strong>
                  </li>
                ))}
              </ul>
            </div>
            <div>
              <h4>Commodities (COT)</h4>
              <ul>
                {(relativeStrength?.heatmap?.strongest_commodities || []).map((c) => (
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
