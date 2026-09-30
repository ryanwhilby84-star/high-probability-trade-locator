import React from 'react'

function pct(value, digits = 1) {
  if (value == null || !Number.isFinite(Number(value))) return '—'
  const n = Number(value)
  return `${n > 0 ? '+' : ''}${n.toFixed(digits)}%`
}

function freq(value) {
  if (value == null || !Number.isFinite(Number(value))) return '—'
  return `${Math.round(Number(value) * 100)}%`
}

function tone(direction) {
  if (direction === 'Bullish') return 'bull'
  if (direction === 'Bearish') return 'bear'
  return 'mixed'
}

export function SeasonalLookbackAudit({ lookback }) {
  const [open, setOpen] = React.useState(false)
  if (!lookback?.available) return null

  const overall = lookback.overall || {}
  const horizons = lookback.horizons || {}
  const core = ['1w', '2w', '4w', '8w', '12w']
  const auditHorizon = horizons['8w'] || {}
  const outcomes = Array.isArray(auditHorizon.annual_outcomes)
    ? auditHorizon.annual_outcomes
    : []

  return (
    <section className="sws-lookback-audit" aria-label="Reliable seasonal lookback">
      <div className="sws-lookback-head">
        <div>
          <span className="sws-lookback-kicker">Seasonal lookback</span>
          <strong className={`sws-lookback-verdict is-${tone(overall.direction)}`}>
            {overall.direction || 'Mixed'}
          </strong>
          <span className="sws-lookback-confidence">
            {overall.confidence || 'LOW'} confidence · W{lookback.anchor_week} · {lookback.lookback}
          </span>
        </div>
        <button
          type="button"
          className="sws-btn"
          aria-expanded={open}
          onClick={() => setOpen((value) => !value)}
        >
          {open ? 'Hide years' : 'Audit years'}
        </button>
      </div>

      <div className="sws-lookback-grid">
        {core.map((key) => {
          const row = horizons[key] || {}
          return (
            <div className="sws-lookback-card" key={key}>
              <div className="sws-lookback-card-top">
                <strong>{key.toUpperCase()}</strong>
                <span className={`is-${tone(row.direction)}`}>{row.direction || 'Mixed'}</span>
              </div>
              <div>median <strong>{pct(row.median_pct)}</strong></div>
              <div>bull {freq(row.bullish_frequency)} · bear {freq(row.bearish_frequency)}</div>
              <div>n={row.n ?? '—'}</div>
              <div className="sws-lookback-excursion">
                fav close {pct(row.median_favourable_close_excursion_pct)} · adverse close {pct(row.median_adverse_close_excursion_pct)}
              </div>
            </div>
          )
        })}
      </div>

      <p className="sws-lookback-method">
        Exact same ISO week · one observation per year · completed adjacent weekly closes only · gaps rejected · no future-data leakage. Excursions are close-to-close, not intrabar MFE/MAE.
      </p>

      {open ? (
        <div className="sws-lookback-table-wrap">
          <div className="sws-lookback-table-title">8-week historical outcomes</div>
          <table className="sws-lookback-table">
            <thead>
              <tr>
                <th>Year</th>
                <th>Anchor</th>
                <th>End</th>
                <th>Return</th>
                <th>Best close excursion</th>
                <th>Worst close excursion</th>
              </tr>
            </thead>
            <tbody>
              {outcomes.map((row) => (
                <tr key={`${row.year}-${row.anchor_date}`}>
                  <td>{row.year}</td>
                  <td>{row.anchor_date}</td>
                  <td>{row.end_date}</td>
                  <td className={Number(row.return_pct) >= 0 ? 'is-bull' : 'is-bear'}>{pct(row.return_pct)}</td>
                  <td>{pct(row.max_up_close_excursion_pct)}</td>
                  <td>{pct(row.max_down_close_excursion_pct)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </section>
  )
}
