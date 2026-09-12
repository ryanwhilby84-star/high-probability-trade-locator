import React from 'react'

import {
  CROSS_MARKET_LOOKBACKS,
  DXY_COMPONENTS,
  DXY_MARKET_ID,
  buildCrossMarketLookbackResult,
  buildCrossMarketStability,
} from './crossMarketSeasonality.js'

const cache = new Map()

function pct(v, digits = 0) {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  return `${Number(v).toFixed(digits)}%`
}

function stateClass(state) {
  if (state === 'agree') return 'sws-cross-agree'
  if (state === 'conflict') return 'sws-cross-conflict'
  if (state === 'mixed') return 'sws-cross-mixed'
  return 'sws-cross-unavailable'
}

function verdictClass(verdict = '') {
  if (/CONFIRMED|CONFIRMATION/.test(verdict)) return 'sws-cross-agree'
  if (/CONTRADICTION/.test(verdict)) return 'sws-cross-conflict'
  return 'sws-cross-mixed'
}

async function fetchSeasonality(marketId, lookback) {
  const key = `${marketId}::${lookback}`
  if (cache.has(key)) return cache.get(key)

  const promise = fetch(
    `/api/seasonality-workstation/${encodeURIComponent(marketId)}?lookback=${encodeURIComponent(lookback)}`,
    { cache: 'no-store' },
  )
    .then(async (response) => {
      const body = await response.json().catch(() => null)
      if (!response.ok || !body || body.status !== 'ok') return null
      return body
    })
    .catch(() => null)

  cache.set(key, promise)
  return promise
}

async function loadLookback(lookback) {
  const ids = [DXY_MARKET_ID, ...DXY_COMPONENTS.map((row) => row.id)]
  const payloads = await Promise.all(ids.map((id) => fetchSeasonality(id, lookback)))
  const byId = Object.fromEntries(ids.map((id, index) => [id, payloads[index]]))

  return buildCrossMarketLookbackResult({
    lookback,
    dxyPayload: byId[DXY_MARKET_ID],
    componentPayloads: Object.fromEntries(DXY_COMPONENTS.map((row) => [row.id, byId[row.id]])),
  })
}

export function CrossMarketSeasonalityPanel({ activeLookback = '15Y' }) {
  const [results, setResults] = React.useState({})
  const [loading, setLoading] = React.useState(true)

  React.useEffect(() => {
    let cancelled = false
    setLoading(true)

    Promise.all(CROSS_MARKET_LOOKBACKS.map(async (lookback) => [lookback, await loadLookback(lookback)]))
      .then((rows) => {
        if (cancelled) return
        setResults(Object.fromEntries(rows))
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })

    return () => {
      cancelled = true
    }
  }, [])

  const selected = results[activeLookback] || results['15Y'] || null
  const stability = React.useMemo(() => buildCrossMarketStability(results), [results])

  return (
    <section className="sws-pane sws-cross-market" data-sws-panel="cross_market_confirmation">
      <div className="sws-cross-head">
        <div>
          <h3>Cross-market seasonal confirmation</h3>
          <p className="sws-muted">
            Sanity check only — it does not change the seasonality engine. A bullish DXY should usually
            be accompanied by bearish standalone currency futures, and vice versa.
          </p>
        </div>
        <div className={`sws-cross-verdict ${verdictClass(stability.verdict)}`}>
          {loading ? 'CHECKING…' : stability.verdict}
        </div>
      </div>

      <div className="sws-cross-note">
        DXY basket coverage available here: 95.8%. EUR 57.6% · JPY 13.6% · GBP 11.9% · CAD 9.1% · CHF
        3.6%. SEK 4.2% is disclosed as missing rather than silently guessed.
      </div>

      {loading && !selected ? (
        <p className="sws-muted">Loading 5Y / 10Y / 15Y cross-market checks…</p>
      ) : null}

      {selected ? (
        <>
          <div className="sws-cross-horizon-grid">
            {selected.horizons.map((row) => (
              <div className="sws-cross-horizon" key={row.horizon}>
                <div className="sws-cross-horizon-top">
                  <strong>{row.horizon}W</strong>
                  <span className={verdictClass(row.verdict)}>{row.verdict}</span>
                </div>
                <div className="sws-cross-kpi">
                  <span>DXY</span>
                  <strong>{row.dxyDirection}</strong>
                </div>
                <div className="sws-cross-kpi">
                  <span>FX should be</span>
                  <strong>{row.expectedCurrencyDirection}</strong>
                </div>
                <div className="sws-cross-kpi">
                  <span>Weighted agreement</span>
                  <strong>{pct(row.agreementPct)}</strong>
                </div>
                <div className="sws-cross-kpi">
                  <span>Directional coverage</span>
                  <strong>{pct(row.directionalCoveragePct, 1)}</strong>
                </div>
              </div>
            ))}
          </div>

          <div className="sws-cross-table-wrap">
            <table className="sws-cross-table">
              <thead>
                <tr>
                  <th>Contract</th>
                  <th>Weight</th>
                  {selected.horizons.map((row) => (
                    <th key={row.horizon}>{row.horizon}W</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {DXY_COMPONENTS.map((component) => (
                  <tr key={component.id}>
                    <td>
                      <strong>{component.code}</strong>
                      <span>{component.label}</span>
                    </td>
                    <td>{pct(component.weight * 100, 1)}</td>
                    {selected.horizons.map((row) => {
                      const cell = row.components.find((item) => item.id === component.id)
                      return (
                        <td key={row.horizon} className={stateClass(cell?.state)}>
                          {cell?.direction || 'Unavailable'}
                        </td>
                      )
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      ) : null}

      <div className="sws-cross-lookbacks">
        {CROSS_MARKET_LOOKBACKS.map((lookback) => {
          const row = results[lookback]
          return (
            <div key={lookback} className={`sws-cross-lookback${lookback === activeLookback ? ' is-active' : ''}`}>
              <span>{lookback}</span>
              <strong className={verdictClass(row?.verdict)}>{row?.verdict || '—'}</strong>
              <small>
                agreement {pct(row?.averageAgreementPct)} · coverage {pct(row?.averageCoveragePct, 1)}
              </small>
            </div>
          )
        })}
      </div>

      <p className="sws-muted sws-cross-footnote">
        Mixed signals do not count as confirmation. Agreement is calculated only from directional
        basket weight; coverage is shown separately so a thin sample cannot masquerade as a strong
        confirmation score.
      </p>
    </section>
  )
}
