import React from 'react'
import { allInstrumentIds } from '../instrumentRegistry.js'
import { TRACKED_MARKET_IDS } from '../marketResolution.js'
import './tradeBasketWorkstation.css'

const MAX_TRADES = 5
const FREQUENCIES = [
  { id: 'daily', label: 'Daily' },
  { id: 'weekly', label: 'Weekly' },
]
const LOOKBACKS = [20, 60, 120, 252]

function newTrade(partial = {}) {
  return {
    key: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    instrument_id: '',
    direction: 'LONG',
    risk_percent: 1.0,
    ...partial,
  }
}

function fmtCorr(v) {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  return Number(v).toFixed(2)
}

function adjClass(v) {
  if (v == null || !Number.isFinite(Number(v))) return 'tbw-zero'
  const n = Number(v)
  if (Math.abs(n) < 0.05) return 'tbw-zero'
  return n > 0 ? 'tbw-pos' : 'tbw-neg'
}

function scoreTone(score, { invert = false } = {}) {
  const s = Number(score)
  if (!Number.isFinite(s)) return ''
  const good = invert ? s <= 30 : s >= 70
  const bad = invert ? s >= 70 : s <= 30
  if (good) return 'tbw-tone-good'
  if (bad) return 'tbw-tone-bad'
  return 'tbw-tone-warn'
}

function fmtPair(p) {
  if (!p) return '—'
  const adj = Number(p.direction_adjusted_correlation)
  const sign = adj > 0 ? '+' : ''
  return `${p.trade_a_instrument_id} ${p.trade_a_direction} × ${p.trade_b_instrument_id} ${p.trade_b_direction} (${sign}${adj.toFixed(2)})`
}

function clusterLabel(cluster) {
  if (!cluster?.members?.length) return '—'
  return cluster.members
    .map((m) => `${m.instrument_id} ${m.direction}`)
    .join(', ')
}

function isFxPairId(id) {
  return /^[A-Za-z]{3}\/[A-Za-z]{3}$/.test(String(id || '').trim())
}

function instrumentOptions() {
  const ids = allInstrumentIds()
  const list = ids?.length ? ids : TRACKED_MARKET_IDS
  const fx = []
  const other = []
  for (const id of list) {
    if (isFxPairId(id)) fx.push(id)
    else other.push(id)
  }
  fx.sort((a, b) => a.localeCompare(b))
  other.sort((a, b) => a.localeCompare(b))
  return [...fx, ...other]
}

function fmtExposure(v) {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  const n = Number(v)
  const sign = n > 0 ? '+' : ''
  return `${sign}${n.toFixed(2)}`
}

/**
 * Trade Basket Workstation — Phase 2B/3 + Phase 4 FX exposure display.
 * React displays only; mathematics stay in Python services.
 */
export function TradeBasketWorkstation() {
  const instruments = React.useMemo(() => instrumentOptions(), [])
  const [trades, setTrades] = React.useState(() => [
    newTrade({ instrument_id: 'AUD/NZD', direction: 'LONG' }),
    newTrade({ instrument_id: 'AUD/CHF', direction: 'LONG' }),
  ])
  const [frequency, setFrequency] = React.useState('daily')
  const [lookback, setLookback] = React.useState(60)
  const [sortKey, setSortKey] = React.useState('adjusted_abs')
  const [payload, setPayload] = React.useState(null)
  const [loading, setLoading] = React.useState(false)
  const [fetchError, setFetchError] = React.useState(null)
  const requestSeq = React.useRef(0)

  const populatedTrades = React.useMemo(
    () =>
      trades
        .filter((t) => String(t.instrument_id || '').trim())
        .map((t) => {
          const id = String(t.instrument_id).trim()
          return {
            instrument_id: id,
            instrument_pair: id,
            direction: t.direction === 'SHORT' ? 'SHORT' : 'LONG',
            risk_percent: Number(t.risk_percent) || 1.0,
          }
        }),
    [trades],
  )

  const refresh = React.useCallback(async () => {
    const seq = ++requestSeq.current
    if (populatedTrades.length === 0) {
      setPayload(null)
      setFetchError(null)
      setLoading(false)
      return
    }
    setLoading(true)
    setFetchError(null)
    try {
      const res = await fetch('/api/trade-basket', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        cache: 'no-store',
        body: JSON.stringify({
          frequency,
          lookback: Number(lookback),
          trades: populatedTrades,
        }),
      })
      const body = await res.json()
      if (seq !== requestSeq.current) return
      setPayload(body)
      if (!body || body.status !== 'ok') {
        const errs = body?.errors?.length
          ? body.errors
          : [body?.message || body?.error || 'Trade basket request failed.']
        setFetchError(errs)
      } else {
        setFetchError(null)
      }
    } catch (err) {
      if (seq !== requestSeq.current) return
      setPayload(null)
      setFetchError([err?.message || 'Fetch failed'])
    } finally {
      if (seq === requestSeq.current) setLoading(false)
    }
  }, [populatedTrades, frequency, lookback])

  // Auto-refresh when trades / direction / frequency / lookback change.
  React.useEffect(() => {
    const t = setTimeout(() => {
      refresh()
    }, 250)
    return () => clearTimeout(t)
  }, [refresh])

  const payloadMatchesBasket = React.useMemo(() => {
    if (!payload || payload.status !== 'ok') return false
    if (payload.frequency !== frequency) return false
    if (Number(payload.lookback) !== Number(lookback)) return false
    if (Number(payload.populated_trade_count) !== populatedTrades.length) return false
    const ids = new Set(populatedTrades.map((t) => `${t.instrument_id}|${t.direction}`))
    const fromPayload = new Set(
      (payload.trades || []).map((t) => `${t.instrument_id}|${t.direction}`),
    )
    if (ids.size !== fromPayload.size) return false
    for (const k of ids) {
      if (!fromPayload.has(k)) return false
    }
    return true
  }, [payload, populatedTrades, frequency, lookback])

  const pairs = React.useMemo(() => {
    if (!payloadMatchesBasket) return []
    const classMap = new Map()
    for (const row of payload?.portfolio_intelligence?.pair_classifications || []) {
      const k = `${row.trade_a_instrument_id}|${row.trade_a_direction}|${row.trade_b_instrument_id}|${row.trade_b_direction}`
      classMap.set(k, row.classification)
    }
    const rows = Array.isArray(payload?.pairs)
      ? payload.pairs.map((p) => {
          const k = `${p.trade_a_instrument_id}|${p.trade_a_direction}|${p.trade_b_instrument_id}|${p.trade_b_direction}`
          const kRev = `${p.trade_b_instrument_id}|${p.trade_b_direction}|${p.trade_a_instrument_id}|${p.trade_a_direction}`
          return {
            ...p,
            classification: classMap.get(k) || classMap.get(kRev) || null,
          }
        })
      : []
    rows.sort((a, b) => {
      if (sortKey === 'raw') {
        return Math.abs(Number(b.raw_correlation) || 0) - Math.abs(Number(a.raw_correlation) || 0)
      }
      if (sortKey === 'raw_asc') {
        return (Number(a.raw_correlation) || 0) - (Number(b.raw_correlation) || 0)
      }
      if (sortKey === 'adjusted') {
        return (
          Math.abs(Number(b.direction_adjusted_correlation) || 0) -
          Math.abs(Number(a.direction_adjusted_correlation) || 0)
        )
      }
      // default: highest absolute adjusted first
      return (
        Math.abs(Number(b.direction_adjusted_correlation) || 0) -
        Math.abs(Number(a.direction_adjusted_correlation) || 0)
      )
    })
    return rows
  }, [payload, sortKey, payloadMatchesBasket])

  const tradesEntered = populatedTrades.length
  const pairCount = payloadMatchesBasket ? Number(payload.pair_count) || pairs.length : 0
  const intel =
    payloadMatchesBasket && payload?.portfolio_intelligence?.status === 'ok'
      ? payload.portfolio_intelligence
      : null
  const fxExposure =
    payloadMatchesBasket &&
    payload?.currency_exposure?.status === 'ok' &&
    payload.currency_exposure.has_fx_trades
      ? payload.currency_exposure
      : null
  const thesis =
    payloadMatchesBasket && payload?.portfolio_thesis?.status === 'ok'
      ? payload.portfolio_thesis
      : null
  const basketWarnings = React.useMemo(() => {
    if (!payloadMatchesBasket) return []
    const rows = Array.isArray(payload?.warnings) ? payload.warnings : []
    return rows.map(String).filter(Boolean)
  }, [payload, payloadMatchesBasket])

  function updateTrade(key, patch) {
    setTrades((prev) => prev.map((t) => (t.key === key ? { ...t, ...patch } : t)))
  }

  function removeTrade(key) {
    setTrades((prev) => prev.filter((t) => t.key !== key))
  }

  function addTrade() {
    setTrades((prev) => {
      if (prev.length >= MAX_TRADES) return prev
      return [...prev, newTrade()]
    })
  }

  function resetBasket() {
    setTrades([])
    setPayload(null)
    setFetchError(null)
    setFrequency('daily')
    setLookback(60)
    setSortKey('adjusted_abs')
  }

  return (
    <div className="tbw-body" data-tbw-phase="4">
      <section className="tbw-section" aria-label="Basket builder">
        <h2>Basket Builder</h2>
        <p className="tbw-muted" style={{ marginTop: 0 }}>
          Enter complete FX pairs (e.g. AUD/NZD) or single-market instruments. FX pairs are
          decomposed into currency legs for exposure only — correlations use the pair return series.
        </p>
        <div className="tbw-cards">
          {trades.map((t, idx) => (
            <div className="tbw-card" key={t.key} data-trade-card={idx + 1}>
              <div className="tbw-card-head">
                <span>Trade {idx + 1}</span>
                <button
                  type="button"
                  className="tbw-btn tbw-btn-danger"
                  onClick={() => removeTrade(t.key)}
                >
                  Remove
                </button>
              </div>
              <div className="tbw-field">
                <label htmlFor={`tbw-inst-${t.key}`}>FX pair / Instrument</label>
                <select
                  id={`tbw-inst-${t.key}`}
                  value={t.instrument_id}
                  onChange={(e) => updateTrade(t.key, { instrument_id: e.target.value })}
                >
                  <option value="">Select FX pair or instrument…</option>
                  {instruments.map((id) => (
                    <option key={id} value={id}>
                      {id}
                    </option>
                  ))}
                </select>
              </div>
              <div className="tbw-field">
                <label>Direction</label>
                <div className="tbw-group" role="group" aria-label="Direction">
                  <button
                    type="button"
                    className={`tbw-btn${t.direction === 'LONG' ? ' is-active' : ''}`}
                    onClick={() => updateTrade(t.key, { direction: 'LONG' })}
                  >
                    LONG
                  </button>
                  <button
                    type="button"
                    className={`tbw-btn${t.direction === 'SHORT' ? ' is-active' : ''}`}
                    onClick={() => updateTrade(t.key, { direction: 'SHORT' })}
                  >
                    SHORT
                  </button>
                </div>
              </div>
              <div className="tbw-field">
                <label htmlFor={`tbw-risk-${t.key}`}>Risk %</label>
                <input
                  id={`tbw-risk-${t.key}`}
                  type="number"
                  min="0"
                  step="0.01"
                  value={t.risk_percent}
                  onChange={(e) =>
                    updateTrade(t.key, { risk_percent: e.target.value })
                  }
                />
              </div>
            </div>
          ))}
        </div>
        <div className="tbw-add-row">
          <button
            type="button"
            className="tbw-btn tbw-btn-primary"
            disabled={trades.length >= MAX_TRADES}
            onClick={addTrade}
          >
            Add Trade
          </button>
          <span className="tbw-muted" style={{ marginLeft: 10 }}>
            {trades.length}/{MAX_TRADES} slots
          </span>
        </div>
      </section>

      <section className="tbw-section" aria-label="Basket controls">
        <h2>Basket Controls</h2>
        <div className="tbw-controls">
          <div className="tbw-group" role="group" aria-label="Frequency">
            <span className="tbw-label">Frequency</span>
            {FREQUENCIES.map((f) => (
              <button
                key={f.id}
                type="button"
                className={`tbw-btn${frequency === f.id ? ' is-active' : ''}`}
                onClick={() => setFrequency(f.id)}
              >
                {f.label}
              </button>
            ))}
          </div>
          <div className="tbw-group" role="group" aria-label="Lookback">
            <span className="tbw-label">Lookback</span>
            {LOOKBACKS.map((lb) => (
              <button
                key={lb}
                type="button"
                className={`tbw-btn${Number(lookback) === lb ? ' is-active' : ''}`}
                onClick={() => setLookback(lb)}
              >
                {lb}
              </button>
            ))}
          </div>
          <button type="button" className="tbw-btn tbw-btn-primary" onClick={refresh} disabled={loading}>
            {loading ? 'Calculating…' : 'Calculate'}
          </button>
          <button type="button" className="tbw-btn" onClick={resetBasket}>
            Reset Basket
          </button>
        </div>
        <div className="tbw-summary" style={{ marginTop: '0.85rem' }}>
          <div>
            Trades Entered<strong>{tradesEntered}</strong>
          </div>
          <div>
            Pair Count<strong>{pairCount}</strong>
          </div>
          <div>
            Frequency<strong>{frequency}</strong>
          </div>
          <div>
            Lookback<strong>{lookback}</strong>
          </div>
        </div>
      </section>

      {fetchError?.length ? (
        <div className="tbw-error" role="alert">
          <strong>Basket validation / engine errors</strong>
          <ul>
            {fetchError.map((e) => (
              <li key={e}>{e}</li>
            ))}
          </ul>
        </div>
      ) : null}

      {basketWarnings.length ? (
        <div className="tbw-warn" role="status">
          <strong>Basket notes</strong>
          <ul>
            {basketWarnings.map((w) => (
              <li key={w}>{w}</li>
            ))}
          </ul>
        </div>
      ) : null}

      {!loading && populatedTrades.length === 0 ? (
        <p className="tbw-muted">Add at least one trade to calculate the basket.</p>
      ) : null}

      {intel ? (
        <section className="tbw-section" aria-label="Portfolio intelligence">
          <h2>Portfolio Intelligence</h2>
          <div className="tbw-intel-grid">
            <div className="tbw-metric">
              <label>Trades Entered</label>
              <strong>{intel.trades_entered}</strong>
            </div>
            <div className="tbw-metric">
              <label>Effective Independent Trades</label>
              <strong>{Number(intel.effective_independent_trades).toFixed(1)}</strong>
            </div>
            <div className="tbw-metric">
              <label>Diversification Score</label>
              <strong className={scoreTone(intel.diversification_score)}>
                {Number(intel.diversification_score).toFixed(1)}
              </strong>
              <div className="tbw-meter tbw-meter-div" title="0 concentrated → 100 diversified">
                <span style={{ width: `${Math.max(0, Math.min(100, intel.diversification_score))}%` }} />
              </div>
            </div>
            <div className="tbw-metric">
              <label>Duplication Score</label>
              <strong className={scoreTone(intel.duplication_score, { invert: true })}>
                {Number(intel.duplication_score).toFixed(1)}
              </strong>
              <div className="tbw-meter tbw-meter-dup" title="0 independent → 100 duplicated">
                <span style={{ width: `${Math.max(0, Math.min(100, intel.duplication_score))}%` }} />
              </div>
            </div>
            <div className="tbw-metric">
              <label>Total Planned Risk</label>
              <strong>{Number(intel.total_planned_risk).toFixed(2)}</strong>
            </div>
            <div className="tbw-metric">
              <label>Largest Risk Concentration</label>
              <strong>
                {(Number(intel.largest_risk_concentration) * 100).toFixed(0)}%
              </strong>
              <div className="tbw-meter tbw-meter-conc">
                <span
                  style={{
                    width: `${Math.max(0, Math.min(100, Number(intel.largest_risk_concentration) * 100))}%`,
                  }}
                />
              </div>
            </div>
            <div className="tbw-metric" style={{ gridColumn: '1 / -1' }}>
              <label>Largest Exposure Cluster</label>
              <div className="tbw-pair-chip">{clusterLabel(intel.largest_exposure_cluster)}</div>
            </div>
            <div className="tbw-metric">
              <label>Highest Correlated Pair</label>
              <div className="tbw-pair-chip">{fmtPair(intel.highest_correlated_pair)}</div>
            </div>
            <div className="tbw-metric">
              <label>Lowest Correlated Pair</label>
              <div className="tbw-pair-chip">{fmtPair(intel.lowest_correlated_pair)}</div>
            </div>
          </div>
          {intel.explanations?.length ? (
            <ul className="tbw-explain">
              {intel.explanations.map((line) => (
                <li key={line}>{line}</li>
              ))}
            </ul>
          ) : null}
        </section>
      ) : null}

      {thesis ? (
        <section className="tbw-section" aria-label="Portfolio thesis summary">
          <h2>Portfolio Thesis Summary</h2>

          <div className="tbw-thesis-block">
            <label>Primary Thesis</label>
            <strong className="tbw-thesis-title">{thesis.primary_thesis}</strong>
          </div>

          <div className="tbw-thesis-block">
            <label>Supporting Trades</label>
            <ul className="tbw-thesis-list">
              {(thesis.supporting_trades || []).map((t) => (
                <li key={t}>{t}</li>
              ))}
            </ul>
          </div>

          <div className="tbw-thesis-block">
            <label>Portfolio Interpretation</label>
            {(thesis.portfolio_interpretation || []).map((p) => (
              <p key={p} className="tbw-thesis-para">
                {p}
              </p>
            ))}
          </div>

          <div className="tbw-thesis-block">
            <label>Risk Concentration</label>
            <div className="tbw-intel-grid">
              <div className="tbw-metric">
                <label>Primary Exposure</label>
                <strong>{thesis.risk_concentration?.primary_exposure || '—'}</strong>
              </div>
              <div className="tbw-metric">
                <label>Shared by</label>
                <strong>
                  {thesis.risk_concentration?.shared_by_trades ?? '—'} trades
                </strong>
              </div>
              <div className="tbw-metric">
                <label>Share of planned risk</label>
                <strong>
                  {thesis.risk_concentration?.share_of_planned_risk_display || '—'}
                </strong>
              </div>
            </div>
          </div>

          <div className="tbw-thesis-block">
            <label>Diversification Interpretation</label>
            <p className="tbw-thesis-para">{thesis.diversification_interpretation}</p>
          </div>

          {thesis.correlation_interpretation ? (
            <div className="tbw-thesis-block">
              <label>Correlation Interpretation</label>
              <div className="tbw-intel-grid">
                <div className="tbw-metric">
                  <label>Relationship</label>
                  <strong>Adjusted Correlation</strong>
                </div>
                <div className="tbw-metric">
                  <label>Value</label>
                  <strong
                    className={adjClass(
                      thesis.correlation_interpretation.adjusted_correlation,
                    )}
                  >
                    {thesis.correlation_interpretation.adjusted_correlation_display}
                  </strong>
                </div>
                <div className="tbw-metric" style={{ gridColumn: '1 / -1' }}>
                  <label>Interpretation</label>
                  <div className="tbw-pair-chip">
                    {thesis.correlation_interpretation.interpretation}
                  </div>
                </div>
              </div>
            </div>
          ) : null}
        </section>
      ) : null}

      {fxExposure ? (
        <section className="tbw-section" aria-label="Currency exposure">
          <h2>Currency Exposure</h2>
          {fxExposure.dominant_currency_exposure ? (
            <div className="tbw-intel-grid">
              <div className="tbw-metric" style={{ gridColumn: '1 / -1' }}>
                <label>Dominant Currency Exposure</label>
                <strong>{fxExposure.dominant_currency_exposure.display}</strong>
                <div className="tbw-muted" style={{ marginTop: 4 }}>
                  {(Number(fxExposure.dominant_currency_exposure.share_of_gross) * 100).toFixed(0)}
                  % of gross currency exposure
                </div>
                {fxExposure.dominant_currency_exposure.contributing_trades?.length ? (
                  <div className="tbw-pair-chip" style={{ marginTop: 6 }}>
                    Contributing Trades:{' '}
                    {fxExposure.dominant_currency_exposure.contributing_trades.join(', ')}
                  </div>
                ) : null}
              </div>
            </div>
          ) : null}
          <div className="tbw-table-wrap" style={{ marginTop: '0.75rem' }}>
            <table className="tbw-table">
              <thead>
                <tr>
                  <th>Currency</th>
                  <th className="tbw-num">Net Exposure</th>
                  <th>Direction</th>
                  <th>Contributing Trades</th>
                </tr>
              </thead>
              <tbody>
                {(fxExposure.currencies || []).map((row) => (
                  <tr key={row.currency}>
                    <td>{row.currency}</td>
                    <td className={`tbw-num ${adjClass(row.net_exposure)}`}>
                      {fmtExposure(row.net_exposure)}
                    </td>
                    <td>{row.direction}</td>
                    <td>{(row.contributing_trades || []).join(', ') || '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {fxExposure.diagnostics?.length ? (
            <ul className="tbw-explain">
              {fxExposure.diagnostics.map((line) => (
                <li key={line}>{line}</li>
              ))}
            </ul>
          ) : null}
        </section>
      ) : null}

      <section className="tbw-section" aria-label="Pairwise relationships">
        <h2>Pairwise Relationships</h2>
        {loading && !pairs.length ? (
          <p className="tbw-muted">Loading basket pairs…</p>
        ) : null}
        {!loading && populatedTrades.length < 2 ? (
          <p className="tbw-muted">Add at least two trades to see pairwise relationships.</p>
        ) : null}
        {pairs.length ? (
          <div className="tbw-table-wrap">
            <table className="tbw-table">
              <thead>
                <tr>
                  <th>Trade A</th>
                  <th>Direction</th>
                  <th>Trade B</th>
                  <th>Direction</th>
                  <th
                    className="tbw-sortable tbw-num"
                    onClick={() =>
                      setSortKey((k) => (k === 'raw' ? 'raw_asc' : 'raw'))
                    }
                  >
                    Raw Correlation{sortKey.startsWith('raw') ? ' ▾' : ''}
                  </th>
                  <th
                    className="tbw-sortable tbw-num"
                    onClick={() => setSortKey('adjusted_abs')}
                  >
                    Direction Adjusted{sortKey.startsWith('adjusted') ? ' ▾' : ''}
                  </th>
                  <th>Strength</th>
                </tr>
              </thead>
              <tbody>
                {pairs.map((p, i) => (
                  <tr key={`${p.trade_a_instrument_id}-${p.trade_b_instrument_id}-${i}`}>
                    <td>{p.trade_a_instrument_id}</td>
                    <td>{p.trade_a_direction}</td>
                    <td>{p.trade_b_instrument_id}</td>
                    <td>{p.trade_b_direction}</td>
                    <td className="tbw-num">{fmtCorr(p.raw_correlation)}</td>
                    <td className={`tbw-num ${adjClass(p.direction_adjusted_correlation)}`}>
                      {fmtCorr(p.direction_adjusted_correlation)}
                    </td>
                    <td>
                      {p.classification
                        ? `${p.classification.strength}${
                            p.classification.relationship === 'negative' ? ' (−)' : ''
                          }`
                        : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
      </section>
    </div>
  )
}
