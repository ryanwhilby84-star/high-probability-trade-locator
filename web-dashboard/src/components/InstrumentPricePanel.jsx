import React from 'react'
import { fmtPrice } from '../priceData.js'
import { useCanonicalCurrentPrice } from '../prices/canonicalCurrentPrice.js'

function MiniCandleTable({ title, rows, limit = 8 }) {
  const slice = (rows || []).slice(-limit).reverse()
  if (!slice.length) {
    return (
      <div className="iprice-block">
        <h4 className="iprice-title">{title}</h4>
        <p className="mcat-empty">No bars</p>
      </div>
    )
  }
  return (
    <div className="iprice-block">
      <h4 className="iprice-title">{title}</h4>
      <table className="mcat-table iprice-table">
        <thead>
          <tr>
            <th>Date</th>
            <th>O</th>
            <th>H</th>
            <th>L</th>
            <th>C</th>
          </tr>
        </thead>
        <tbody>
          {slice.map((b) => (
            <tr key={b.date}>
              <td className="mcat-mono">{b.date}</td>
              <td className="mcat-mono">{fmtPrice(b.open)}</td>
              <td className="mcat-mono">{fmtPrice(b.high)}</td>
              <td className="mcat-mono">{fmtPrice(b.low)}</td>
              <td className="mcat-mono">{fmtPrice(b.close)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function statusTone(status) {
  const s = String(status || '').toUpperCase()
  if (s === 'LIVE') return 'iprice-status--live'
  if (s === 'STALE' || s === 'RECONNECTING') return 'iprice-status--stale'
  if (s === 'FALLBACK') return 'iprice-status--fallback'
  return 'iprice-status--off'
}

export function InstrumentPricePanel({ instrumentId, prices, loading, error }) {
  const canonical = useCanonicalCurrentPrice(instrumentId)

  if (loading) {
    return (
      <section className="iprice-panel">
        <h3 className="iprice-heading">Price data</h3>
        <p className="mcat-empty">Loading price store…</p>
      </section>
    )
  }

  if (error && !prices) {
    return (
      <section className="iprice-panel">
        <h3 className="iprice-heading">Price data</h3>
        <p className="mcat-empty">{error}</p>
        <p className="iprice-hint">
          Run <code className="mcat-mono">python -m hptl.prices.run_price_refresh</code> after the coverage audit.
        </p>
      </section>
    )
  }

  if (!prices || prices.error) {
    return (
      <section className="iprice-panel">
        <h3 className="iprice-heading">Price data</h3>
        <p className="mcat-empty">{prices?.error || 'No price series for this instrument'}</p>
        {canonical.price != null ? (
          <div className="iprice-spot" style={{ marginTop: 12 }}>
            <div>
              <span className="iprice-label">Current ({canonical.label})</span>
              <strong className="iprice-mid">{fmtPrice(canonical.price)}</strong>
            </div>
            <span className={`iprice-status ${statusTone(canonical.status)}`}>{canonical.status}</span>
          </div>
        ) : null}
      </section>
    )
  }

  const r52 = prices.range_52w || {}
  const hist = prices.history || {}
  const digits = canonical.quote?.pricePrecision ?? 2

  return (
    <section className="iprice-panel">
      <h3 className="iprice-heading">Price data</h3>
      <div className="iprice-spot">
        <div>
          <span className="iprice-label">Current ({canonical.label})</span>
          <strong className="iprice-mid">{fmtPrice(canonical.price, digits)}</strong>
        </div>
        <div>
          <span className="iprice-label">Bid / Ask</span>
          <span className="mcat-mono">
            {fmtPrice(canonical.bid, digits)} / {fmtPrice(canonical.ask, digits)}
          </span>
        </div>
        <div>
          <span className="iprice-label">As of</span>
          <span className="mcat-mono">{(canonical.asOf || '—').slice(0, 19)}</span>
        </div>
        <div>
          <span className="iprice-label">Status</span>
          <span className={`iprice-status ${statusTone(canonical.status)}`}>{canonical.status}</span>
        </div>
      </div>
      {canonical.note ? <p className="iprice-hint">{canonical.note}</p> : null}
      {r52.high != null && (
        <div className="iprice-range">
          <span className="iprice-label">52-week range</span>
          <span className="mcat-mono">
            {fmtPrice(r52.low)} – {fmtPrice(r52.high)}
            {r52.start_date ? ` (${r52.start_date} → ${r52.end_date})` : ''}
          </span>
        </div>
      )}
      <p className="iprice-meta">
        Daily bars: {hist.bar_count_daily ?? prices.daily?.length ?? 0} · Weekly:{' '}
        {hist.bar_count_weekly ?? prices.weekly?.length ?? 0}
        {canonical.providerSymbol
          ? ` · ${canonical.provider || 'provider'}:${canonical.providerSymbol}`
          : ''}
      </p>
      <div className="iprice-grid">
        <MiniCandleTable title="Daily candles (recent)" rows={prices.daily} />
        <MiniCandleTable title="Weekly candles (recent)" rows={prices.weekly} />
      </div>
    </section>
  )
}
