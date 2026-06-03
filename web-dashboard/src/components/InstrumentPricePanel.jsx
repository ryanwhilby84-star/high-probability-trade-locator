import React from 'react'
import { fmtPrice } from '../priceData.js'

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

export function InstrumentPricePanel({ prices, loading, error }) {
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
      </section>
    )
  }

  const p = prices.price || {}
  const r52 = prices.range_52w || {}
  const hist = prices.history || {}

  return (
    <section className="iprice-panel">
      <h3 className="iprice-heading">Price data</h3>
      <div className="iprice-spot">
        <div>
          <span className="iprice-label">Last</span>
          <strong className="iprice-mid">{fmtPrice(p.mid)}</strong>
        </div>
        <div>
          <span className="iprice-label">Bid / Ask</span>
          <span className="mcat-mono">
            {fmtPrice(p.bid)} / {fmtPrice(p.ask)}
          </span>
        </div>
        <div>
          <span className="iprice-label">As of</span>
          <span className="mcat-mono">{(p.as_of || '—').slice(0, 19)}</span>
        </div>
      </div>
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
      </p>
      <div className="iprice-grid">
        <MiniCandleTable title="Daily candles (recent)" rows={prices.daily} />
        <MiniCandleTable title="Weekly candles (recent)" rows={prices.weekly} />
      </div>
    </section>
  )
}
