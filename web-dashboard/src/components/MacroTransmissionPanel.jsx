import React from 'react'

function impactClass(impact) {
  const v = String(impact || '').toLowerCase()
  if (v === 'supportive') return 'mtp-impact-supportive'
  if (v === 'headwind') return 'mtp-impact-headwind'
  if (v === 'mixed' || v === 'conflicting') return 'mtp-impact-mixed'
  return 'mtp-impact-neutral'
}

function resolveTransmission(row) {
  const top = row?.macro_transmission
  if (top && typeof top === 'object' && top.available !== false) return top
  const nested = row?.institutional_context?.macro_transmission
  if (nested && typeof nested === 'object' && nested.available !== false) return nested
  return top && typeof top === 'object' ? top : nested && typeof nested === 'object' ? nested : null
}

function DriverCard({ block }) {
  if (!block) return null
  return (
    <article className="mtp-driver">
      <header className="mtp-driver-head">
        <h4 className="mtp-driver-title">{block.title}</h4>
        <span className={`mtp-impact-pill ${impactClass(block.asset_impact)}`}>{block.asset_impact || '—'}</span>
      </header>
      <dl className="mtp-driver-meta">
        <div>
          <dt>Direction</dt>
          <dd>{block.direction || '—'}</dd>
        </div>
        <div>
          <dt>Momentum</dt>
          <dd>{block.momentum || '—'}</dd>
        </div>
        <div>
          <dt>Confidence</dt>
          <dd>{block.confidence || '—'}</dd>
        </div>
      </dl>
      {block.detail ? <p className="mtp-driver-detail">{block.detail}</p> : null}
    </article>
  )
}

export function MacroTransmissionPanel({ row }) {
  const tx = React.useMemo(() => resolveTransmission(row), [row])

  if (!tx) {
    return (
      <section className="mtp-section" aria-label="Macro transmission">
        <h3 className="mtp-title">Macro transmission</h3>
        <p className="mtp-unavailable">Macro transmission not loaded for this week — rebuild confluence export on latest COT week.</p>
      </section>
    )
  }

  if (tx.available === false) {
    return (
      <section className="mtp-section" aria-label="Macro transmission">
        <h3 className="mtp-title">Macro transmission</h3>
        <p className="mtp-unavailable">{tx.headline || 'Rates data incomplete for transmission layer.'}</p>
      </section>
    )
  }

  const regime = tx.global_regime || {}
  const mvp = tx.macro_vs_price || {}
  const drivers = Array.isArray(tx.drivers) ? tx.drivers : []

  return (
    <section className="mtp-section" aria-label="Macro transmission">
      <div className="mtp-header">
        <h3 className="mtp-title">Macro transmission</h3>
        <span className={`mtp-align-pill ${impactClass(tx.asset_alignment)}`}>{tx.asset_alignment_label}</span>
      </div>

      {tx.generic_rates_only ? (
        <p className="mtp-incomplete-banner" role="status">
          Macro transmission incomplete — generic rates backdrop only. Asset-specific drivers (PMI, weather, inventories,
          etc.) are not wired in the feed.
        </p>
      ) : null}

      <p className="mtp-headline">{tx.headline}</p>

      {regime.tags?.length ? (
        <p className="mtp-regime-tags">
          Global regime:{' '}
          {regime.tags.map((t) => (
            <span key={t} className="mtp-tag">
              {String(t).replace(/_/g, ' ')}
            </span>
          ))}
        </p>
      ) : regime.headline ? (
        <p className="mtp-regime-tags">Global regime: {regime.headline}</p>
      ) : null}

      {tx.primary_sensitivities?.length ? (
        <details className="mtp-sensitivities">
          <summary>Primary drivers for this market</summary>
          <ul>
            {tx.primary_sensitivities.map((s) => (
              <li key={s}>{s}</li>
            ))}
          </ul>
          {tx.stress_note ? <p className="mtp-stress">{tx.stress_note}</p> : null}
        </details>
      ) : null}

      <div className="mtp-drivers-grid">
        {drivers.map((b) => (
          <DriverCard key={b.driver_id || b.title} block={b} />
        ))}
      </div>

      <aside className={`mtp-mvp mvp-${String(mvp.state || 'unknown').replace(/_/g, '-')}`}>
        <h4 className="mtp-mvp-title">Macro vs price · {mvp.label || '—'}</h4>
        <p>{mvp.interpretation}</p>
      </aside>
    </section>
  )
}
