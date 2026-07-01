import React from 'react'
import {
  fmtRawGap,
  freshnessLabel,
  freshnessTone,
} from '../valuation/fxValuationDiagnosticsDisplay.js'

function MetaItem({ label, value, tone = null }) {
  return (
    <div className={`fxv3-meta-item${tone ? ` fxv3-meta-${tone}` : ''}`}>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  )
}

/** Pipeline freshness block for FX valuation panels. */
export function FxValuationDiagnosticsSection({ diagnostics, v3DocGeneratedAt }) {
  if (!diagnostics) return null

  const tone = freshnessTone(diagnostics.freshness_status)
  const inputDates = diagnostics.input_latest_dates || {}

  return (
    <section className="fxv3-driver-block fxv3-diagnostics" aria-label="Valuation pipeline diagnostics">
      <h4 className="fxv3-section-k">Pipeline diagnostics</h4>
      <dl className="fxv3-dev-meta fxv3-valuation-grid">
        <MetaItem label="Input status" value={freshnessLabel(diagnostics.freshness_status)} tone={tone} />
        <MetaItem label="Valuation date" value={diagnostics.valuation_date || '—'} />
        <MetaItem label="Spot date" value={diagnostics.spot_date || '—'} />
        <MetaItem label="Raw gap %" value={fmtRawGap(diagnostics.raw_gap_pct_unrounded)} />
        <MetaItem label="Rounded gap %" value={diagnostics.gap_pct_rounded != null ? `${diagnostics.gap_pct_rounded}%` : '—'} />
        <MetaItem label="Cache generated" value={String(diagnostics.cache_generated_at || v3DocGeneratedAt || '—').slice(0, 19)} />
        <MetaItem label="Source file" value={diagnostics.source_file || '—'} />
      </dl>
      {Object.keys(inputDates).length ? (
        <>
          <h5 className="fxv3-section-k fxv3-section-k-sub">Input latest dates</h5>
          <ul className="fxv3-freshness-list">
            {Object.entries(inputDates).map(([k, v]) => (
              <li key={k}>
                <span>{k.replace(/_/g, ' ')}</span>
                <span>{String(v)}</span>
              </li>
            ))}
          </ul>
        </>
      ) : null}
      {(diagnostics.stale_inputs?.length || diagnostics.missing_inputs?.length) ? (
        <ul className="fxv3-freshness-list">
          {diagnostics.missing_inputs?.map((s) => (
            <li key={`miss-${s}`} className="fxv3-meta-fail">
              <span>Missing</span>
              <span>{s}</span>
            </li>
          ))}
          {diagnostics.stale_inputs?.map((s) => (
            <li key={`stale-${s}`} className="fxv3-meta-warn">
              <span>Stale</span>
              <span>{s}</span>
            </li>
          ))}
        </ul>
      ) : null}
      {diagnostics.price_stale ? (
        <p className="fxv3-unavailable-reason">Price input exceeds staleness threshold.</p>
      ) : null}
    </section>
  )
}
