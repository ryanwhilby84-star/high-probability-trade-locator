import React from 'react'
import { getInstrumentMeta } from '../instrumentRegistry.js'
import { isCotRowResolved } from '../marketResolution.js'
import { navigateToInstrument } from '../routing.js'
import { MacroTransmissionPanel } from './MacroTransmissionPanel.jsx'

export function CotUnavailablePanel({ row, marketId }) {
  if (isCotRowResolved(row)) return null

  const meta = row?.instrument_meta || getInstrumentMeta(marketId)
  const proxy = meta?.cot_proxy_of || row?.instrument_meta?.cot_proxy_of
  const cotStatus = row?.cot_status || meta?.cot_status
  const legMarkets = row?.leg_cot_markets || meta?.leg_cot_markets || []

  // FX pair: no direct pair COT exists — positioning is derived from the currency legs.
  if (cotStatus === 'leg_derived_cot' && legMarkets.length) {
    return (
      <section className="cot-unavail-section" aria-label="Leg-derived COT">
        <h3 className="cot-unavail-title">Derived from currency leg COT</h3>
        <p className="cot-unavail-lede">
          <strong>{marketId}</strong> has <strong>no direct pair-level COT</strong> (CFTC reports
          single-currency futures, not crosses). Positioning here is derived from each currency
          leg&apos;s canonical COT entity.
        </p>
        <div className="cot-leg-links">
          {legMarkets.map((m) => (
            <button
              key={m}
              type="button"
              className="ws-btn"
              onClick={() => navigateToInstrument(m)}
            >
              Open canonical COT: {m}
            </button>
          ))}
        </div>
        <p className="cot-unavail-meta">
          <span className="pos-badge pos-badge-leg">Leg-derived COT</span>{' '}
          See the Relative Strength panel on the scanner for the derived leg differential.
        </p>
        <MacroTransmissionPanel row={row} />
      </section>
    )
  }

  return (
    <section className="cot-unavail-section" aria-label="Positioning unavailable">
      <h3 className="cot-unavail-title">No direct COT mapping yet</h3>
      <p className="cot-unavail-lede">
        {row?.cot_status_label ||
          row?.positioning_interpretation ||
          'This OANDA instrument does not have a direct CFTC positioning series in HPTL yet.'}
      </p>
      {proxy ? (
        <p className="cot-unavail-proxy">
          <strong>Related COT market:</strong> {proxy} — leg-level proxy logic is planned; not auto-applied.
        </p>
      ) : null}
      {meta?.macro_driver_profile ? (
        <p className="cot-unavail-meta">
          <strong>Macro profile:</strong> {meta.macro_driver_profile} · <strong>Asset class:</strong>{' '}
          {meta.asset_class}
          {meta.subgroup ? ` / ${meta.subgroup}` : ''}
        </p>
      ) : null}
      <MacroTransmissionPanel row={row} />
    </section>
  )
}
