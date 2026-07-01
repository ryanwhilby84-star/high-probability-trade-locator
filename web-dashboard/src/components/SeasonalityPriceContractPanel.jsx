import React from 'react'

import { auditPriceSourceContract } from '../seasonality/priceSourceContract.js'

function PanelTable({ title, panel }) {
  if (!panel) return null
  const rows = [
    ['Source file', panel.sourceFile],
    ['Instrument key', panel.instrumentKey],
    ['Canonical source', panel.canonicalSource || '—'],
    ['Canonical symbol', panel.canonicalSymbol || '—'],
    ['Store key', panel.priceStoreKey],
    ['Price field', panel.priceField],
    ['Bar cadence', panel.barCadence],
    ['Date range', panel.dateStart && panel.dateEnd ? `${panel.dateStart} → ${panel.dateEnd}` : '—'],
    ['Bars', panel.barCount ?? '—'],
    [
      'Latest',
      panel.latestDate
        ? `${panel.latestDate} @ ${panel.latestPrice ?? '—'}${panel.priceDate && panel.priceDate !== panel.latestDate ? ` (bar ${panel.priceDate})` : ''}`
        : '—',
    ],
    ['Transformation', panel.transformation],
  ]
  if (panel.barSource) rows.splice(7, 0, ['Derivation label', panel.barSource])
  if (panel.proxy) {
    rows.push(['Proxy', 'Yes'])
    rows.push(['Proxy explanation', panel.proxyExplanation || '—'])
  }

  return (
    <div className="sea-contract-panel">
      <h4 className="sea-v2-section-title">{title}</h4>
      <dl className="sea-ctrl-stats sea-contract-stats">
        {rows.map(([k, v]) => (
          <React.Fragment key={k}>
            <dt>{k}</dt>
            <dd>{v}</dd>
          </React.Fragment>
        ))}
      </dl>
    </div>
  )
}

/** Data-contract audit — shown instead of seasonality timeline until price paths match. */
export function SeasonalityPriceContractPanel({ marketId, cotBlock, seasonBlock }) {
  const audit = React.useMemo(
    () => auditPriceSourceContract(cotBlock, seasonBlock, marketId),
    [cotBlock, seasonBlock, marketId],
  )

  const statusCls =
    audit.status === 'ALIGNED'
      ? 'sea-contract-status--ok'
      : audit.status === 'PROXY'
        ? 'sea-contract-status--proxy'
        : 'sea-contract-status--fail'

  return (
    <div className="sea-contract-wrap" role="status">
      <div className="sea-contract-head">
        <h3 className="sea-contract-title">Canonical price contract</h3>
        <span className={`sea-contract-status ${statusCls}`}>{audit.status}</span>
      </div>

      {audit.status === 'MISMATCH' ? (
        <p className="sea-contract-banner" role="alert">
          <strong>Seasonality timeline hidden.</strong> Consumers reference different canonical sources
          or store keys.
        </p>
      ) : null}

      {audit.disclosure ? <p className="sea-contract-disclosure">{audit.disclosure}</p> : null}

      <PanelTable title="1. COT workstation price panel" panel={audit.cotPanel} />
      <PanelTable title="2. Seasonality price panel (timeline — hidden)" panel={audit.seaPanel} />

      <div className="sea-contract-why">
        <h4 className="sea-v2-section-title">Why the paths differ</h4>
        <ul>
          {audit.reasons.map((r) => (
            <li key={r}>{r}</li>
          ))}
        </ul>
      </div>

      <p className="sea-v2-outcome-hint">
        Full audit: run{' '}
        <code>python scripts/run_price_source_contract_audit.py &quot;{marketId}&quot;</code>
      </p>
    </div>
  )
}
