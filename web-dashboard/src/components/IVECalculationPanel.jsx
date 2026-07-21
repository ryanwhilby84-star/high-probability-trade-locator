import React from 'react'

import { useCurrencyFuturesIVE } from '../hooks/useCurrencyFuturesIVE.js'
import { useValuationLatest } from '../hooks/useValuationLatest.js'
import {
  fmtPct,
  fmtPrice,
  iveSummaryLine,
  labelTone,
  readIVE,
  statusTone,
} from '../valuation/iveDisplay.js'
import {
  isCurrencyFuturesMarket,
  readFuturesIVE,
} from '../valuation/currencyFuturesIveDisplay.js'
import { isAgriValuationMarket } from '../valuation/agriValuationDisplay.js'
import { isMetalsValuationMarket } from '../valuation/metalsValuationDisplay.js'
import { useCanonicalCurrentPrice } from '../prices/canonicalCurrentPrice.js'

function MetaItem({ label, value, tone = null }) {
  return (
    <div className={`fxv3-meta-item${tone ? ` fxv3-meta-${tone}` : ''}`}>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  )
}

function Section({ title, children }) {
  return (
    <section className="val-ws-section">
      <h4 className="val-ws-section-title">{title}</h4>
      {children}
    </section>
  )
}

/**
 * IVE Calculation Panel — auditable fair value on traded futures instrument.
 */
export function IVECalculationPanel({ marketId, row }) {
  const futuresDoc = useCurrencyFuturesIVE()
  const valuationDoc = useValuationLatest()
  const canonical = useCanonicalCurrentPrice(marketId)

  const ive = React.useMemo(() => {
    if (isCurrencyFuturesMarket(marketId)) {
      const block = futuresDoc?.instruments?.[marketId]
      return readFuturesIVE(block)
    }
    const block = valuationDoc?.instruments?.[marketId]
    return readIVE(block)
  }, [futuresDoc, valuationDoc, marketId])

  if (!ive && !shouldShowIVEPanel(marketId)) return null

  const isFutures = isCurrencyFuturesMarket(marketId)
  const priceDigits = isAgriValuationMarket(marketId) ? 2 : isFutures && marketId?.includes('6J') ? 3 : 4
  const tone = labelTone(ive?.valuationLabel)
  const statusClass = statusTone(ive?.modelStatus)
  const showCalc = ive?.modelStatus === 'VALIDATED' || ive?.modelStatus === 'DATA_STALE'
  const displayCurrent =
    canonical.price != null ? canonical.price : null
  const currentLabel =
    canonical.price != null ? `Current price (${canonical.label})` : 'Current price (unavailable)'

  return (
    <section
      className="fxv3-panel val-ws-panel ive-panel ws-panel"
      id="valuation-evidence"
      aria-label={`IVE calculation — ${marketId}`}
    >
      <header className="fxv3-dev-head">
        <div>
          <p className="fxv3-dev-eyebrow">Institutional valuation</p>
          <h3 className="fxv3-dev-title">{marketId}</h3>
          <p className="val-ws-model-id">
            {ive?.futuresSymbol ? `${ive.futuresSymbol} · ` : ''}
            {ive?.modelName || '—'}
          </p>
        </div>
        <span className={`fxv3-foundation-pill fxv3-status-${statusClass}`}>
          {ive?.modelStatus || 'MODEL_INCOMPLETE'}
        </span>
      </header>

      {!showCalc ? (
        <div className="fxv3-unavailable">
          <p className="fxv3-unavailable-title">{ive?.modelStatus || 'MODEL_INCOMPLETE'}</p>
          <p className="fxv3-unavailable-reason">{ive?.unavailableReason}</p>
          {(ive?.inputs?._missing_inputs?.length || ive?.inputs?._stale_inputs?.length) ? (
            <ul className="fxv3-freshness-list">
              {ive.inputs._missing_inputs?.map((s) => (
                <li key={`miss-${s}`}>
                  <span>Missing</span>
                  <span>{s}</span>
                </li>
              ))}
              {ive.inputs._stale_inputs?.map((s) => (
                <li key={`stale-${s}`}>
                  <span>Stale</span>
                  <span>{s}</span>
                </li>
              ))}
            </ul>
          ) : null}
        </div>
      ) : (
        <>
          <Section title="Valuation">
            <dl className="fxv3-dev-meta fxv3-valuation-grid">
              <MetaItem label={currentLabel} value={fmtPrice(displayCurrent, priceDigits)} />
              {ive.currentPrice != null &&
              displayCurrent != null &&
              Math.abs(Number(ive.currentPrice) - Number(displayCurrent)) > 1e-6 ? (
                <MetaItem
                  label="Model spot (valuation only)"
                  value={fmtPrice(ive.currentPrice, priceDigits)}
                />
              ) : null}
              <MetaItem label="Fair value" value={fmtPrice(ive.fairValue, priceDigits)} />
              <MetaItem label="Valuation %" value={fmtPct(ive.valuationPct)} tone={tone} />
              <MetaItem label="Valuation label" value={ive.valuationLabel} tone={tone} />
              <MetaItem label="Valuation grade" value={ive.valuationGrade} />
              <MetaItem label="Model" value={ive.modelName} />
              <MetaItem label="Last updated" value={ive.lastUpdated} />
            </dl>
          </Section>

          <Section title="Source lineage">
            {ive.sourceLineage?.length ? (
              <ul className="ive-lineage-list">
                {ive.sourceLineage.map((src, i) => (
                  <li key={`${src.source_id}-${i}`} className="ive-lineage-item">
                    <div className="ive-lineage-head">
                      <strong>{src.source_name}</strong>
                      <span className="ive-lineage-id">{src.source_id}</span>
                    </div>
                    <div className="ive-lineage-dates">
                      <span>Data date: {src.source_date || '—'}</span>
                      <span>Refresh: {src.last_refresh || '—'}</span>
                    </div>
                  </li>
                ))}
              </ul>
            ) : (
              <p className="val-ws-prose">No source lineage recorded for this model.</p>
            )}
          </Section>

          <Section title="Calculation breakdown">
            {ive.calculationBreakdown?.length ? (
              <ol className="ive-calc-steps">
                {ive.calculationBreakdown.map((step, i) => (
                  <li key={`step-${step.step ?? i}`} className="ive-calc-step">
                    <span className="ive-calc-step-n">{step.step ?? i + 1}</span>
                    <div>
                      <p className="ive-calc-desc">{step.description}</p>
                      <p className="ive-calc-val">{String(step.value ?? '—')}</p>
                    </div>
                  </li>
                ))}
              </ol>
            ) : (
              <p className="val-ws-prose">No calculation steps exported.</p>
            )}
          </Section>

          <Section title="Model status">
            <p className="val-ws-trust-narrative">
              <strong>{ive.modelStatus}</strong>
              {ive.modelStatus === 'VALIDATED'
                ? ' — all required inputs present; fair value published from futures-native model.'
                : ive.modelStatus === 'DATA_STALE'
                  ? ' — fair value computed but one or more inputs exceed freshness threshold.'
                  : ' — valuation cannot be audited until required inputs and model path are complete.'}
            </p>
            <p className="val-ws-footnote">{iveSummaryLine(ive)}</p>
          </Section>
        </>
      )}
    </section>
  )
}

export function shouldShowIVEPanel(marketId) {
  if (!marketId) return false
  if (isCurrencyFuturesMarket(marketId)) return true
  if (isMetalsValuationMarket(marketId)) return true
  if (isAgriValuationMarket(marketId)) return true
  return false
}

export function ValuationInstrumentSection({ row }) {
  if (!shouldShowIVEPanel(row?.market)) return null
  return <IVECalculationPanel marketId={row?.market} row={row} />
}

/** @deprecated use IVECalculationPanel */
export const ValuationWorkstationPanel = IVECalculationPanel
