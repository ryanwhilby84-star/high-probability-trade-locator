import React from 'react'
import { resolveFxPairId } from '../fx/fxInstitutionalValuation.js'
import {
  fxValuationV3FromDocs,
  fxV3StateTone,
  fmtFxPrice,
  fmtPct,
  fmtPp,
  fmtRate,
} from '../fx/fxValuationV3Display.js'
import {
  pairFromFoundationAudit,
  useFxValuationFoundationAudit,
  useFxValuationV3Latest,
} from '../hooks/useFxValuationV3Dev.js'

function MetaItem({ label, value, tone = null }) {
  return (
    <div className={`fxv3-meta-item${tone ? ` fxv3-meta-${tone}` : ''}`}>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  )
}

function DriverBlock({ title, children }) {
  return (
    <div className="fxv3-driver-block">
      <h4 className="fxv3-section-k">{title}</h4>
      {children}
    </div>
  )
}

/**
 * Valuation V3.0 — live FX fair value (audit-gated only).
 * Answers: where should price be?
 */
export function FxValuationV3Panel({ marketId, row }) {
  const v3Doc = useFxValuationV3Latest()
  const foundationDoc = useFxValuationFoundationAudit()

  const pairId = React.useMemo(
    () => resolveFxPairId(marketId, row?.fx_valuation),
    [marketId, row?.fx_valuation],
  )

  if (!pairId) return null

  const foundationPair = pairFromFoundationAudit(foundationDoc, pairId)
  const model = fxValuationV3FromDocs(v3Doc, foundationPair, pairId)

  if (!model) return null

  const d = model.drivers || {}
  const dxy = model.dxy || {}
  const treas = model.treasury || {}
  const tone = fxV3StateTone(model.state)

  return (
    <section className="fxv3-panel ws-panel" aria-label={`Valuation V3 — ${pairId}`}>
      <header className="fxv3-dev-head">
        <div>
          <p className="fxv3-dev-eyebrow">Valuation V3</p>
          <h3 className="fxv3-dev-title">{pairId}</h3>
        </div>
        <span className={`fxv3-foundation-pill fxv3-status-${model.foundationTone}`}>
          {model.wired ? 'LIVE' : model.unavailable ? 'UNAVAILABLE' : 'GATED'}
        </span>
      </header>

      {model.wired ? (
        <>
          <dl className="fxv3-dev-meta fxv3-valuation-grid">
            <MetaItem label="Spot price" value={fmtFxPrice(model.spot)} />
            <MetaItem label="Fair value" value={fmtFxPrice(model.fairValue)} />
            <MetaItem label="Deviation" value={fmtPct(model.deviation)} tone={tone} />
            <MetaItem label="State" value={model.state || '—'} tone={tone} />
            <MetaItem label="Confidence" value={model.confidence || 'None'} />
            <MetaItem label="Model" value={model.modelId || 'fx_carry_real_yield_v3'} />
            <MetaItem label="Audit status" value={model.auditStatus || '—'} />
          </dl>

          <div className="fxv3-dev-columns">
            <div>
              <h4 className="fxv3-section-k">Macro drivers</h4>

              <DriverBlock title="Policy differential">
                <p className="fxv3-driver-detail">
                  {model.base} policy: {fmtRate(d.base_policy_rate)}
                  <br />
                  {model.quote} policy: {fmtRate(d.quote_policy_rate)}
                </p>
                <p className="fxv3-driver-result">
                  Policy differential: <strong>{fmtPp(d.policy_rate_diff)}</strong>
                </p>
              </DriverBlock>

              <DriverBlock title="2-year yield differential">
                <p className="fxv3-driver-detail">
                  {model.base} 2Y: {fmtRate(d.base_yield_2y)}
                  <br />
                  {model.quote} 2Y: {fmtRate(d.quote_yield_2y)}
                </p>
                <p className="fxv3-driver-result">
                  2Y differential: <strong>{fmtPp(d.yield_2y_diff)}</strong>
                </p>
              </DriverBlock>

              <DriverBlock title="Real yield differential">
                <p className="fxv3-driver-detail">
                  {model.base} real yield: {fmtRate(d.base_real_yield)}
                  <br />
                  {model.quote} real yield: {fmtRate(d.quote_real_yield)}
                </p>
                <p className="fxv3-driver-result">
                  Real yield differential: <strong>{fmtPp(d.real_yield_diff)}</strong>
                </p>
              </DriverBlock>

              <DriverBlock title="DXY regime">
                <p className="fxv3-driver-result">
                  {dxy.regime_label || dxy.regime || '—'}
                  {dxy.percentile_52w != null ? (
                    <>
                      {' '}
                      · {Number(dxy.percentile_52w).toFixed(0)}th pct 52w
                    </>
                  ) : null}
                </p>
                {dxy.as_of ? <p className="fxv3-driver-asof">As of {dxy.as_of}</p> : null}
              </DriverBlock>

              <DriverBlock title="Treasury regime">
                <p className="fxv3-driver-result">
                  {treas.regime_label || treas.regime || '—'}
                  {treas.slope_2s10s != null ? (
                    <>
                      {' '}
                      · 2s10s {fmtPp(treas.slope_2s10s)}
                    </>
                  ) : null}
                </p>
                {treas.as_of ? <p className="fxv3-driver-asof">As of {treas.as_of}</p> : null}
              </DriverBlock>
            </div>

            <div>
              <h4 className="fxv3-section-k">Explanation</h4>
              <p className="fxv3-explanation">{model.explanation || model.driverSummary}</p>
              {model.inputFreshness ? (
                <>
                  <h4 className="fxv3-section-k fxv3-section-k-sub">Input freshness</h4>
                  <ul className="fxv3-freshness-list">
                    {Object.entries(model.inputFreshness).map(([k, v]) => (
                      <li key={k}>
                        <span>{k.replace(/_/g, ' ')}</span>
                        <span>{String(v)}</span>
                      </li>
                    ))}
                  </ul>
                </>
              ) : null}
            </div>
          </div>
        </>
      ) : (
        <div className="fxv3-unavailable">
          <p className="fxv3-unavailable-title">VALUATION UNAVAILABLE</p>
          <p className="fxv3-unavailable-reason">{model.unavailableReason}</p>
          {(model.blockers || []).length ? (
            <ul className="fxv3-blocker-list">
              {model.blockers.map((b, i) => (
                <li key={`${b}-${i}`}>{b}</li>
              ))}
            </ul>
          ) : null}
        </div>
      )}
    </section>
  )
}

/** @deprecated Use FxValuationV3Panel */
export const FxValuationV3DevPanel = FxValuationV3Panel
