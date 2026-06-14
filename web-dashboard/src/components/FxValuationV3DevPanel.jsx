import React from 'react'
import { resolveFxPairId } from '../fx/fxInstitutionalValuation.js'
import {
  foundationReadinessStatus,
  pairFromFoundationAudit,
  pairFromV3Latest,
  useFxValuationFoundationAudit,
  useFxValuationV3Latest,
} from '../hooks/useFxValuationV3Dev.js'
import { useFxValuation } from '../hooks/useFxValuation.js'

const STATUS_CLASS = {
  PASS: 'fxv3-status-pass',
  'NEAR PASS': 'fxv3-status-near',
  FAIL: 'fxv3-status-fail',
}

function fmtPp(v) {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)} pp`
}

function fmtPct(v) {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`
}

function DriverLine({ label, value }) {
  return (
    <li>
      <span className="fxv3-driver-k">{label}</span>
      <span className="fxv3-driver-v">{value}</span>
    </li>
  )
}

/**
 * Valuation V3 development panel — macro evidence + foundation status.
 * Does not invent fair values; shows exports and blockers only.
 */
export function FxValuationV3DevPanel({ marketId, row }) {
  const v3Doc = useFxValuationV3Latest()
  const foundationDoc = useFxValuationFoundationAudit()
  const fxRatesDoc = useFxValuation()

  const pairId = React.useMemo(
    () => resolveFxPairId(marketId, row?.fx_valuation),
    [marketId, row?.fx_valuation],
  )

  if (!pairId) return null

  const v3Pair = pairFromV3Latest(v3Doc, pairId)
  const foundationPair = pairFromFoundationAudit(foundationDoc, pairId)
  const foundationStatus = foundationReadinessStatus(foundationPair, v3Pair)

  const v3Blockers = foundationPair?.v3_blocker?.blockers || []
  const aligned = foundationPair?.v3_blocker?.aligned_panel_days
  const r2 = foundationPair?.v3_blocker?.regression_r_squared

  const drivers = v3Pair?.drivers || {}
  const dxy = v3Pair?.dxy_regime || {}
  const treas = v3Pair?.treasury_regime || {}

  const ratesPair = (fxRatesDoc?.pairs || []).find(
    (p) => String(p?.pair || '').toUpperCase() === pairId,
  )

  const policyDiff =
    drivers.policy_rate_diff ??
    row?.fx_policy_rate_diff ??
    row?.fx_valuation?.policy_rate_diff ??
    ratesPair?.policy_rate_diff
  const y2Diff =
    drivers.yield_2y_diff ??
    row?.fx_2y_yield_diff ??
    row?.fx_valuation?.yield_2y_diff ??
    ratesPair?.yield_2y_diff
  const realDiff =
    drivers.real_yield_diff ??
    row?.fx_real_yield_diff ??
    row?.fx_valuation?.real_yield_diff ??
    ratesPair?.real_yield_diff

  const modelPass = v3Pair?.audit_status === 'PASS'
  const statusLabel = modelPass ? 'Audit pass (development)' : 'In development'

  return (
    <section className="fxv3-dev-panel ws-panel" aria-label={`Valuation V3 development — ${pairId}`}>
      <header className="fxv3-dev-head">
        <div>
          <p className="fxv3-dev-eyebrow">Valuation V3</p>
          <h3 className="fxv3-dev-title">{pairId}</h3>
        </div>
        <span className={`fxv3-foundation-pill ${STATUS_CLASS[foundationStatus] || 'fxv3-status-fail'}`}>
          Foundation: {foundationStatus}
        </span>
      </header>

      <dl className="fxv3-dev-meta">
        <div>
          <dt>Status</dt>
          <dd>{statusLabel}</dd>
        </div>
        <div>
          <dt>Model</dt>
          <dd>{v3Doc?.model_id || 'fx_carry_real_yield_v3'}</dd>
        </div>
        {modelPass && v3Pair?.fair_value != null ? (
          <div>
            <dt>V3 fair value (audit)</dt>
            <dd>
              {Number(v3Pair.fair_value).toFixed(5)} · {fmtPct(v3Pair.deviation_pct)} vs spot
            </dd>
          </div>
        ) : (
          <div>
            <dt>Fair value</dt>
            <dd>Not published — gate failed or history incomplete</dd>
          </div>
        )}
        {aligned != null ? (
          <div>
            <dt>Aligned observations</dt>
            <dd>
              {aligned}
              {r2 != null ? ` · R² ${Number(r2).toFixed(4)}` : ''}
            </dd>
          </div>
        ) : null}
      </dl>

      <div className="fxv3-dev-columns">
        <div>
          <h4 className="fxv3-section-k">Current drivers (macro evidence)</h4>
          <ul className="fxv3-driver-list">
            <DriverLine label="Policy rate differential" value={fmtPp(policyDiff)} />
            <DriverLine label="2Y yield differential" value={fmtPp(y2Diff)} />
            <DriverLine label="Real yield differential" value={fmtPp(realDiff)} />
            <DriverLine
              label="DXY regime"
              value={
                dxy.available
                  ? `${dxy.regime || '—'}${dxy.percentile_52w != null ? ` · ${Number(dxy.percentile_52w).toFixed(0)}th pct 52w` : ''}`
                  : 'Data source pending'
              }
            />
            <DriverLine
              label="Treasury regime"
              value={
                treas.available
                  ? `${treas.regime || '—'} · 2s10s ${treas.slope_2s10s != null ? fmtPp(treas.slope_2s10s) : '—'}`
                  : 'Data source pending'
              }
            />
          </ul>
        </div>

        <div>
          <h4 className="fxv3-section-k">Blockers</h4>
          {v3Blockers.length ? (
            <ul className="fxv3-blocker-list">
              {v3Blockers.map((b, i) => (
                <li key={`${b}-${i}`}>{b}</li>
              ))}
            </ul>
          ) : modelPass ? (
            <p className="fxv3-blocker-none">No blockers on latest foundation audit for this pair.</p>
          ) : (
            <p className="fxv3-blocker-none">
              {v3Pair?.valuation_reason ||
                v3Pair?.driver_summary ||
                'Foundation or regression gate not cleared — see aligned obs / R² above.'}
            </p>
          )}
          {(v3Pair?.missing_inputs || []).length ? (
            <>
              <h4 className="fxv3-section-k fxv3-section-k-sub">Missing inputs</h4>
              <ul className="fxv3-blocker-list">
                {v3Pair.missing_inputs.map((m) => (
                  <li key={m}>{m}</li>
                ))}
              </ul>
            </>
          ) : null}
        </div>
      </div>

      <p className="fxv3-dev-footnote">
        Development readout only — not a trade signal. V2 macro block below remains separate legacy context from
        confluence export.
      </p>
    </section>
  )
}
