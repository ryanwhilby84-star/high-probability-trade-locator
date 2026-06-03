import React from 'react'
import { evaluatePillars, alignmentSummary } from '../thesisTracker/alignmentEngine.js'
import { directionFromSnapshot } from '../thesisTracker/thesisModel.js'

function PillarRow({ label, bias, score, reason, pass }) {
  const passCls = pass === true ? 'toe-pass-yes' : pass === false ? 'toe-pass-no' : 'toe-pass-na'
  return (
    <div className="toe-pillar-row">
      <span className="toe-pillar-label">{label}</span>
      <span className="toe-pillar-state">{bias || '—'}</span>
      <span className="toe-pillar-score">{score != null ? `${Number(score).toFixed(1)} / 10` : '—'}</span>
      <span className={`toe-pillar-pass ${passCls}`}>{pass === true ? 'PASS' : pass === false ? 'FAIL' : '—'}</span>
      {reason ? <p className="toe-pillar-reason">{reason}</p> : null}
    </div>
  )
}

/** Valuation + seasonality strip for instrument / confluence rows. */
export function OpportunityPillarsPanel({ row, direction: directionProp }) {
  const snap = React.useMemo(
    () => ({
      cot_bias: row?.cot_bias,
      valuation_bias: row?.valuation_bias,
      valuation_score: row?.valuation_score,
      valuation_reason: row?.valuation_reason,
      valuation_wired: row?.valuation_wired,
      seasonality_bias: row?.seasonality_bias,
      seasonality_score: row?.seasonality_score,
      seasonality_reason: row?.seasonality_reason,
      seasonality_wired: row?.seasonality_wired,
    }),
    [row],
  )
  const direction = directionProp || directionFromSnapshot(snap)
  const pillars = React.useMemo(() => evaluatePillars(snap, direction), [snap, direction])
  const val = pillars.find((p) => p.pillar === 'valuation')
  const sea = pillars.find((p) => p.pillar === 'seasonality')
  const align = alignmentSummary(pillars.filter((p) => ['valuation', 'seasonality'].includes(p.pillar)))

  if (!row?.valuation_bias && !row?.seasonality_bias && !val?.wired && !sea?.wired) {
    return null
  }

  return (
    <section className="ws-panel toe-instrument-pillars">
      <h3 className="ws-panel-title">Opportunity pillars — Valuation &amp; Seasonality</h3>
      <p className="ws-topbar-meta">
        Direction for pass/fail: <strong>{direction}</strong> · wired pillars {align.label}
      </p>
      <PillarRow
        label="Valuation"
        bias={row?.valuation_bias || val?.state}
        score={row?.valuation_score}
        reason={row?.valuation_reason || val?.one_line}
        pass={val?.pass}
      />
      <PillarRow
        label="Seasonality"
        bias={row?.seasonality_bias || sea?.state}
        score={row?.seasonality_score}
        reason={row?.seasonality_reason || sea?.one_line}
        pass={sea?.pass}
      />
    </section>
  )
}
