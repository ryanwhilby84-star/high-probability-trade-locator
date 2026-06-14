import React from 'react'
import { pillarBar } from '../fx/fxSetupRanking.js'

const STATUS_CLASS = {
  Ripening: 'fx-val-status-ripening',
  Strong: 'fx-val-status-strong',
  Extended: 'fx-val-status-extended',
  Neutral: 'fx-val-status-neutral',
  Deteriorating: 'fx-val-status-deteriorating',
}

const BIAS_CLASS = {
  Bullish: 'fx-val-bias-bull',
  Bearish: 'fx-val-bias-bear',
  Neutral: 'fx-val-bias-neutral',
}

function DriverRow({ driver }) {
  if (!driver) return null
  return (
    <div className="fx-val-driver-row">
      <span className="fx-val-driver-label">{driver.label}</span>
      <span className="fx-val-driver-value">{driver.display ?? '—'}</span>
    </div>
  )
}

/** Dedicated FX Valuation Panel — V2 secondary track (setup ranking / legacy drawer). */
export function FxValuationPanel({ panel, pair }) {
  if (!panel) {
    return (
      <section className="fx-val-panel fx-val-panel-empty" aria-label="FX Valuation">
        <p className="fx-val-empty">Valuation panel unavailable for {pair || 'this pair'}.</p>
      </section>
    )
  }

  const bar = pillarBar(panel.valuation_score)
  const statusCls = STATUS_CLASS[panel.status] || 'fx-val-status-neutral'
  const biasCls = BIAS_CLASS[panel.bias] || 'fx-val-bias-neutral'
  const drivers = panel.drivers || {}

  return (
    <section className="fx-val-panel" aria-label={`FX Valuation — ${panel.pair || pair}`}>
      <header className="fx-val-head">
        <div>
          <h3 className="fx-val-title">FX Valuation — {panel.pair || pair}</h3>
          <p className="fx-val-sub">Institutional Macro V2 + TFF overlay · standalone valuation readout</p>
        </div>
        <span className={`fx-val-status-pill ${statusCls}`}>{panel.status || 'Neutral'}</span>
      </header>

      <div className="fx-val-score-grid">
        <div className="fx-val-score-main">
          <span className="fx-val-score-label">Valuation Score</span>
          <div className="fx-val-score-row">
            <span className="fx-val-pillar-bar" aria-hidden="true">
              {'█'.repeat(bar.filled)}
              {'░'.repeat(bar.empty)}
            </span>
            <span className="fx-val-score-num">{panel.score_display || bar.label}</span>
          </div>
        </div>
        <div className="fx-val-meta-cell">
          <span className="fx-val-meta-label">Bias</span>
          <span className={`fx-val-meta-value ${biasCls}`}>{panel.bias || '—'}</span>
        </div>
        <div className="fx-val-meta-cell">
          <span className="fx-val-meta-label">Momentum</span>
          <span className="fx-val-meta-value">{panel.momentum || '—'}</span>
        </div>
        <div className="fx-val-meta-cell">
          <span className="fx-val-meta-label">Daily Δ</span>
          <span className="fx-val-meta-value">{panel.daily_change_display ?? '—'}</span>
        </div>
        <div className="fx-val-meta-cell">
          <span className="fx-val-meta-label">Weekly Δ</span>
          <span className="fx-val-meta-value">{panel.weekly_change_display ?? '—'}</span>
        </div>
      </div>

      <div className="fx-val-drivers">
        <h4 className="fx-val-section-title">Valuation Drivers</h4>
        <DriverRow driver={drivers.policy_rate_differential} />
        <DriverRow driver={drivers.yield_2y_differential} />
        <DriverRow driver={drivers.real_yield_differential} />
        <div className="fx-val-driver-row">
          <span className="fx-val-driver-label">{drivers.dxy_tff_overlay?.label || 'DXY TFF overlay'}</span>
          <span className="fx-val-driver-value">{drivers.dxy_tff_overlay?.display ?? '—'}</span>
        </div>
        {drivers.dxy_tff_overlay?.detail ? (
          <p className="fx-val-driver-detail">{drivers.dxy_tff_overlay.detail}</p>
        ) : null}
        <div className="fx-val-driver-row">
          <span className="fx-val-driver-label">
            {drivers.treasury_positioning_overlay?.label || 'Treasury positioning overlay'}
          </span>
          <span className="fx-val-driver-value">{drivers.treasury_positioning_overlay?.display ?? '—'}</span>
        </div>
        {drivers.treasury_positioning_overlay?.detail ? (
          <p className="fx-val-driver-detail">{drivers.treasury_positioning_overlay.detail}</p>
        ) : null}
      </div>

      <div className="fx-val-narrative">
        <h4 className="fx-val-section-title">Plain English</h4>
        <p>{panel.narrative}</p>
      </div>
    </section>
  )
}
