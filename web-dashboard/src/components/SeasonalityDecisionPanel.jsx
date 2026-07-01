import React from 'react'

import {
  SeasonalityProjectionErrorBoundary,
  SeasonalityProjectionPanel,
} from './SeasonalityProjectionPanel.jsx'
import { SeasonalityPriceContractPanel } from './SeasonalityPriceContractPanel.jsx'
import { gradePillClass, resolveSeasonBlock } from '../seasonality/seasonalityDecision.js'
import { auditPriceSourceContract } from '../seasonality/priceSourceContract.js'
import {
  dataSourceLabel,
  isSeasonalityVisible,
  loadToggles,
  saveToggles,
  seasonalBiasLabel,
  weeksAvailable,
} from '../seasonality/seasonalityControls.js'

function Toggle({ label, checked, onChange, disabled = false, title = null }) {
  return (
    <label className={`sea-ctrl-toggle${disabled ? ' sea-ctrl-toggle--disabled' : ''}`} title={title || undefined}>
      <input type="checkbox" checked={checked} disabled={disabled} onChange={(e) => onChange(e.target.checked)} />
      <span>{label}</span>
    </label>
  )
}

function ControlStats({ block }) {
  const grade = block?.trust_grade || 'C'
  const forward8 = block?.forward_read?.next_8w
  const rows = [
    ['Grade', grade],
    ['Years used', block?.years_used ?? block?.years_of_history ?? '—'],
    ['Sample size', block?.sample_size ?? forward8?.sample_years ?? '—'],
    ['Weeks available', `${weeksAvailable(block)}/52`],
    ['Data source', dataSourceLabel(block)],
    ['Current bias', seasonalBiasLabel(block)],
    ['Calendar position', `ISO week ${block?.current_week ?? '—'} · ${block?.seasonal_phase || '—'}`],
    ['Path vs seasonal', block?.path_alignment || '—'],
  ]
  return (
    <dl className="sea-ctrl-stats">
      {rows.map(([k, v]) => (
        <React.Fragment key={k}>
          <dt>{k}</dt>
          <dd>{v}</dd>
        </React.Fragment>
      ))}
    </dl>
  )
}

/** Seasonality workstation — projection chart + forward windows (display only). */
export function SeasonalityDecisionPanel({ marketId, seasonalityDoc, cotBlock = null }) {
  const block = React.useMemo(
    () => resolveSeasonBlock(seasonalityDoc, marketId),
    [seasonalityDoc, marketId],
  )

  const [toggles, setToggles] = React.useState(() => loadToggles(marketId, block))

  React.useEffect(() => {
    setToggles(loadToggles(marketId, block))
  }, [marketId, block?.market, block?.trust_grade])

  const setToggle = React.useCallback(
    (key, value) => {
      setToggles((prev) => {
        const next = { ...prev, [key]: value }
        saveToggles(marketId, next)
        return next
      })
    },
    [marketId],
  )

  if (!block) {
    return (
      <section className="chart-ws-seasonality-panel sea-ctrl-panel" aria-label="Seasonality control panel">
        <p className="sea-v2-outcome-empty">Seasonality data not loaded.</p>
      </section>
    )
  }

  const grade = block.trust_grade || 'C'
  const windows = block.windows_available || []
  const visible = isSeasonalityVisible(block, toggles)
  const gradeCls = gradePillClass(grade)

  const has3y = windows.includes('3Y')
  const has5y = windows.includes('5Y')
  const has10y = windows.includes('10Y')

  const contract = cotBlock ? auditPriceSourceContract(cotBlock, block, marketId) : null
  const showContractDetails = contract?.status === 'MISMATCH'

  return (
    <section className="chart-ws-seasonality-panel sea-ctrl-panel" aria-label="Seasonality control panel">
      <header className="chart-ws-seasonality-head sea-ctrl-head">
        <div>
          <h2 className="chart-ws-seasonality-title">Seasonality</h2>
          <p className="sea-ctrl-sub">
            Historical path · current-year tracking · forward projection · ISO weeks 1–52 ·{' '}
            {grade === 'A' ? 'confluence eligible' : 'not in confluence score'}
          </p>
        </div>
        <span className={`sea-v2-pill sea-v2-pill--${gradeCls}`}>Grade {grade}</span>
      </header>

      <div className="sea-ctrl-toggles" role="group" aria-label="Seasonality display controls">
        <Toggle
          label="Show seasonality"
          checked={toggles.showSeasonality}
          onChange={(v) => setToggle('showSeasonality', v)}
        />
        <Toggle
          label="Current year only"
          checked={toggles.currentYearOnly}
          disabled={!toggles.showSeasonality}
          title="Hide historical seasonal averages; keep current-year path and forward projection"
          onChange={(v) => setToggle('currentYearOnly', v)}
        />
        <Toggle
          label="3Y average"
          checked={toggles.show3y}
          disabled={!toggles.showSeasonality || toggles.currentYearOnly || !has3y}
          onChange={(v) => setToggle('show3y', v)}
        />
        <Toggle
          label="5Y average"
          checked={toggles.show5y}
          disabled={!toggles.showSeasonality || toggles.currentYearOnly || !has5y}
          onChange={(v) => setToggle('show5y', v)}
        />
        <Toggle
          label="10Y average"
          checked={toggles.show10y}
          disabled={!toggles.showSeasonality || toggles.currentYearOnly || !has10y}
          onChange={(v) => setToggle('show10y', v)}
        />
        <Toggle
          label="Forward projection"
          checked={toggles.forwardProjection}
          disabled={!toggles.showSeasonality || toggles.currentYearOnly}
          onChange={(v) => setToggle('forwardProjection', v)}
        />
        <Toggle
          label="Hide unreliable (Grade C)"
          checked={toggles.hideUnreliable}
          onChange={(v) => setToggle('hideUnreliable', v)}
        />
      </div>

      <ControlStats block={block} />

      {!visible ? (
        <div className="sea-ctrl-hidden-msg" role="status">
          {grade === 'C' && toggles.hideUnreliable ? (
            <>
              <strong>Seasonality unreliable — hidden by default.</strong>
              <p>{block.trust_notes || block.reason || 'Insufficient price history for trustworthy seasonality.'}</p>
              <p className="sea-v2-outcome-hint">
                Enable &quot;Show seasonality&quot; and turn off &quot;Hide unreliable&quot; to inspect anyway.
              </p>
            </>
          ) : (
            <>
              <strong>Seasonality hidden.</strong>
              <p>Turn on &quot;Show seasonality&quot; to view the seasonal projection chart.</p>
            </>
          )}
        </div>
      ) : (
        <>
          <SeasonalityProjectionErrorBoundary>
            <SeasonalityProjectionPanel
              block={block}
              toggles={toggles}
              generatedAt={seasonalityDoc?.generated_at}
              marketId={marketId}
              cotBlock={cotBlock}
            />
          </SeasonalityProjectionErrorBoundary>

          {showContractDetails ? (
            <details className="sea-proj-contract-details">
              <summary>Price source contract audit (mismatch)</summary>
              <SeasonalityPriceContractPanel marketId={marketId} cotBlock={cotBlock} seasonBlock={block} />
            </details>
          ) : null}
        </>
      )}
    </section>
  )
}
