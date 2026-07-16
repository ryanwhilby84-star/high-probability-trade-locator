import React from 'react'

import { POSITIONING_SHEET_TABS, rolling3yContextForGroup } from '../cot/groupPositioningView.js'
import { buildRawRowsForGroup } from '../cot/rawCotPositioning.js'
import { useLegacyCot } from '../hooks/useLegacyCot.js'
import { navigateToCotWorkstation, navigateToNaturalGasValuation } from '../routing.js'
import { GroupPositioningSheet } from './GroupPositioningSheet.jsx'
import { PositioningWeeklySummary } from './PositioningWeeklySummary.jsx'

export function InstrumentPositioningWorkspace({
  marketId,
  headlineRow,
  asOfDate,
}) {
  const [activeTab, setActiveTab] = React.useState('noncommercials')
  const { instrumentData, loading: legacyLoading } = useLegacyCot(marketId)

  const tab = POSITIONING_SHEET_TABS.find((t) => t.id === activeTab) || POSITIONING_SHEET_TABS[0]

  const rawRows = React.useMemo(
    () => buildRawRowsForGroup(instrumentData, activeTab, asOfDate),
    [instrumentData, activeTab, asOfDate],
  )

  const { ctx: rolling3y, multiyear } = React.useMemo(
    () =>
      rolling3yContextForGroup({
        groupId: activeTab,
        headlineRow,
        legacyInstrument: instrumentData,
        asOfDate,
      }),
    [activeTab, headlineRow, instrumentData, asOfDate],
  )

  return (
    <section
      id="instrument-positioning-workspace"
      className="instrument-positioning-workspace instrument-positioning-workspace--dark"
      aria-label="COT positioning data"
    >
      <div className="instrument-positioning-head">
        <div className="instrument-positioning-head-row">
          <div>
            <h3 className="wo-cot-title">Positioning</h3>
            <p className="wo-cot-sub">
              Raw COT positioning data and weekly context. Open the dedicated workstation for full-screen charts.
            </p>
          </div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }}>
            {marketId === 'Natural Gas / NG' ? (
              <button
                type="button"
                className="ws-btn ws-btn-primary"
                onClick={navigateToNaturalGasValuation}
              >
                Open Valuation
              </button>
            ) : null}
            <button
              type="button"
              className="ws-btn ws-btn-primary instrument-cot-ws-open-btn"
              onClick={() => navigateToCotWorkstation(marketId)}
            >
              Open COT Workstation
            </button>
          </div>
        </div>
      </div>

      {legacyLoading ? (
        <p className="wo-cot-hint" style={{ marginTop: '12px' }}>
          Loading legacy COT data…
        </p>
      ) : (
        <>
          <PositioningWeeklySummary instrumentData={instrumentData} asOfDate={asOfDate} />

          <div
            className="instrument-positioning-tabs cot-raw-data-tabs instrument-positioning-tabs--raw"
            role="tablist"
            aria-label="COT raw data by participant group"
          >
            {POSITIONING_SHEET_TABS.map((t) => (
              <button
                key={t.id}
                type="button"
                role="tab"
                aria-selected={t.id === activeTab}
                className={`cot-raw-data-tab instrument-positioning-tab${t.id === activeTab ? ' active' : ''}`}
                onClick={() => setActiveTab(t.id)}
              >
                {t.label}
              </button>
            ))}
          </div>

          <div role="tabpanel" aria-label={tab.label} className="instrument-positioning-panel">
            <GroupPositioningSheet
              rawRows={rawRows}
              rolling3y={rolling3y}
              multiyear={multiyear}
              groupLabel={tab.label}
            />
          </div>
        </>
      )}
    </section>
  )
}
