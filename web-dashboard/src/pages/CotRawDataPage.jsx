import React from 'react'

import { AppShell } from '../components/AppShell.jsx'
import { CotRawDataTable } from '../components/CotRawDataTable.jsx'
import {
  RAW_COT_GROUP_TABS,
  buildRawCotTableRows,
  legacyCotInstrumentIds,
} from '../cot/rawCotPositioning.js'
import { useLegacyCot } from '../hooks/useLegacyCot.js'
import { canonicalMarketId } from '../marketResolution.js'
import { navigateToCotRawData, navigateToScanner } from '../routing.js'

export function CotRawDataPage({ marketId, tabId, sidebarClass, onSidebarClass }) {
  const { latestStore, loading, error } = useLegacyCot(null)

  const instrumentIds = React.useMemo(
    () => legacyCotInstrumentIds(latestStore),
    [latestStore],
  )

  const activeMarket = React.useMemo(() => {
    const id = canonicalMarketId(marketId)
    if (id && instrumentIds.includes(id)) return id
    return instrumentIds[0] || null
  }, [marketId, instrumentIds])

  const activeTab = React.useMemo(() => {
    const found = RAW_COT_GROUP_TABS.find((t) => t.id === tabId)
    return found?.id || RAW_COT_GROUP_TABS[0].id
  }, [tabId])

  const instrumentData = React.useMemo(
    () => (activeMarket && latestStore ? latestStore.instruments?.[activeMarket] : null),
    [activeMarket, latestStore],
  )

  const table = React.useMemo(
    () => buildRawCotTableRows(instrumentData, activeTab),
    [instrumentData, activeTab],
  )

  return (
    <AppShell
      title="COT Raw Data"
      sidebarClass={sidebarClass}
      onSidebarClass={onSidebarClass}
      topActions={
        <button type="button" className="ws-btn" onClick={navigateToScanner}>
          Scanner
        </button>
      }
    >
      <section className="cot-raw-data-page detail-panel detail-panel-terminal" aria-label="Raw COT positioning data">
        <div className="cot-raw-data-toolbar">
          <label className="cot-raw-data-market">
            Market
            <select
              className="ws-select cot-raw-data-select"
              value={activeMarket || ''}
              onChange={(e) => navigateToCotRawData(e.target.value, activeTab)}
              disabled={!instrumentIds.length}
            >
              {instrumentIds.map((m) => (
                <option key={m} value={m}>
                  {m}
                </option>
              ))}
            </select>
          </label>

          <div className="cot-raw-data-tabs" role="tablist" aria-label="COT cohort">
            {RAW_COT_GROUP_TABS.map((tab) => (
              <button
                key={tab.id}
                type="button"
                role="tab"
                aria-selected={tab.id === activeTab}
                className={`cot-raw-data-tab${tab.id === activeTab ? ' active' : ''}`}
                onClick={() => navigateToCotRawData(activeMarket, tab.id)}
              >
                {tab.label}
              </button>
            ))}
          </div>
        </div>

        {loading ? <p className="chart-ws-empty">Loading legacy COT…</p> : null}
        {!loading && error ? (
          <p className="cot-chart-empty">Dataset unavailable — {error}</p>
        ) : null}
        {!loading && !error && !table.available ? (
          <p className="cot-chart-empty">Dataset unavailable — {table.reason}</p>
        ) : null}
        {!loading && !error && table.available ? (
          <CotRawDataTable rows={table.rows} />
        ) : null}
      </section>
    </AppShell>
  )
}
