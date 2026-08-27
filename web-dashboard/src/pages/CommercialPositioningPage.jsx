import React from 'react'

import { AppShell, filterMarketsBySidebar } from '../components/AppShell.jsx'
import { CommercialPositioningTable } from '../components/CommercialPositioningTable.jsx'
import { buildCommercialTableRows } from '../cot/commercialPositioning.js'
import { useLegacyCot } from '../hooks/useLegacyCot.js'
import { allInstrumentIds } from '../instrumentRegistry.js'
import { canonicalMarketId } from '../marketResolution.js'
import { navigateToCommercialPositioning, navigateToScanner } from '../routing.js'

export function CommercialPositioningPage({ marketId, sidebarClass, onSidebarClass }) {
  const { latestStore, loading, error } = useLegacyCot(null)

  const instrumentIds = React.useMemo(() => {
    const fromStore = latestStore?.instruments ? Object.keys(latestStore.instruments).sort() : []
    const registry = allInstrumentIds()
    if (!registry.length) return fromStore
    const regSet = new Set(registry)
    return fromStore.filter((id) => regSet.has(id))
  }, [latestStore])

  const activeMarket = React.useMemo(() => {
    const id = canonicalMarketId(marketId)
    if (id && instrumentIds.includes(id)) return id
    return instrumentIds[0] || null
  }, [marketId, instrumentIds])

  const instrumentData = React.useMemo(
    () => (activeMarket && latestStore ? latestStore.instruments?.[activeMarket] : null),
    [activeMarket, latestStore],
  )

  const table = React.useMemo(() => buildCommercialTableRows(instrumentData), [instrumentData])

  const navMarkets = React.useMemo(
    () => filterMarketsBySidebar(instrumentIds, sidebarClass),
    [instrumentIds, sidebarClass],
  )

  return (
    <AppShell
      title="Commercial Positioning"
      subtitle={activeMarket ? activeMarket : 'Legacy COT commercials'}
      sidebarClass={sidebarClass}
      onSidebarClass={onSidebarClass}
      marketSwitcher={
        <label className="ws-topbar-meta">
          Market
          <select
            className="ws-select"
            style={{ marginLeft: 6, minWidth: 180 }}
            value={activeMarket || ''}
            onChange={(e) => navigateToCommercialPositioning(e.target.value)}
            disabled={!navMarkets.length}
          >
            {navMarkets.map((m) => (
              <option key={m} value={m}>
                {m}
              </option>
            ))}
          </select>
        </label>
      }
      topActions={
        <button type="button" className="ws-btn" onClick={navigateToScanner}>
          Scanner
        </button>
      }
    >
      <section className="cot-comm-page" aria-label="Commercial positioning raw data">
        {loading ? <p className="chart-ws-empty">Loading commercial COT…</p> : null}
        {!loading && error ? (
          <p className="cot-chart-empty">Commercial data unavailable — {error}</p>
        ) : null}
        {!loading && !error && !table.available ? (
          <p className="cot-chart-empty">Commercial data unavailable — {table.reason}</p>
        ) : null}
        {!loading && !error && table.available ? (
          <CommercialPositioningTable rows={table.rows} />
        ) : null}
      </section>
    </AppShell>
  )
}
