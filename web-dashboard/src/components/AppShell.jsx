import React from 'react'
import { getAssetClasses, assetClassForMarket, assetClassLabel, marketsInAssetClass } from '../marketCatalog.js'
import { allInstrumentIds } from '../instrumentRegistry.js'
import { canonicalMarketId, TRACKED_MARKET_IDS } from '../marketResolution.js'
import {
  navigateToJournal,
  navigateToScanner,
  navigateToNaturalGasValuation,
  navigateToDxyMacroBias,
  navigateToGoldValuation,
  navigateToInstrument,
  navigateToMacroHub,
  navigateToSeasonalityWorkstation,
  navigateToCorrelationMatrix,
  navigateToTradeBasket,
  navigateToMacroIntelligence,
} from '../routing.js'

const DXY_MARKET = 'US Dollar Index / DX'

export function AppShell({
  children,
  title,
  subtitle,
  date,
  dates,
  onDateChange,
  latestCotReportDate,
  cotFeedStatus,
  sidebarClass,
  onSidebarClass,
  marketSwitcher,
  topActions,
  contentClassName = '',
}) {
  const weekDates = Array.isArray(dates) ? dates : []
  const showWeekPicker = weekDates.length > 0 && typeof onDateChange === 'function'

  return (
    <div className="ws-root">
      <aside className="ws-sidebar" aria-label="Asset class navigation">
        <button
          type="button"
          className="ws-brand ws-brand-btn"
          onClick={() => {
            onSidebarClass('all')
            navigateToScanner()
          }}
          title="Back to market scanner"
        >
          HPTL
          <strong>Markets</strong>
        </button>
        <nav className="ws-nav">
          <button
            type="button"
            className={`ws-nav-btn${sidebarClass === 'all' ? ' active' : ''}`}
            onClick={() => {
              onSidebarClass('all')
              navigateToScanner()
            }}
          >
            All markets
            <span className="ws-nav-count" />
          </button>
          {(getAssetClasses() || []).map((ac) => {
            const marketCount = Array.isArray(ac.markets) ? ac.markets.length : 0
            return (
            <button
              key={ac.id}
              type="button"
              className={`ws-nav-btn${sidebarClass === ac.id ? ' active' : ''}`}
              onClick={() => {
                onSidebarClass(ac.id)
                navigateToScanner()
              }}
              disabled={!marketCount}
              title={!marketCount ? 'No instruments in this class yet' : undefined}
            >
              {ac.label}
              <span className="ws-nav-count">{marketCount || '—'}</span>
            </button>
            )
          })}
        </nav>
        {/* Phase 6: product nav only. Audit/debug routes remain routable but hidden. */}
        <div className="ws-product-nav">
          <button type="button" className="ws-btn ws-btn-primary" style={{ width: '100%' }} onClick={navigateToScanner}>
            Scanner
          </button>
          <button type="button" className="ws-btn" style={{ width: '100%', marginTop: 8 }} onClick={navigateToMacroHub}>
            Macro Hub
          </button>
          <button type="button" className="ws-btn" style={{ width: '100%', marginTop: 8 }} onClick={navigateToJournal}>
            Trade Journal
          </button>
          <button
            type="button"
            className="ws-btn"
            style={{ width: '100%', marginTop: 8 }}
            onClick={navigateToNaturalGasValuation}
          >
            NG Valuation
          </button>
          <button
            type="button"
            className="ws-btn"
            style={{ width: '100%', marginTop: 8 }}
            onClick={navigateToGoldValuation}
          >
            Gold Valuation
          </button>
          <button
            type="button"
            className="ws-btn"
            style={{ width: '100%', marginTop: 8 }}
            onClick={() => {
              const universe = allInstrumentIds()
              const first =
                (universe && universe[0]) ||
                (TRACKED_MARKET_IDS && TRACKED_MARKET_IDS[0]) ||
                'NASDAQ / NQ'
              navigateToSeasonalityWorkstation(first)
            }}
          >
            Seasonality Workstation
          </button>
          <button
            type="button"
            className="ws-btn"
            style={{ width: '100%', marginTop: 8 }}
            onClick={navigateToCorrelationMatrix}
          >
            Correlation Matrix
          </button>
          <button
            type="button"
            className="ws-btn"
            style={{ width: '100%', marginTop: 8 }}
            onClick={navigateToTradeBasket}
          >
            Trade Basket
          </button>
          <button
            type="button"
            className="ws-btn"
            style={{ width: '100%', marginTop: 8 }}
            onClick={navigateToMacroIntelligence}
          >
            Macro Intelligence
          </button>
          <button
            type="button"
            className="ws-btn"
            style={{ width: '100%', marginTop: 8 }}
            onClick={() => navigateToInstrument(DXY_MARKET)}
          >
            US Dollar Index / DX
          </button>
          <button
            type="button"
            className="ws-btn"
            style={{ width: '100%', marginTop: 8 }}
            onClick={navigateToDxyMacroBias}
          >
            DXY Macro Bias
          </button>
        </div>
      </aside>

      <div className="ws-main">
        <header className="ws-topbar">
          <div>
            <h1>{title}</h1>
            {subtitle ? <div className="ws-topbar-meta">{subtitle}</div> : null}
          </div>
          {showWeekPicker ? (
            <label className="ws-topbar-meta">
              Week
              <select
                className="ws-select"
                style={{ marginLeft: 6 }}
                value={date || weekDates[0] || ''}
                onChange={(e) => onDateChange(e.target.value)}
              >
                {weekDates.map((d) => (
                  <option key={d} value={d}>
                    {d}
                  </option>
                ))}
              </select>
            </label>
          ) : null}
          {latestCotReportDate ? (
            <span className="ws-topbar-meta">Latest COT bundle: {latestCotReportDate}</span>
          ) : null}
          {cotFeedStatus?.is_stale ? (
            <span
              className="ws-stale-badge"
              title={
                cotFeedStatus.latest_cftc_report_date
                  ? `Export week ${cotFeedStatus.latest_export_cot_week || '—'} is behind CFTC ${cotFeedStatus.latest_cftc_report_date}. Run: python -m hptl.cot.run_update`
                  : 'COT export may be behind latest CFTC release'
              }
            >
              COT stale
            </span>
          ) : null}
          {marketSwitcher}
          {topActions}
        </header>
        <main className={`ws-content${contentClassName ? ` ${contentClassName}` : ''}`}>{children}</main>
      </div>
    </div>
  )
}

export function filterMarketsBySidebar(marketIds, sidebarClass) {
  if (!sidebarClass || sidebarClass === 'all') return marketIds
  const allowed = new Set((marketsInAssetClass(sidebarClass) || []).map((m) => canonicalMarketId(m)))
  return marketIds.filter((m) => allowed.has(canonicalMarketId(m)))
}

export { assetClassForMarket, assetClassLabel }
