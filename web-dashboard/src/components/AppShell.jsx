import React from 'react'
import { getAssetClasses, assetClassForMarket, assetClassLabel, marketsInAssetClass } from '../marketCatalog.js'
import { canonicalMarketId } from '../marketResolution.js'
import {
  navigateToJournal,
  navigateToOandaCoverage,
  navigateToCotProof,
  navigateToCotSourceTruth,
  navigateToDataLineage,
  navigateToPriceCoverage,
  navigateToScanner,
  navigateToThesisTracker,
  navigateToNaturalGasValuation,
  navigateToDxyMacroBias,
  navigateToGoldValuation,
  navigateToInstrument,
  navigateToMacroHub,
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
        <div style={{ padding: '8px 10px 14px', borderTop: '1px solid rgba(255,255,255,0.12)' }}>
          <button type="button" className="ws-btn ws-btn-primary" style={{ width: '100%' }} onClick={navigateToScanner}>
            Scanner
          </button>
          <button type="button" className="ws-btn" style={{ width: '100%', marginTop: 8 }} onClick={navigateToMacroHub}>
            Macro Hub
          </button>
          <button type="button" className="ws-btn" style={{ width: '100%', marginTop: 8 }} onClick={navigateToThesisTracker}>
            Thesis Tracker
          </button>
          <button type="button" className="ws-btn" style={{ width: '100%', marginTop: 8 }} onClick={navigateToJournal}>
            Trade Journal
          </button>
          <button type="button" className="ws-btn ws-btn-primary" style={{ width: '100%', marginTop: 8 }} onClick={navigateToDataLineage}>
            Data Lineage
          </button>
          <button type="button" className="ws-btn" style={{ width: '100%', marginTop: 8 }} onClick={navigateToCotSourceTruth}>
            COT Source Truth
          </button>
          <button type="button" className="ws-btn" style={{ width: '100%', marginTop: 8 }} onClick={navigateToCotProof}>
            COT Proof (HTPL)
          </button>
          <button type="button" className="ws-btn" style={{ width: '100%', marginTop: 8 }} onClick={navigateToPriceCoverage}>
            Price Coverage Audit
          </button>
          <button type="button" className="ws-btn" style={{ width: '100%', marginTop: 8 }} onClick={navigateToOandaCoverage}>
            OANDA coverage
          </button>
          <button
            type="button"
            className="ws-btn ws-btn-primary"
            style={{ width: '100%', marginTop: 8 }}
            onClick={navigateToNaturalGasValuation}
          >
            NG Valuation
          </button>
          <button
            type="button"
            className="ws-btn ws-btn-primary"
            style={{ width: '100%', marginTop: 8 }}
            onClick={navigateToGoldValuation}
          >
            Gold Valuation
          </button>
          <button
            type="button"
            className="ws-btn ws-btn-primary"
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
