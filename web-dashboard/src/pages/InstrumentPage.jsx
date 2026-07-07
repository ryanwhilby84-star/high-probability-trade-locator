import React from 'react'
import { AppShell, filterMarketsBySidebar } from '../components/AppShell.jsx'
import { InstrumentDetail, buildMarketHistoryForMarket } from '../legacy/dashboardLegacy.jsx'
import { resolveMacroRelationshipMap } from '../macroRelationshipMapData.js'
import { recordCotReportDate, isCotRowResolved } from '../marketResolution.js'
import { navigateToInstrument, navigateToScanner, navigateToThesisTracker, navigateToCotWorkstation } from '../routing.js'
import { CotUnavailablePanel } from '../components/CotUnavailablePanel.jsx'
import { ValuationInstrumentSection } from '../components/IVECalculationPanel.jsx'
import { InstrumentWorkstationLayout } from '../workstation/InstrumentWorkstationLayout.jsx'
import { InstrumentPositioningWorkspace } from '../components/InstrumentPositioningWorkspace.jsx'
import { hasRealWeather, resolveWeatherForMarket } from '../weatherData.js'
import { buildJournalPrefill } from '../journal/journalPrefill.js'
import { LogTradeIdeaModal } from '../components/LogTradeIdeaModal.jsx'
import { addThesisFromRow, loadOverlay, removeThesis } from '../thesisTracker/thesisLocal.js'
import { isTffMacroInstrument, tffHistoryRows } from '../tffMacroPositioning.js'

function scrollToPositioningWorkspace() {
  document.getElementById('instrument-positioning-workspace')?.scrollIntoView({ behavior: 'smooth', block: 'start' })
}

function scrollToValuationEvidence() {
  document.getElementById('valuation-evidence')?.scrollIntoView({ behavior: 'smooth', block: 'start' })
}

export function InstrumentPage({ marketId, confluence, sidebarClass, onSidebarClass }) {
  const {
    data,
    date,
    dates,
    setDate,
    latestCotReportDate,
    cotFeedStatus,
    marketRows,
    peersByMarket,
    globalMarketRegime,
    macroRelationshipMaps,
    trackedMarkets,
    economicCalendar,
    weatherContext,
    weatherLoadError,
    tffDoc,
  } = confluence

  const row = React.useMemo(
    () => marketRows.find((r) => r.market === marketId) || { market: marketId },
    [marketRows, marketId],
  )

  const weatherResolved = React.useMemo(
    () => resolveWeatherForMarket(row, weatherContext, { loadError: weatherLoadError }),
    [row, weatherContext, weatherLoadError],
  )
  const hideWeatherPlaceholder = hasRealWeather(weatherResolved)
  const [journalOpen, setJournalOpen] = React.useState(false)
  const journalPrefill = React.useMemo(
    () =>
      buildJournalPrefill({
        row,
        date,
        weatherContext,
        economicCalendar,
      }),
    [row, date, weatherContext, economicCalendar],
  )

  const historyRows = React.useMemo(() => {
    if (isTffMacroInstrument(marketId) && tffDoc) {
      const tffHist = tffHistoryRows(tffDoc, marketId)
      if (tffHist.length) return tffHist
    }
    return buildMarketHistoryForMarket(data, marketId, date)
  }, [data, marketId, date, tffDoc])

  const relationshipMapData = React.useMemo(
    () => resolveMacroRelationshipMap(macroRelationshipMaps, marketId) ?? null,
    [macroRelationshipMaps, marketId],
  )

  const cotDate = recordCotReportDate(row) || row.latest_report_date || '—'

  const [tracked, setTracked] = React.useState(false)
  const [trackedId, setTrackedId] = React.useState(null)
  React.useEffect(() => {
    const overlay = loadOverlay()
    const local = overlay.added.find((t) => t.market === marketId)
    setTracked(Boolean(local))
    setTrackedId(local?.thesis_id || null)
  }, [marketId])

  const handleTrack = React.useCallback(() => {
    if (tracked && trackedId) {
      removeThesis(trackedId)
      setTracked(false)
      setTrackedId(null)
      return
    }
    const t = addThesisFromRow({ market: marketId, row, week: date })
    setTracked(true)
    setTrackedId(t?.thesis_id || null)
  }, [tracked, trackedId, marketId, row, date])

  const navMarkets = React.useMemo(
    () => filterMarketsBySidebar(trackedMarkets, sidebarClass),
    [trackedMarkets, sidebarClass],
  )

  const navIndex = navMarkets.indexOf(marketId)
  const prevMarket = navIndex > 0 ? navMarkets[navIndex - 1] : null
  const nextMarket = navIndex >= 0 && navIndex < navMarkets.length - 1 ? navMarkets[navIndex + 1] : null

  const confluenceRecordsForMarket = React.useMemo(
    () => (data?.records || []).filter((r) => r.market === marketId),
    [data?.records, marketId],
  )

  React.useEffect(() => {
    let scroll = false
    try {
      scroll = sessionStorage.getItem('scrollToValuation') === '1'
      if (scroll) sessionStorage.removeItem('scrollToValuation')
    } catch {
      /* ignore */
    }
    if (!scroll) return undefined
    const t = window.setTimeout(scrollToValuationEvidence, 120)
    return () => window.clearTimeout(t)
  }, [marketId])

  const marketSwitcher = (
    <label className="ws-topbar-meta">
      Market
      <select
        className="ws-select"
        style={{ marginLeft: 6, minWidth: 160 }}
        value={marketId}
        onChange={(e) => navigateToInstrument(e.target.value)}
      >
        {navMarkets.map((m) => (
          <option key={m} value={m}>
            {m}
          </option>
        ))}
      </select>
    </label>
  )

  const topActions = (
    <>
      <button type="button" className="ws-btn" onClick={navigateToScanner}>
        ← Scanner
      </button>
      <button
        type="button"
        className="ws-btn"
        disabled={!prevMarket}
        onClick={() => prevMarket && navigateToInstrument(prevMarket)}
      >
        Prev
      </button>
      <button
        type="button"
        className="ws-btn"
        disabled={!nextMarket}
        onClick={() => nextMarket && navigateToInstrument(nextMarket)}
      >
        Next
      </button>
      <button
        type="button"
        className={`ws-btn${tracked ? ' ws-btn-primary' : ''}`}
        onClick={handleTrack}
      >
        {tracked ? '★ Tracking' : '☆ Track thesis'}
      </button>
      <button type="button" className="ws-btn" onClick={() => setJournalOpen(true)}>
        Log trade idea
      </button>
      <button type="button" className="ws-btn ws-btn-primary" onClick={() => navigateToCotWorkstation(marketId)}>
        Open COT Workstation
      </button>
      <button type="button" className="ws-btn" onClick={scrollToPositioningWorkspace}>
        Positioning data
      </button>
      {tracked ? (
        <button type="button" className="ws-btn" onClick={navigateToThesisTracker}>
          Open tracker
        </button>
      ) : null}
    </>
  )

  return (
    <AppShell
      title={marketId}
      subtitle={`Week ${date} · COT ${cotDate}`}
      date={date}
      dates={dates}
      onDateChange={setDate}
      latestCotReportDate={latestCotReportDate}
      cotFeedStatus={cotFeedStatus}
      sidebarClass={sidebarClass}
      onSidebarClass={onSidebarClass}
      marketSwitcher={marketSwitcher}
      topActions={topActions}
      contentClassName="ws-content--instrument-canvas"
    >
      {!isCotRowResolved(row) ? <CotUnavailablePanel row={row} marketId={marketId} /> : null}

      <InstrumentWorkstationLayout>
        <ValuationInstrumentSection row={row} />
        <InstrumentPositioningWorkspace
          marketId={marketId}
          headlineRow={row}
          confluenceRecords={confluenceRecordsForMarket}
          asOfDate={date}
        />

        <details className="instrument-page-detail-collapse" id="valuation-evidence">
          <summary className="instrument-page-detail-summary">
            Valuation, seasonality &amp; market context
          </summary>
          <InstrumentDetail
            row={row}
            historyRows={historyRows}
            peersByMarket={peersByMarket}
            globalMarketRegime={globalMarketRegime}
            relationshipMapData={relationshipMapData}
            hideWeatherPlaceholder={hideWeatherPlaceholder}
            workspaceMode
            economicCalendar={economicCalendar}
            weatherContext={weatherContext}
            weatherLoadError={weatherLoadError}
          />
        </details>
      </InstrumentWorkstationLayout>

      <LogTradeIdeaModal
        open={journalOpen}
        prefill={journalPrefill}
        onClose={() => setJournalOpen(false)}
        onSaved={() => setJournalOpen(false)}
      />
    </AppShell>
  )
}
