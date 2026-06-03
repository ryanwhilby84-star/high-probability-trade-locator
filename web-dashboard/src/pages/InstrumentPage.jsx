import React from 'react'
import { AppShell, filterMarketsBySidebar } from '../components/AppShell.jsx'
import { InstrumentDetail, buildMarketHistoryForMarket } from '../legacy/dashboardLegacy.jsx'
import { resolveMacroRelationshipMap } from '../macroRelationshipMapData.js'
import { recordCotReportDate } from '../marketResolution.js'
import { defaultPositioningSlug, isFinancialFuturesMarket } from '../cotPositioningConfig.js'
import { navigateToCotPositioning, navigateToInstrument, navigateToScanner } from '../routing.js'
import { deriveActionLabel } from '../marketCatalog.js'
import { MacroCatalystPanel } from '../components/MacroCatalystPanel.jsx'
import { MacroTransmissionPanel } from '../components/MacroTransmissionPanel.jsx'
import { CotUnavailablePanel } from '../components/CotUnavailablePanel.jsx'
import { PositioningStatusBadges } from '../components/PositioningStatusBadges.jsx'
import { isCotRowResolved } from '../marketResolution.js'
import { WeatherCropPanel } from '../components/WeatherCropPanel.jsx'
import { hasRealWeather, resolveWeatherForMarket } from '../weatherData.js'
import { buildJournalPrefill } from '../journal/journalPrefill.js'
import { LogTradeIdeaModal } from '../components/LogTradeIdeaModal.jsx'
import { InstrumentPricePanel } from '../components/InstrumentPricePanel.jsx'
import { OpportunityPillarsPanel } from '../components/OpportunityPillarsPanel.jsx'
import { LegacyCotPanel } from '../components/LegacyCotPanel.jsx'
import { useInstrumentPrices } from '../hooks/useInstrumentPrices.js'
import { addThesisFromRow, loadOverlay, removeThesis } from '../thesisTracker/thesisLocal.js'
import { navigateToThesisTracker } from '../routing.js'

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

  const historyRows = React.useMemo(
    () => buildMarketHistoryForMarket(data, marketId, date),
    [data, marketId, date],
  )

  const { data: priceData, loading: priceLoading, error: priceError } = useInstrumentPrices(marketId)

  const relationshipMapData = React.useMemo(
    () => resolveMacroRelationshipMap(macroRelationshipMaps, marketId) ?? null,
    [macroRelationshipMaps, marketId],
  )

  const cotDate = recordCotReportDate(row) || row.latest_report_date || '—'
  const action = deriveActionLabel(row)

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
        title={tracked ? 'Remove this market from the Thesis Tracker' : 'Add this market to the Thesis Tracker'}
      >
        {tracked ? '★ Tracking' : '☆ Track thesis'}
      </button>
      {tracked ? (
        <button type="button" className="ws-btn" onClick={navigateToThesisTracker}>
          Open tracker
        </button>
      ) : null}
      <button type="button" className="ws-btn inst-journal-btn" onClick={() => setJournalOpen(true)}>
        Log trade idea
      </button>
    </>
  )

  return (
    <AppShell
      title={marketId}
      subtitle={`COT report ${cotDate} · ${action}`}
      date={date}
      dates={dates}
      onDateChange={setDate}
      latestCotReportDate={latestCotReportDate}
      cotFeedStatus={cotFeedStatus}
      sidebarClass={sidebarClass}
      onSidebarClass={onSidebarClass}
      marketSwitcher={marketSwitcher}
      topActions={topActions}
    >
      <div className="inst-header">
        <PositioningStatusBadges row={row} />
        <p className="ws-topbar-meta" style={{ margin: 0 }}>
          Calendar week <strong>{date}</strong> ·{' '}
          {isCotRowResolved(row) ? 'positioning snapshot and trail below' : 'macro transmission (no direct COT)'}
        </p>
        <div className="inst-positioning-nav">
          <button
            type="button"
            className="ws-btn"
            onClick={() => navigateToCotPositioning(marketId, defaultPositioningSlug(marketId, row))}
          >
            Positioning graphs
          </button>
          <button
            type="button"
            className="ws-btn"
            onClick={() => navigateToCotPositioning(marketId, 'combined')}
          >
            Combined COT view
          </button>
          <span className="ws-topbar-meta">Legacy COT: Non-Commercial · Commercial · Non-Reportable</span>
        </div>
      </div>

      {!isCotRowResolved(row) ? (
        <CotUnavailablePanel row={row} marketId={marketId} />
      ) : (
        <MacroTransmissionPanel row={row} />
      )}

      <MacroCatalystPanel row={row} globalCalendar={economicCalendar} />

      <WeatherCropPanel row={row} weatherContext={weatherContext} weatherLoadError={weatherLoadError} />

      <InstrumentDetail
        row={row}
        historyRows={historyRows}
        peersByMarket={peersByMarket}
        globalMarketRegime={globalMarketRegime}
        relationshipMapData={relationshipMapData}
        hideWeatherPlaceholder={hideWeatherPlaceholder}
        economicCalendar={economicCalendar}
        weatherContext={weatherContext}
        weatherLoadError={weatherLoadError}
        workspaceMode
      />

      <OpportunityPillarsPanel row={row} />

      <InstrumentPricePanel prices={priceData} loading={priceLoading} error={priceError} />

      {row?.instrument_meta?.has_cot_mapping !== false ? (
        <LegacyCotPanel instrumentId={marketId} />
      ) : null}

      <LogTradeIdeaModal
        open={journalOpen}
        prefill={journalPrefill}
        onClose={() => setJournalOpen(false)}
        onSaved={() => setJournalOpen(false)}
      />
    </AppShell>
  )
}
