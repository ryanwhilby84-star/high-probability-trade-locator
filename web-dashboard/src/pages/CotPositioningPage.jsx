import React from 'react'
import { AppShell, filterMarketsBySidebar } from '../components/AppShell.jsx'
import { CotPositioningChart } from '../components/CotPositioningChart.jsx'
import { CotPositioningRawTable } from '../components/CotPositioningRawTable.jsx'
import {
  cotPositioningProfile,
  defaultPositioningSlug,
  normalizePositioningSlug,
  positioningGroupsForMarket,
  resolvePositioningGroup,
} from '../cotPositioningConfig.js'
import { buildCotPositioningHistory, buildPositioningChartSeries } from '../cotPositioningHistory.js'
import { resolveMacroRelationshipMap } from '../macroRelationshipMapData.js'
import { navigateToCotPositioning, navigateToInstrument, navigateToScanner } from '../routing.js'

const CHART_MODES = [
  { id: 'full', label: 'Long / short / net' },
  { id: 'net', label: 'Net only' },
  { id: 'bands', label: 'Net + min/max bands' },
  { id: 'price', label: 'Price overlay' },
  { id: 'raw', label: 'Raw table' },
]

function cohortUnavailableMessage(marketId, group) {
  if (group.id === 'combined') {
    return 'Combined view plots non-commercial, commercial, and non-reportable nets from Legacy COT when published for this contract.'
  }
  return `${group.tabLabel} is not available for this Legacy COT week on ${marketId}. Try another tab or rebuild confluence after legacy_cot_latest.json is refreshed.`
}

export function CotPositioningPage({ marketId, groupSlug, confluence, sidebarClass, onSidebarClass }) {
  const {
    data,
    date,
    dates,
    setDate,
    latestCotReportDate,
    cotFeedStatus,
    trackedMarkets,
    macroRelationshipMaps,
  } = confluence

  const [bandMode, setBandMode] = React.useState('13')
  const [weeksWindow, setWeeksWindow] = React.useState(52)
  const [chartMode, setChartMode] = React.useState('full')

  const historyRows = React.useMemo(
    () => buildCotPositioningHistory(data, marketId, weeksWindow),
    [data, marketId, weeksWindow],
  )

  const latest = historyRows[historyRows.length - 1]
  const profile = cotPositioningProfile(marketId, latest)
  const normalizedSlug = normalizePositioningSlug(marketId, groupSlug, latest)
  const group = resolvePositioningGroup(marketId, normalizedSlug, latest)
  const tabGroups = positioningGroupsForMarket(marketId, latest)

  React.useEffect(() => {
    if (normalizedSlug !== groupSlug) {
      navigateToCotPositioning(marketId, normalizedSlug)
    }
  }, [marketId, groupSlug, normalizedSlug])

  const relationshipMap = React.useMemo(
    () => resolveMacroRelationshipMap(macroRelationshipMaps, marketId) ?? null,
    [macroRelationshipMaps, marketId],
  )

  const chartData = React.useMemo(
    () => buildPositioningChartSeries(historyRows, group?.id || defaultPositioningSlug(marketId, latest), relationshipMap),
    [historyRows, group, relationshipMap, marketId, latest],
  )

  const navMarkets = React.useMemo(
    () => filterMarketsBySidebar(trackedMarkets, sidebarClass),
    [trackedMarkets, sidebarClass],
  )

  if (!group) {
    return (
      <AppShell title="COT positioning" subtitle="Unknown chart type">
        <p className="ws-error-banner">Unknown positioning chart route.</p>
        <button type="button" className="ws-btn" onClick={() => navigateToInstrument(marketId)}>
          Back to instrument
        </button>
      </AppShell>
    )
  }

  const latestGroup = latest?.cot_positioning_groups?.[group.id]
  const primaryScoringGroup = 'managed_money'
  const cohortUnavailable =
    group.id !== 'combined' &&
    latestGroup?.available === false &&
    group.id !== primaryScoringGroup &&
    !chartData.anyAvailable

  if (cohortUnavailable) {
    return (
      <AppShell
        title={marketId}
        subtitle={`${group.title} · not published for this report type`}
        date={date}
        dates={dates}
        onDateChange={setDate}
        latestCotReportDate={latestCotReportDate}
        cotFeedStatus={cotFeedStatus}
        sidebarClass={sidebarClass}
        onSidebarClass={onSidebarClass}
        topActions={
          <button type="button" className="ws-btn" onClick={() => navigateToInstrument(marketId)}>
            ← Instrument
          </button>
        }
      >
        <section className="cot-pos-page">
          <p className="cot-chart-empty">{cohortUnavailableMessage(marketId, group)}</p>
          <nav className="cot-pos-tabs" aria-label="Positioning chart type">
            {Object.values(tabGroups).map((g) => (
              <button
                key={g.routeSlug}
                type="button"
                className={`cot-pos-tab${g.routeSlug === normalizedSlug ? ' active' : ''}`}
                onClick={() => navigateToCotPositioning(marketId, g.routeSlug)}
              >
                {g.tabLabel}
              </button>
            ))}
          </nav>
        </section>
      </AppShell>
    )
  }

  const effectiveBandMode = chartMode === 'bands' ? bandMode : chartMode === 'net' ? 'off' : bandMode
  const showBandToolbar = chartMode === 'full' || chartMode === 'bands'

  return (
    <AppShell
      title={marketId}
      subtitle={`${group.title} · ${chartData.weeks} COT weeks · Legacy COT`}
      date={date}
      dates={dates}
      onDateChange={setDate}
      latestCotReportDate={latestCotReportDate}
      cotFeedStatus={cotFeedStatus}
      sidebarClass={sidebarClass}
      onSidebarClass={onSidebarClass}
      marketSwitcher={
        <label className="ws-topbar-meta">
          Market
          <select
            className="ws-select"
            style={{ marginLeft: 6, minWidth: 160 }}
            value={marketId}
            onChange={(e) => navigateToCotPositioning(e.target.value, normalizedSlug)}
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
        <>
          <button type="button" className="ws-btn" onClick={() => navigateToInstrument(marketId)}>
            ← Instrument
          </button>
          <button type="button" className="ws-btn" onClick={navigateToScanner}>
            Scanner
          </button>
        </>
      }
    >
      <section className="cot-pos-page">
        <header className="cot-pos-head">
          <div>
            <h2 className="cot-pos-title">{group.title}</h2>
            <p className="cot-pos-sub">{group.subtitle}</p>
          </div>
          <nav className="cot-pos-tabs" aria-label="Positioning chart type">
            {Object.values(tabGroups).map((g) => (
              <button
                key={g.routeSlug}
                type="button"
                className={`cot-pos-tab${g.routeSlug === normalizedSlug ? ' active' : ''}`}
                onClick={() => navigateToCotPositioning(marketId, g.routeSlug)}
              >
                {g.tabLabel}
              </button>
            ))}
          </nav>
        </header>

        <div className="cot-pos-toolbar">
          <label>
            View
            <select className="ws-select" value={chartMode} onChange={(e) => setChartMode(e.target.value)}>
              {CHART_MODES.filter((m) => m.id !== 'raw' || group.id !== 'combined').map((m) => (
                <option key={m.id} value={m.id}>
                  {m.label}
                </option>
              ))}
            </select>
          </label>
          <label>
            History window
            <select className="ws-select" value={weeksWindow} onChange={(e) => setWeeksWindow(Number(e.target.value))}>
              <option value={13}>13 weeks</option>
              <option value={26}>26 weeks</option>
              <option value={52}>52 weeks</option>
              <option value={104}>104 weeks</option>
            </select>
          </label>
          {showBandToolbar && group.id !== 'combined' ? (
            <label>
              Net bands
              <select className="ws-select" value={bandMode} onChange={(e) => setBandMode(e.target.value)}>
                <option value="13">13-week min/max</option>
                <option value="52">52-week min/max</option>
                <option value="both">13W + 52W</option>
                <option value="off">Off</option>
              </select>
            </label>
          ) : null}
          {chartData.hasPrice ? (
            <span className="cot-pos-meta">Price overlay: {chartData.priceLabel} (FRED rebased %)</span>
          ) : chartMode === 'price' ? (
            <span className="cot-pos-meta">Price overlay not available for this market</span>
          ) : null}
        </div>

        {chartMode === 'raw' && group.id !== 'combined' ? (
          <CotPositioningRawTable series={chartData.series} groupTitle={group.title} />
        ) : (
          <CotPositioningChart
            chartData={chartData}
            groupId={group.id}
            showBands={effectiveBandMode}
            chartMode={chartMode}
          />
        )}

        <div className="cot-pos-foot">
          {latestGroup?.available === false && group.id !== primaryScoringGroup ? (
            <p className="cot-pos-warn">
              Latest week has sparse {group.tabLabel} fields — older history may still plot from backfill.
            </p>
          ) : null}
          <p className="cot-pos-meta">
            Source: <code>data/legacy_cot_latest.json</code> — Legacy Futures Only (non-commercial, commercial,
            non-reportable). Headline scores use non-commercial via <code>long_value</code> / <code>short_value</code>.
          </p>
        </div>
      </section>
    </AppShell>
  )
}
