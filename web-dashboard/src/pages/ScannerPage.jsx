import React from 'react'
import { AppShell, filterMarketsBySidebar, assetClassForMarket, assetClassLabel } from '../components/AppShell.jsx'
import { getAssetClasses } from '../marketCatalog.js'
import { CommercialAttentionPanel } from '../components/CommercialAttentionPanel.jsx'
// Phase 6 UI cleanup: RelativeStrengthPanel / PriorityMarketsPanel retained on disk but hidden from Scanner.
import { deriveActionLabel, positioningShiftMagnitude, loadWatchlist, toggleWatchlist } from '../marketCatalog.js'
import {
  priorityClass,
  priorityLabel,
  priorityTier,
  sortByPriority,
  attentionAlerts,
  tacticalReadable,
} from '../marketAttention.js'
import { dominantNarrative, hasInstitutionalContext, structuralBiasTone } from '../institutionalContext.js'
import { macroAlignmentFromRow, positioningStatus } from '../instrumentRegistry.js'
import { PositioningStatusBadges } from '../components/PositioningStatusBadges.jsx'
import { catalystSummaryFromRow } from '../liveFeedStatus.js'
import { navigateToInstrument } from '../routing.js'
import { isCotRowResolved, recordCotReportDate, canonicalMarketId } from '../marketResolution.js'
import { eventRiskBadge, eventRiskLabel } from '../macroCalendarCatalyst.js'
import { isRadarEligible } from '../radarEligibility.js'

const display = (v) => (v === null || v === undefined || v === '' ? '—' : v)

const fmtNum = (v) => {
  if (v === null || v === undefined || v === '' || v === 'N/A') return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return String(v)
  return n.toLocaleString(undefined, { maximumFractionDigits: 0 })
}

function MarketRowDetail({ row, expanded }) {
  if (!expanded || !hasInstitutionalContext(row)) return null
  const ctx = row.institutional_context?.scanner_display
  const tx =
    row?.macro_transmission ||
    row?.institutional_context?.macro_transmission
  if (!ctx?.lines?.length && !tx?.headline) return null
  return (
    <>
      {tx?.headline ? (
        <p className="scanner-macro-tx" title={tx.asset_alignment_label || ''}>
          <strong>Macro transmission.</strong> {tx.headline}
        </p>
      ) : null}
      {ctx?.lines?.length ? (
        <dl className="scanner-context-dl scanner-context-dl-compact">
          {ctx.lines.map(({ layer, value, detail }) => (
            <div key={layer} className="scanner-context-row">
              <dt>{layer}</dt>
              <dd>
                {display(value)}
                {detail ? <span className="scanner-context-detail">{detail}</span> : null}
              </dd>
            </div>
          ))}
        </dl>
      ) : null}
    </>
  )
}

export function ScannerPage({
  marketRows,
  trackedMarkets,
  date,
  dates,
  setDate,
  latestCotReportDate,
  cotFeedStatus,
  loading,
  error,
  scannerAttentionWeek: _scannerAttentionWeek,
  priorityDebug: _priorityDebug,
  commercialAttention,
  relativeStrength: _relativeStrength,
  payloadGeneratedAt,
  economicCalendar,
  weatherContext,
  weatherLoadError,
  sidebarClass: sidebarClassProp,
  onSidebarClass: onSidebarClassProp,
}) {
  const [search, setSearch] = React.useState('')
  const [assetFilter, setAssetFilter] = React.useState('all')
  const [biasFilter, setBiasFilter] = React.useState('all')
  const [priorityFilter, setPriorityFilter] = React.useState('all')
  const [cotFilter, setCotFilter] = React.useState('all')
  const [macroFilter, setMacroFilter] = React.useState('all')
  const [tacticalFilter, setTacticalFilter] = React.useState('all')
  const [subgroupFilter, setSubgroupFilter] = React.useState('all')
  const [highAttentionOnly, setHighAttentionOnly] = React.useState(false)
  const [watchOnly, setWatchOnly] = React.useState(false)
  const [radarEligibleOnly, setRadarEligibleOnly] = React.useState(true)
  const [sortKey, setSortKey] = React.useState('priority')
  const [expanded, setExpanded] = React.useState(() => new Set())
  const [sidebarLocal, setSidebarLocal] = React.useState('all')
  const sidebarClass = sidebarClassProp ?? sidebarLocal
  const onSidebarClass = onSidebarClassProp ?? setSidebarLocal
  const [watchlist, setWatchlist] = React.useState(loadWatchlist)

  const onStar = (e, market) => {
    e.stopPropagation()
    setWatchlist(toggleWatchlist(market))
  }

  const toggleExpand = (e, market) => {
    e.stopPropagation()
    setExpanded((prev) => {
      const next = new Set(prev)
      if (next.has(market)) next.delete(market)
      else next.add(market)
      return next
    })
  }

  // Phase 6: Priority Markets board computation removed from UI (Commercial Attention is the ranking surface).
  // Props scannerAttentionWeek / priorityDebug / relativeStrength retained for call-site compatibility.

  const rows = React.useMemo(() => {
    let list = marketRows.map((r) => ({
      ...r,
      action: deriveActionLabel(r),
      narrative: dominantNarrative(r),
      alerts: attentionAlerts(r),
      tactical: tacticalReadable(r),
      tier: priorityTier(r),
      tierLabel: priorityLabel(r),
      tierClass: priorityClass(r),
      cotDate: recordCotReportDate(r) || r.latest_report_date,
      assetClass: assetClassForMarket(r.market),
      resolved: isCotRowResolved(r),
      positioningStatus: positioningStatus(r),
      macroAlign: macroAlignmentFromRow(r),
      shift: positioningShiftMagnitude(r),
      eventRisk: eventRiskBadge(r, economicCalendar),
    }))

    const sidebarIds = new Set(
      filterMarketsBySidebar(trackedMarkets, sidebarClass).map((m) => canonicalMarketId(m)),
    )
    list = list.filter((r) => sidebarIds.has(canonicalMarketId(r.market)))

    if (radarEligibleOnly) list = list.filter((r) => isRadarEligible(r.market))

    if (assetFilter !== 'all') list = list.filter((r) => r.assetClass === assetFilter)
    if (biasFilter === 'bull') list = list.filter((r) => structuralBiasTone(r) === 'bull')
    if (biasFilter === 'bear') list = list.filter((r) => structuralBiasTone(r) === 'bear')
    if (priorityFilter === 'high') list = list.filter((r) => r.tier === 'high_attention')
    if (priorityFilter === 'developing') list = list.filter((r) => r.tier === 'developing')
    if (priorityFilter === 'watchlist') list = list.filter((r) => r.tier === 'watchlist')
    if (priorityFilter === 'low') list = list.filter((r) => r.tier === 'low_priority')
    if (highAttentionOnly) list = list.filter((r) => r.tier === 'high_attention')
    if (cotFilter === 'available') list = list.filter((r) => r.resolved)
    if (cotFilter === 'unavailable') list = list.filter((r) => !r.resolved)
    if (macroFilter !== 'all') {
      list = list.filter((r) => String(r.macroAlign || '').toLowerCase().includes(macroFilter))
    }
    if (tacticalFilter !== 'all') {
      list = list.filter((r) => String(r.tactical || '').toLowerCase().includes(tacticalFilter))
    }
    if (subgroupFilter !== 'all') {
      list = list.filter((r) => positioningStatus(r) === subgroupFilter || r.assetClass === subgroupFilter)
    }
    if (watchOnly) list = list.filter((r) => watchlist.includes(r.market))

    const q = search.trim().toLowerCase()
    if (q) list = list.filter((r) => r.market.toLowerCase().includes(q) || (r.narrative || '').toLowerCase().includes(q))

    if (sortKey === 'priority') return sortByPriority(list)
    if (sortKey === 'net') {
      return [...list].sort((a, b) => Math.abs(Number(b.one_week_net_change) || 0) - Math.abs(Number(a.one_week_net_change) || 0))
    }
    if (sortKey === 'shift') {
      return [...list].sort((a, b) => b.shift - a.shift)
    }
    return [...list].sort((a, b) => a.market.localeCompare(b.market))
  }, [
    marketRows,
    trackedMarkets,
    sidebarClass,
    assetFilter,
    biasFilter,
    priorityFilter,
    watchOnly,
    watchlist,
    search,
    sortKey,
    economicCalendar,
    cotFilter,
    macroFilter,
    tacticalFilter,
    subgroupFilter,
    highAttentionOnly,
    radarEligibleOnly,
  ])

  return (
    <AppShell
      title="Market radar"
      subtitle={`Attention triage · ${rows.length} instruments · week ${date}`}
      date={date}
      dates={dates}
      onDateChange={setDate}
      latestCotReportDate={latestCotReportDate}
      sidebarClass={sidebarClass}
      onSidebarClass={onSidebarClass}
    >
      {loading ? <p className="ws-topbar-meta">Loading confluence data…</p> : null}
      {error ? <p className="ws-error-banner">Data error: {error}</p> : null}
      {cotFeedStatus?.is_stale ? (
        <p className="ws-error-banner" role="status">
          COT data is stale: dashboard export{' '}
          <strong>{cotFeedStatus.latest_export_cot_week || latestCotReportDate || '—'}</strong>
          {cotFeedStatus.latest_cftc_report_date ? (
            <>
              {' '}
              vs latest CFTC week <strong>{cotFeedStatus.latest_cftc_report_date}</strong>
            </>
          ) : null}
          . Run <code>python -m hptl.cot.run_update</code> from the project root.
        </p>
      ) : null}

      <CommercialAttentionPanel doc={commercialAttention} radarEligibleOnly={radarEligibleOnly} topN={8} />

      <div className="ws-toolbar">
        <input
          className="ws-input"
          type="search"
          placeholder="Search market or narrative…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          aria-label="Search markets"
        />
        <label>
          Priority
          <select className="ws-select" value={priorityFilter} onChange={(e) => setPriorityFilter(e.target.value)}>
            <option value="all">All</option>
            <option value="high">High attention</option>
            <option value="developing">Developing</option>
            <option value="watchlist">Watchlist</option>
            <option value="low">Low / ignore</option>
          </select>
        </label>
        <label>
          Asset class
          <select className="ws-select" value={assetFilter} onChange={(e) => setAssetFilter(e.target.value)}>
            <option value="all">All</option>
            {getAssetClasses().map((ac) => (
              <option key={ac.id} value={ac.id}>
                {ac.label} ({ac.markets?.length || 0})
              </option>
            ))}
          </select>
        </label>
        <label>
          COT
          <select className="ws-select" value={cotFilter} onChange={(e) => setCotFilter(e.target.value)}>
            <option value="all">All</option>
            <option value="available">COT available</option>
            <option value="unavailable">COT unavailable</option>
          </select>
        </label>
        <label>
          Macro
          <select className="ws-select" value={macroFilter} onChange={(e) => setMacroFilter(e.target.value)}>
            <option value="all">All</option>
            <option value="supportive">Supportive</option>
            <option value="headwind">Headwind</option>
            <option value="neutral">Neutral</option>
            <option value="contradiction">Contradiction</option>
          </select>
        </label>
        <label>
          <input type="checkbox" checked={highAttentionOnly} onChange={(e) => setHighAttentionOnly(e.target.checked)} />{' '}
          High attention only
        </label>
        <label>
          Structure
          <select className="ws-select" value={biasFilter} onChange={(e) => setBiasFilter(e.target.value)}>
            <option value="all">All</option>
            <option value="bull">Structural bull</option>
            <option value="bear">Structural bear</option>
          </select>
        </label>
        <label>
          <input type="checkbox" checked={watchOnly} onChange={(e) => setWatchOnly(e.target.checked)} /> Watchlist only
        </label>
        <label title="Show only canonical primary assets with valid coverage. Suppressed: duplicates, derived crosses, orphaned and no-data instruments. Scoring still runs for all instruments in the background.">
          <input
            type="checkbox"
            checked={radarEligibleOnly}
            onChange={(e) => setRadarEligibleOnly(e.target.checked)}
          />{' '}
          Radar-eligible only
        </label>
        <label>
          Sort
          <select className="ws-select" value={sortKey} onChange={(e) => setSortKey(e.target.value)}>
            <option value="priority">Attention priority</option>
            <option value="market">Market name</option>
            <option value="net">Net weekly change</option>
            <option value="shift">Positioning shift</option>
          </select>
        </label>
      </div>

      <div className="scanner-table-wrap">
        <table className="scanner-table scanner-table-triage">
          <thead>
            <tr>
              <th aria-label="Favourite" />
              <th>Priority</th>
              <th>Market</th>
              <th>Dominant narrative</th>
              <th>Net Δ</th>
              <th aria-label="Expand" />
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => {
              const isOpen = expanded.has(r.market)
              const low = r.tier === 'low_priority'
              return (
                <React.Fragment key={r.market}>
                  <tr
                    className={`scanner-triage-row ${r.tierClass}${low ? ' row-muted' : ''}`}
                    onClick={() => navigateToInstrument(r.market)}
                  >
                    <td>
                      <button
                        type="button"
                        className={`scanner-star${watchlist.includes(r.market) ? ' on' : ''}`}
                        onClick={(e) => onStar(e, r.market)}
                        aria-label={watchlist.includes(r.market) ? 'Remove from watchlist' : 'Add to watchlist'}
                      >
                        ★
                      </button>
                    </td>
                    <td>
                      <span className={`priority-tier-pill ${r.tierClass}`}>{r.tierLabel}</span>
                    </td>
                    <td>
                      <div className="scanner-market">{r.market}</div>
                      <PositioningStatusBadges row={r} compact />
                      <div className="scanner-market-meta">
                        {assetClassLabel(r.assetClass)} · {r.resolved ? `COT ${display(r.cotDate)}` : 'Macro only'}
                      </div>
                      {r.alerts?.length ? (
                        <ul className="scanner-alert-chips">
                          {r.alerts.slice(0, 2).map((a) => (
                            <li key={a.text}>
                              <span className="scanner-alert-chip">
                                {a.icon} {a.text}
                              </span>
                            </li>
                          ))}
                        </ul>
                      ) : null}
                    </td>
                    <td className="scanner-narrative-cell">
                      <p className="scanner-dominant-narrative">{r.narrative || '—'}</p>
                      {r.tactical ? <p className="scanner-tactical-line">{r.tactical}</p> : null}
                    </td>
                    <td className="scanner-net-cell">{fmtNum(r.one_week_net_change)}</td>
                    <td>
                      <button
                        type="button"
                        className="scanner-expand-btn"
                        onClick={(e) => toggleExpand(e, r.market)}
                        aria-expanded={isOpen}
                      >
                        {isOpen ? '−' : '+'}
                      </button>
                    </td>
                  </tr>
                  {isOpen ? (
                    <tr className="scanner-detail-row">
                      <td colSpan={6}>
                        <MarketRowDetail row={r} expanded />
                      </td>
                    </tr>
                  ) : null}
                </React.Fragment>
              )
            })}
          </tbody>
        </table>
      </div>
    </AppShell>
  )
}
