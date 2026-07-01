/**
 * Explain why a radar-eligible market is missing from the visible scanner table.
 */

import { isCotRowResolved } from './marketResolution.js'
import { isRadarEligible, radarSuppressionReason } from './radarEligibility.js'

/** Direct COT markets we always surface a status row for when filtered out. */
export const MONITORED_RADAR_MARKETS = [
  'Gold',
  'Silver',
  'Copper / HG',
  'Platinum',
  'Palladium',
  'Crude Oil / CL',
  'Wheat',
  'Bitcoin',
]

function passesVisibilityFilter(row, { showAllMarkets, radarEligibleOnly, showMacroAssets, showUnresolved }) {
  if (showAllMarkets) return true
  if (radarEligibleOnly && isRadarEligible(row.market, row)) return true
  if (showMacroAssets) return false
  if (showUnresolved && !isCotRowResolved(row)) return true
  if (!radarEligibleOnly && !showMacroAssets && !showUnresolved) return true
  return false
}

/**
 * @returns {string|null} human-readable exclusion reason
 */
export function explainScannerExclusion(row, filters) {
  if (!row) return 'No confluence row for selected calendar week'

  const suppression = radarSuppressionReason(row)
  if (suppression) {
    if (suppression === 'duplicate') return 'Suppressed as synthetic/proxy duplicate'
    if (suppression === 'derived') return 'Suppressed as leg-derived / macro-only row'
    return 'Not radar-eligible (orphaned or no COT mapping)'
  }

  if (!passesVisibilityFilter(row, filters.visibilityOpts)) {
    if (filters.visibilityOpts?.radarEligibleOnly && !isRadarEligible(row.market, row)) {
      return 'Hidden by visibility filter (not direct COT eligible)'
    }
    if (!isCotRowResolved(row)) return 'Hidden by visibility filter (COT unresolved for week)'
    return 'Hidden by visibility filter'
  }

  if (filters.assetFilter !== 'all' && row.assetClass !== filters.assetFilter) {
    return `Asset class filter (${filters.assetFilter})`
  }

  if (filters.stateFilter === 'attention' && (row.marketState?.attentionScore || 0) <= 5) {
    return `State filter: unusual states only (attention score ${row.marketState?.attentionScore ?? 0})`
  }
  if (filters.stateFilter === 'mixed' && row.marketState?.stateId !== 'MIXED') {
    return 'State filter: mixed/neutral only'
  }

  if (filters.cotFilter === 'available' && !row.resolved) return 'COT filter: available only'
  if (filters.cotFilter === 'unavailable' && row.resolved) return 'COT filter: unavailable only'

  if (filters.macroFilter !== 'all') {
    const align = String(row.macroAlign || '').toLowerCase()
    if (!align.includes(filters.macroFilter)) return `Macro filter (${filters.macroFilter})`
  }

  if (filters.watchOnly && !filters.watchlist.includes(row.market)) return 'Watchlist filter'

  const q = filters.search?.trim().toLowerCase()
  if (q) {
    const hay = `${row.market} ${row.marketState?.state || ''} ${row.marketState?.reason || ''}`.toLowerCase()
    if (!hay.includes(q)) return `Search filter (“${filters.search.trim()}”)`
  }

  return 'Grouped under parent or display deduplication'
}

/**
 * Monitored markets present in pool but not in final display rows.
 */
export function monitoredRadarGaps(poolRows, displayRows, filters) {
  const displaySet = new Set((displayRows || []).map((r) => r.market))
  const poolMap = new Map((poolRows || []).map((r) => [r.market, r]))

  return MONITORED_RADAR_MARKETS.map((market) => {
    const row = poolMap.get(market)
    if (!row) {
      return {
        market,
        status: 'missing_pool',
        reason: 'Not in confluence pool for selected week (check COT mapping / export)',
        cotResolved: false,
      }
    }
    if (displaySet.has(market)) return null
    return {
      market,
      status: 'filtered',
      reason: explainScannerExclusion(row, filters),
      cotResolved: isCotRowResolved(row),
      stateId: row.marketState?.stateId,
      attentionScore: row.marketState?.attentionScore,
      cotDate: row.cotDate || row.latest_report_date,
    }
  }).filter(Boolean)
}
