/**
 * Static macro driver definitions for the Macro Relationship Map (UI).
 * Correlation coefficients are never fabricated — correlationAvailable stays false until wired.
 */

import { getSeriesPair } from './macroRelationshipChartNarrative.js'

const NA = false

/** Backend keys for `macro_relationship_maps` (must stay aligned with `macro_relationship_maps.py`). */
export const MACRO_RELATIONSHIP_MAP_MARKETS = new Set([
  'NASDAQ / NQ',
  'S&P 500 / ES',
  'Gold',
  'Silver',
  'Copper / HG',
  'Crude Oil / CL',
  'Natural Gas / NG',
  'Coffee',
  'Cocoa',
  'Wheat',
  'Corn',
  'Soybeans',
])

/** @param {string} market */
function canonMarketKey(market = '') {
  const m = String(market || '').toLowerCase()
  if (m.includes('nasdaq') || m.includes('/ nq')) return 'NASDAQ / NQ'
  if (m.includes('s&p') || m.includes('sp 500') || m.includes('/ es')) return 'S&P 500 / ES'
  if (m.includes('dow') || m.includes('djia') || m.includes('/ ym')) return 'Dow / YM'
  if (m.includes('gold') || m.includes('/ gc')) return 'Gold'
  if (m.includes('silver') || m.includes('/ si')) return 'Silver'
  if (m.includes('copper') || m.includes('/ hg')) return 'Copper / HG'
  if (m.includes('crude oil') || m.includes('/ cl')) return 'Crude Oil / CL'
  if (m.includes('natural gas') || m.includes('/ ng')) return 'Natural Gas / NG'
  if (m.includes('coffee') || m.includes('/ kc')) return 'Coffee'
  if (m.includes('cocoa') || m.includes('/ cc')) return 'Cocoa'
  if (m.includes('corn') || m.includes('/ zc')) return 'Corn'
  if (m.includes('wheat') || m.includes('/ zw')) return 'Wheat'
  if (m.includes('soybeans') || m.includes('/ zs')) return 'Soybeans'
  return String(market || '').trim()
}

/**
 * @param {Record<string, unknown>|null|undefined} maps
 * @param {string} rowMarket
 */
export function resolveMacroRelationshipMap(maps, rowMarket) {
  if (!maps || typeof maps !== 'object' || rowMarket == null || rowMarket === '') return undefined
  const raw = String(rowMarket).trim()
  if (maps[raw]) return maps[raw]
  const want = canonMarketKey(raw)
  for (const k of Object.keys(maps)) {
    if (canonMarketKey(k) === want) return maps[k]
  }
  return undefined
}

/** @param {string} market */
export function expectsMacroRelationshipMap(market) {
  return MACRO_RELATIONSHIP_MAP_MARKETS.has(canonMarketKey(market))
}

/** @param {string} a @param {string} b */
export function marketsMacroAlign(a, b) {
  return canonMarketKey(a) === canonMarketKey(b)
}

/** @typedef {{ id: string, label: string, relationship: string, correlationAvailable: boolean, cohortNote?: string }} MacroDriver */

/** @type {Record<string, MacroDriver[]>} */
const DRIVERS_BY_MARKET = {
  'NASDAQ / NQ': [
    { id: 'dgs10', label: 'US 10Y Treasury yield', relationship: 'Usually moves opposite growth-heavy equity beta', correlationAvailable: NA, cohortNote: 'Same-week Treasury context when the macro panel includes it' },
    { id: 'dgs2', label: '2Y Treasury yield', relationship: 'Typically inverse vs duration; policy path sensitivity', correlationAvailable: NA, cohortNote: '2Y yield appears in macro audit when present' },
    { id: 'dxy', label: 'DXY (USD index)', relationship: 'Often inverse for large-cap exporters', correlationAvailable: NA, cohortNote: 'Not in this bundle — monitor separately' },
    { id: 'vix', label: 'VIX (implied vol)', relationship: 'Typically inverse for cash equity beta', correlationAvailable: NA, cohortNote: 'Not in dataset — wire vol surface for live read' },
  ],
  'S&P 500 / ES': [
    { id: 'dgs10', label: 'US 10Y Treasury yield', relationship: 'Usually a headwind when rising for broad equity beta', correlationAvailable: NA, cohortNote: 'Same-week rates context when present' },
    { id: 'dgs2', label: 'US 2Y Treasury yield', relationship: 'Fed path sensitivity for risk assets', correlationAvailable: NA },
    { id: 'dxy', label: 'Broad US dollar index', relationship: 'Stronger dollar often pressures multinationals', correlationAvailable: NA },
    { id: 'vix', label: 'VIX (implied volatility)', relationship: 'Higher vol often coincides with de-risking', correlationAvailable: NA, cohortNote: 'Wire VIX series for live read' },
  ],
  'Dow / YM': [
    { id: 'dgs10', label: '10Y Treasury yield', relationship: 'Cyclical value tilt — less pure duration than NQ', correlationAvailable: NA },
    { id: 'curve', label: 'Curve (2s10s / steepener)', relationship: 'Bank / reflation channel', correlationAvailable: NA, cohortNote: '2s10s spread in macro audit when available' },
    { id: 'dxy', label: 'DXY', relationship: 'Mixed for multinationals', correlationAvailable: NA },
    { id: 'risk', label: 'Global risk appetite', relationship: 'Positive for cyclical Dow beta', correlationAvailable: NA, cohortNote: 'Proxy via intermarket + regime text' },
  ],
  Gold: [
    { id: 'dxy', label: 'Broad US dollar index', relationship: 'Stronger dollar often weighs on dollar-priced gold', correlationAvailable: NA },
    { id: 'dgs10', label: 'US 10Y Treasury yield', relationship: 'Nominal yields matter alongside real-rate story', correlationAvailable: NA },
    { id: 'real_yield', label: 'Real yields (when wired)', relationship: 'Higher real yields often pressure non-yielding bullion', correlationAvailable: NA, cohortNote: 'TIPS / real-rate series optional' },
    { id: 'risk', label: 'Risk-off flows', relationship: 'Safe-haven bid can override rates for stretches', correlationAvailable: NA, cohortNote: 'Use regime + headline scan' },
  ],
  Silver: [
    { id: 'gold', label: 'Gold (import price index)', relationship: 'Silver often tracks the precious leg with gold', correlationAvailable: NA },
    { id: 'dxy', label: 'Broad US dollar index', relationship: 'Typically inverse for precious channel', correlationAvailable: NA },
    { id: 'dgs10', label: 'US 10Y Treasury yield', relationship: 'Industrial + precious mix — yields matter but not alone', correlationAvailable: NA },
    { id: 'copper', label: 'Copper / growth tone', relationship: 'Industrial demand read via related markets', correlationAvailable: NA, cohortNote: 'Intermarket + drivers list' },
  ],
  'Copper / HG': [
    { id: 'dgs10', label: 'US 10Y Treasury yield', relationship: 'Growth vs financial conditions — mixed read', correlationAvailable: NA },
    { id: 'dxy', label: 'Broad US dollar index', relationship: 'Often inverse for invoiced metals', correlationAvailable: NA },
    { id: 'china', label: 'China / manufacturing tone', relationship: 'Demand proxy for industrial bellwether', correlationAvailable: NA, cohortNote: 'Intermarket + macro impact lines on this row' },
    { id: 'risk', label: 'Global risk sentiment', relationship: 'Typically positive for cyclical beta', correlationAvailable: NA },
  ],
  'Crude Oil / CL': [
    { id: 'dxy', label: 'Broad US dollar index', relationship: 'Stronger dollar often pressures dollar-priced crude', correlationAvailable: NA },
    { id: 'dgs10', label: 'US 10Y Treasury yield', relationship: 'Growth / liquidity backdrop', correlationAvailable: NA },
    { id: 'inventories', label: 'Inventories (EIA)', relationship: 'Balances path — event-heavy', correlationAvailable: NA, cohortNote: 'Keyword scan until series merge' },
    { id: 'risk', label: 'Global growth expectations', relationship: 'Demand narrative channel', correlationAvailable: NA },
  ],
  'Natural Gas / NG': [
    { id: 'wti', label: 'WTI crude oil', relationship: 'Energy complex correlation — sentiment link', correlationAvailable: NA },
    { id: 'storage', label: 'Storage vs seasonal norm', relationship: 'Henry Hub core driver', correlationAvailable: NA, cohortNote: 'EIA storage — calendar keywords' },
    { id: 'weather', label: 'Weather (HDD/CDD)', relationship: 'Demand shock channel', correlationAvailable: NA },
    { id: 'lng', label: 'LNG / export headlines', relationship: 'Arbitrage / feedgas narrative', correlationAvailable: NA },
  ],
  Wheat: [
    { id: 'dgs10', label: 'US 10Y Treasury yield', relationship: 'Macro / financial conditions backdrop for ags', correlationAvailable: NA },
    { id: 'dxy', label: 'Broad US dollar index', relationship: 'Export competitiveness — often inverse', correlationAvailable: NA },
    { id: 'weather', label: 'Weather / crop', relationship: 'Supply shock channel', correlationAvailable: NA },
    { id: 'grains', label: 'Corn & soybeans (complex)', relationship: 'Substitution / spread dynamics', correlationAvailable: NA, cohortNote: 'Peer positioning cluster' },
  ],
  Corn: [
    { id: 'dgs10', label: 'US 10Y Treasury yield', relationship: 'Macro liquidity backdrop', correlationAvailable: NA },
    { id: 'dxy', label: 'Broad US dollar index', relationship: 'Export channel — often inverse', correlationAvailable: NA },
    { id: 'weather', label: 'US / South America weather', relationship: 'Supply narrative', correlationAvailable: NA },
    { id: 'grains', label: 'Wheat & soybeans', relationship: 'Spread / substitution', correlationAvailable: NA },
  ],
  Soybeans: [
    { id: 'dgs10', label: 'US 10Y Treasury yield', relationship: 'Macro conditions vs export sector', correlationAvailable: NA },
    { id: 'dxy', label: 'Broad US dollar index', relationship: 'Export competitiveness', correlationAvailable: NA },
    { id: 'china', label: 'China import pace', relationship: 'Demand channel', correlationAvailable: NA },
    { id: 'grains', label: 'Corn & wheat', relationship: 'Spread / substitution', correlationAvailable: NA },
  ],
  Coffee: [
    { id: 'dgs10', label: 'US 10Y Treasury yield', relationship: 'Macro / USD liquidity backdrop', correlationAvailable: NA },
    { id: 'dxy', label: 'Broad US dollar / producer FX', relationship: 'Producer selling & financing', correlationAvailable: NA },
    { id: 'weather', label: 'Origin weather', relationship: 'Crop risk channel', correlationAvailable: NA },
    { id: 'risk', label: 'Logistics / exports', relationship: 'Disruption headlines', correlationAvailable: NA },
  ],
  Cocoa: [
    { id: 'dgs10', label: 'US 10Y Treasury yield', relationship: 'Macro financing conditions', correlationAvailable: NA },
    { id: 'dxy', label: 'USD / origin FX', relationship: 'Pricing and working capital', correlationAvailable: NA },
    { id: 'geo', label: 'Origin politics / ports', relationship: 'Supply disruption', correlationAvailable: NA },
    { id: 'risk', label: 'Risk appetite', relationship: 'Carry and margin tone', correlationAvailable: NA },
  ],
}

const DEFAULT_DRIVERS = [
  { id: 'dgs10', label: '10Y Treasury yield', relationship: 'Macro discount-rate channel', correlationAvailable: NA },
  { id: 'dxy', label: 'DXY', relationship: 'USD liquidity / invoicing', correlationAvailable: NA },
  { id: 'risk', label: 'Global risk regime', relationship: 'Summarized from the week backdrop block when present', correlationAvailable: NA },
]

export function getMacroRelationshipDrivers(market, options = {}) {
  const key = String(market ?? '').trim()
  const list = DRIVERS_BY_MARKET[key] || DEFAULT_DRIVERS
  const drivers = Array.isArray(list) ? list : DEFAULT_DRIVERS
  if (!options?.hidePlaceholderWeather) return drivers
  return drivers.filter((d) => d?.id !== 'weather' || d?.correlationAvailable === true)
}

function intermarketLabel(row) {
  const x = row?.intermarket_impulse_context
  if (!x || typeof x !== 'object') return null
  return String(x.intermarket_confirmation || '').trim() || null
}

/**
 * Text-only divergence lens (no fabricated correlation).
 */
export function getMacroMapDivergenceLens(row, tension, options = {}) {
  const plain = options.relationshipMapLive === true
  const conf = intermarketLabel(row)?.toUpperCase()
  if (conf === 'DIVERGING' || conf === 'WARNING') {
    return {
      status: plain ? 'Cross-markets: split' : 'Diverging (intermarket)',
      detail: plain
        ? 'Related futures are not confirming the same story — treat headlines and yields as noisier this week.'
        : 'Related markets disagree with this week’s positioning read — expect noisier macro drivers.',
      tone: 'amber',
    }
  }
  if (conf === 'MIXED') {
    return {
      status: plain ? 'Cross-markets: mixed' : 'Mixed confirmation',
      detail: plain
        ? 'Some related markets agree, some do not — cleaner trends are harder until the complex picks a leader.'
        : 'Cross-asset sponsorship is split — driver relationships may not line up cleanly this week.',
      tone: 'slate',
    }
  }
  if (conf === 'CONFIRMING') {
    return {
      status: plain ? 'Cross-markets: confirming' : 'Aligned (intermarket)',
      detail: plain
        ? 'Related markets are broadly pointing the same way — fewer conflicting macro messages this week.'
        : 'Related markets broadly confirm the same-week story — cleaner read-through to macro channels.',
      tone: 'emerald',
    }
  }
  if (tension.label.includes('Cautious flow vs friendlier macro')) {
    return {
      status: plain ? 'Flow vs macro: soft' : 'Cautious flow',
      detail: plain ? 'Macro reads a bit friendlier than flow language on this export.' : tension.lines[0] || tension.label,
      tone: 'slate',
    }
  }
  if (tension.label.includes('Constructive flow vs cautious macro')) {
    return {
      status: plain ? 'Flow vs macro: tension' : 'Flow vs macro tension',
      detail: plain ? 'Flow reads stronger than the macro filter on this export.' : tension.lines[0] || tension.label,
      tone: 'amber',
    }
  }
  if (tension.label.includes('both cautious')) {
    return {
      status: plain ? 'Macro + flow: both cautious' : 'Macro + tape cautious',
      detail: plain ? 'Both skew cautious — follow-through may need a clear catalyst.' : tension.lines[0] || tension.label,
      tone: 'amber',
    }
  }
  if (tension.label.includes('aligned (text)')) {
    return {
      status: plain ? 'Macro + flow: aligned' : 'Aligned (text)',
      detail: plain ? 'Macro tone and flow language are not obviously fighting this week.' : 'Flow language and macro labels are not obviously fighting each other in text.',
      tone: 'emerald',
    }
  }
  return {
    status: plain ? 'Cross-check' : 'Unscored',
    detail: plain
      ? 'Intermarket label is thin this row — lean on the chart and your own cross-asset checks.'
      : 'Insufficient intermarket label and flow text for a divergence badge — wire cross-asset series for a quantitative map.',
    tone: 'slate',
  }
}

/* ------------------------------------------------------------------ */
/* Stage B: macro data freshness / health (read-only presentation)     */
/* ------------------------------------------------------------------ */

/** @type {Record<string, { label: string, tone: string }>} */
export const MACRO_STATUS_META = {
  live: { label: 'Live', tone: 'emerald' },
  cached: { label: 'Cached', tone: 'sky' },
  stale: { label: 'Stale', tone: 'amber' },
  warning: { label: 'Warning', tone: 'rose' },
  missing: { label: 'Missing', tone: 'slate' },
  unknown: { label: 'Unknown', tone: 'slate' },
}

export function macroStatusMeta(status) {
  return MACRO_STATUS_META[String(status || '').toLowerCase()] || MACRO_STATUS_META.unknown
}

/**
 * Normalize the freshness/provenance metadata carried on a relationship map.
 * @param {Record<string, unknown>|null|undefined} rm
 */
export function readMacroFreshness(rm) {
  if (!rm || typeof rm !== 'object') return null
  const status = String(rm.data_status || (rm.available ? 'unknown' : 'missing')).toLowerCase()
  const meta = macroStatusMeta(status)
  const latency = Number(rm.latency_days)
  const age = Number(rm.refresh_age_days)
  return {
    status,
    label: meta.label,
    tone: meta.tone,
    sourceSeriesIds: Array.isArray(rm.source_series_ids) ? rm.source_series_ids : [],
    latestObservationDate: rm.latest_observation_date || rm.latest_date || null,
    latencyDays: Number.isFinite(latency) ? latency : null,
    lastSuccessfulRefresh: rm.last_successful_refresh || null,
    refreshAgeDays: Number.isFinite(age) ? age : null,
    carriedOver: rm.carried_over === true,
    lastRefreshError: rm.last_refresh_error || rm.error || null,
  }
}

/**
 * Aggregate a dashboard-wide macro health summary directly from the maps payload.
 * @param {Record<string, unknown>|null|undefined} maps
 */
export function buildMacroHealthSummary(maps) {
  const counts = { live: 0, cached: 0, stale: 0, warning: 0, missing: 0, unknown: 0 }
  let total = 0
  let available = 0
  let lastSuccessfulRefresh = null
  let lastFailedRefresh = null
  if (maps && typeof maps === 'object') {
    for (const p of Object.values(maps)) {
      if (!p || typeof p !== 'object') continue
      total += 1
      const status = String(p.data_status || (p.available ? 'unknown' : 'missing')).toLowerCase()
      counts[status] = (counts[status] || 0) + 1
      if (p.available === true || p.carried_over === true) available += 1
      const ls = p.last_successful_refresh
      if (ls && (!lastSuccessfulRefresh || ls > lastSuccessfulRefresh)) lastSuccessfulRefresh = ls
      const lf = p.last_refresh_error_at
      if (lf && (!lastFailedRefresh || lf > lastFailedRefresh)) lastFailedRefresh = lf
    }
  }
  return { total, available, ...counts, lastSuccessfulRefresh, lastFailedRefresh }
}

export function buildMacroMapRead(market, row, tension, options = {}) {
  const live = options.relationshipMapLive === true
  const rm = options.relationshipRm && typeof options.relationshipRm === 'object' ? options.relationshipRm : null
  const m = market || 'This market'
  const line0 = tension.lines[0] ? String(tension.lines[0]).trim() : ''
  const bits = [`${m} — ${tension.label}.`]
  if (line0) bits.push(line0)
  if (live && rm) {
    const { priceLabel, driverLabel } = getSeriesPair(rm)
    bits.push(`Chart: ${priceLabel} vs ${driverLabel} (rebased window) — context only.`)
  } else if (!live) {
    bits.push('Macro overlay not in this export for this contract — rebuild confluence with macro maps if you need the chart.')
  }
  return bits.join(' ')
}
