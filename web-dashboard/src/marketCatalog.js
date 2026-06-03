import { TRACKED_MARKET_IDS, canonicalMarketId, isCotRowResolved } from './marketResolution.js'
import { getInstitutionalContext, tacticalActionLabel } from './institutionalContext.js'
import {
  allInstrumentIds,
  assetClassesFromRegistry,
  getInstrumentMeta,
  assetClassLabel as registryAssetClassLabel,
} from './instrumentRegistry.js'

const LEGACY_ASSET_CLASSES = [
  { id: 'indices', label: 'Indices', markets: ['NASDAQ / NQ', 'S&P 500 / ES', 'Dow / YM'] },
  { id: 'fx', label: 'FX', markets: ['Euro FX / 6E', 'British Pound / 6B', 'Japanese Yen / 6J', 'Swiss Franc / 6S', 'Australian Dollar / 6A', 'Canadian Dollar / 6C', 'NZ Dollar / 6N'] },
  { id: 'metals', label: 'Metals', markets: ['Gold', 'Silver', 'Copper / HG'] },
  { id: 'energy', label: 'Energy', markets: ['Crude Oil / CL', 'Natural Gas / NG'] },
  { id: 'agriculture', label: 'Agriculture', markets: ['Corn', 'Wheat', 'Soybeans'] },
  { id: 'softs', label: 'Softs', markets: ['Coffee', 'Cocoa'] },
  { id: 'bonds', label: 'Bonds/Rates', markets: [] },
]

export function getAssetClasses() {
  return assetClassesFromRegistry() || LEGACY_ASSET_CLASSES
}

export const ASSET_CLASSES = LEGACY_ASSET_CLASSES

function buildMarketToClass() {
  const map = new Map()
  for (const ac of getAssetClasses()) {
    for (const m of ac.markets || []) map.set(m, ac.id)
  }
  return map
}

const MARKET_TO_CLASS = buildMarketToClass()

export function assetClassForMarket(market) {
  const id = canonicalMarketId(market)
  const meta = getInstrumentMeta(id)
  if (meta?.asset_class) return meta.asset_class
  return MARKET_TO_CLASS.get(id) || buildMarketToClass().get(id) || 'other'
}

export function assetClassLabel(classId) {
  const hit = getAssetClasses().find((c) => c.id === classId)
  return hit?.label || registryAssetClassLabel(classId) || 'Other'
}

export function marketsInAssetClass(classId) {
  if (!classId || classId === 'all') return allInstrumentIds().length ? allInstrumentIds() : [...TRACKED_MARKET_IDS]
  const hit = getAssetClasses().find((c) => c.id === classId)
  return hit?.markets?.length ? hit.markets : []
}

export function subgroupForMarket(market) {
  const meta = getInstrumentMeta(canonicalMarketId(market))
  return meta?.subgroup || '—'
}

const WATCHLIST_KEY = 'hptl_watchlist_v1'

const _allowedIds = () => {
  const ids = allInstrumentIds()
  return ids.length ? ids : TRACKED_MARKET_IDS
}

export function loadWatchlist() {
  try {
    const raw = localStorage.getItem(WATCHLIST_KEY)
    const arr = raw ? JSON.parse(raw) : []
    const allowed = new Set(_allowedIds())
    return Array.isArray(arr) ? arr.filter((m) => allowed.has(m)) : []
  } catch {
    return []
  }
}

export function saveWatchlist(ids) {
  const allowed = new Set(_allowedIds())
  const clean = [...new Set(ids)].filter((m) => allowed.has(m))
  localStorage.setItem(WATCHLIST_KEY, JSON.stringify(clean))
  return clean
}

export function toggleWatchlist(market) {
  const id = canonicalMarketId(market)
  const cur = loadWatchlist()
  const next = cur.includes(id) ? cur.filter((m) => m !== id) : [...cur, id]
  return saveWatchlist(next)
}

/** Tactical action from L5 when COT present; macro-only label otherwise. */
export function deriveActionLabel(row) {
  if (!row) return '—'
  const inst = getInstitutionalContext(row)
  if (inst?.data_mode === 'macro_only') {
    return inst.tactical_posture_label || 'Macro watch'
  }
  if (!isCotRowResolved(row)) return 'Macro only'

  const label = tacticalActionLabel(row)
  if (label && label !== '—') return label

  if (inst?.tactical_posture) {
    const p = String(inst.tactical_posture)
    if (p.includes('stalk_long')) return 'Stalk Long on Pullback'
    if (p.includes('stalk_short')) return 'Stalk Short on Rally'
    if (p.includes('avoid_chase')) return 'Avoid Chasing'
    if (p.includes('wait_confirmation')) return 'Wait for Confirmation'
    if (p.includes('stand_aside')) return 'Stand Aside'
    if (p.includes('stalk_long_continuation')) return 'Stalk Long'
    if (p.includes('stalk_short_continuation')) return 'Stalk Short'
    if (p === 'watch') return 'Watch'
  }

  const setup = String(row.setup_type || '').toLowerCase()
  if (setup.includes('avoid') || setup.includes('overextended') || setup.includes('squeeze')) return 'Avoid Chasing'
  if (setup.includes('wait') || setup.includes('transition')) return 'Wait for Confirmation'
  if (setup.includes('pullback') || setup.includes('profit-taking')) return 'Stalk Long on Pullback'
  if (setup.includes('covering rally')) return 'Stalk Short on Rally'
  if (setup.includes('long continuation')) return 'Stalk Long'
  if (setup.includes('short continuation')) return 'Stalk Short'
  if (setup.includes('no clean')) return 'Watch'
  return 'Watch'
}

export function cotBiasTone(bias = '') {
  const b = String(bias).toLowerCase()
  if (b.includes('bull')) return 'bull'
  if (b.includes('bear')) return 'bear'
  return 'neutral'
}

export { catalystSummaryFromRow } from './liveFeedStatus.js'

export function positioningShiftMagnitude(row) {
  const n = Math.abs(Number(row?.one_week_net_change))
  return Number.isFinite(n) ? n : 0
}
