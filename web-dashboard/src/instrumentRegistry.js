/**
 * Instrument universe — loaded from confluence export or instrument_registry.json.
 */
import { TRACKED_MARKET_IDS as LEGACY_TRACKED, isCotRowResolved } from './marketResolution.js'

/** @typedef {import('./types').InstrumentMeta} InstrumentMeta */

let _registry = null

export function setInstrumentRegistry(doc) {
  if (!doc || typeof doc !== 'object') {
    _registry = null
    return
  }
  const markets = Array.isArray(doc.markets) ? doc.markets : []
  _registry = {
    version: doc.version || 1,
    legacy_cot_markets: doc.legacy_cot_markets || [],
    markets,
    byId: new Map(markets.map((m) => [m.id, m])),
  }
}

export function getInstrumentRegistry() {
  return _registry
}

export function allInstrumentIds() {
  if (_registry?.markets?.length) return _registry.markets.map((m) => m.id)
  return [...LEGACY_TRACKED]
}

export function getInstrumentMeta(marketId) {
  return _registry?.byId?.get(marketId) || null
}

export function assetClassesFromRegistry() {
  if (!_registry?.markets?.length) return null
  const groups = new Map()
  for (const m of _registry.markets) {
    const ac = m.asset_class || 'other'
    if (!groups.has(ac)) groups.set(ac, { id: ac, label: assetClassLabel(ac), markets: [] })
    groups.get(ac).markets.push(m.id)
  }
  const order = ['indices', 'fx', 'metals', 'commodities', 'bonds', 'crypto', 'other']
  return order.filter((id) => groups.has(id)).map((id) => groups.get(id))
}

export function assetClassLabel(classId) {
  const labels = {
    indices: 'Indices',
    fx: 'FX',
    metals: 'Metals',
    commodities: 'Commodities',
    bonds: 'Bonds/Rates',
    crypto: 'Crypto',
    other: 'Other',
  }
  return labels[classId] || classId
}

export function isCotMappedInstrument(marketId) {
  const m = getInstrumentMeta(marketId)
  if (m) return m.has_cot_mapping === true
  return LEGACY_TRACKED.includes(marketId)
}

export function positioningStatus(row) {
  return row?.positioning_status || row?.instrument_meta?.positioning_status || 'unknown'
}

export function cotStatusLabel(row) {
  return row?.cot_status_label || (isCotRowResolved(row) ? 'COT mapped' : 'COT unavailable')
}

export function macroAlignmentFromRow(row) {
  const inst = row?.institutional_context
  return inst?.macro_alignment || row?.macro_transmission?.asset_alignment || '—'
}

export function tacticalPostureFromRow(row) {
  const inst = row?.institutional_context
  if (inst?.tactical_posture_label) return inst.tactical_posture_label
  if (inst?.data_mode === 'macro_only') return 'Macro watch'
  return row?.setup_type || '—'
}
