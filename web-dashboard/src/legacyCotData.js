/**
 * Legacy COT layer — Non-Commercial / Commercial / Non-Reportable (futures only).
 */

const LATEST_URL = '/data/legacy_cot_latest.json'
const AUDIT_URL = '/data/legacy_cot_audit.json'
const RECON_URL = '/data/legacy_cot_reconciliation.json'

let _latestCache = null
let _latestLoadedAt = 0
let _auditCache = null
let _latestPromise = null
let _auditPromise = null

export const LEGACY_COT_TABS = [
  { id: 'noncommercials', label: 'Non-Commercials' },
  { id: 'commercials', label: 'Commercials' },
  { id: 'nonreportables', label: 'Non-Reportables' },
  { id: 'combined', label: 'Combined' },
  { id: 'audit', label: 'Audit' },
]

export async function loadLegacyCotLatest(force = false) {
  if (!force && _latestCache && Date.now() - _latestLoadedAt < 60_000) return _latestCache
  if (!_latestPromise) {
    _latestPromise = fetch(`${LATEST_URL}?v=${Date.now()}`, { cache: 'no-store' })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((doc) => {
        if (!doc?.instruments || typeof doc.instruments !== 'object') throw new Error('Invalid legacy COT archive')
        _latestCache = doc
        _latestLoadedAt = Date.now()
        return _latestCache
      })
      .finally(() => { _latestPromise = null })
  }
  return _latestPromise
}

export async function loadLegacyCotAudit() {
  if (_auditCache) return _auditCache
  if (!_auditPromise) {
    _auditPromise = fetch(AUDIT_URL)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((doc) => {
        _auditCache = doc && typeof doc === 'object' ? doc : { instruments: {} }
        return _auditCache
      })
      .catch(() => {
        _auditCache = { instruments: {} }
        return _auditCache
      })
  }
  return _auditPromise
}

export function getLegacyCotForInstrument(store, instrumentId) {
  if (!store?.instruments) return null
  return store.instruments[instrumentId] || null
}

export function getLegacyAuditForInstrument(auditStore, instrumentId) {
  if (!auditStore?.instruments) return null
  return auditStore.instruments[instrumentId] || null
}

export function isLegacyScoringEligible(store, instrumentId) {
  return (store?.scoring_eligible_instruments || []).includes(instrumentId)
}
