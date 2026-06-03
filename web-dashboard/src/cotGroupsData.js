/**
 * COT group integrity layer — institutions / commercials / retail proxy (audited).
 */

const GROUPS_URL = '/data/cot_groups_latest.json'
const AUDIT_URL = '/data/cot_group_audit_latest.json'

let _groupsCache = null
let _auditCache = null
let _groupsPromise = null
let _auditPromise = null

export async function loadCotGroupsStore() {
  if (_groupsCache) return _groupsCache
  if (!_groupsPromise) {
    _groupsPromise = fetch(GROUPS_URL)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((doc) => {
        _groupsCache = doc && typeof doc === 'object' ? doc : { instruments: {} }
        return _groupsCache
      })
      .catch(() => {
        _groupsCache = { instruments: {} }
        return _groupsCache
      })
  }
  return _groupsPromise
}

export async function loadCotGroupAudit() {
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

export function getCotGroupsForInstrument(store, instrumentId) {
  if (!store?.instruments) return null
  return store.instruments[instrumentId] || null
}

export function getCotAuditForInstrument(auditStore, instrumentId) {
  if (!auditStore?.instruments) return null
  return auditStore.instruments[instrumentId] || null
}

export const COT_INTEGRITY_TABS = [
  { id: 'institutions', label: 'Institutions' },
  { id: 'commercials', label: 'Commercials' },
  { id: 'retail_proxy', label: 'Retail Proxy' },
  { id: 'combined', label: 'Combined' },
  { id: 'audit', label: 'Audit Proof' },
]
