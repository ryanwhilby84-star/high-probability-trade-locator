const TRUTH_URL = '/data/cot_source_truth_audit_latest.json'

let _cache = null
let _promise = null

export async function loadCotSourceTruthLatest() {
  if (_cache) return _cache
  if (!_promise) {
    _promise = fetch(TRUTH_URL)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status} loading cot_source_truth_audit_latest.json`)
        return r.json()
      })
      .then((doc) => {
        _cache = doc && typeof doc === 'object' ? doc : { instruments: {}, summary: {} }
        return _cache
      })
  }
  return _promise
}

export function sourceTruthInstruments(doc) {
  return Object.values(doc?.instruments || {}).sort((a, b) =>
    String(a.instrument).localeCompare(String(b.instrument)),
  )
}

export function statusTone(status) {
  if (status === 'PASS') return 'pass'
  if (status === 'NEEDS_MANUAL_REVIEW') return 'review'
  return 'fail'
}
