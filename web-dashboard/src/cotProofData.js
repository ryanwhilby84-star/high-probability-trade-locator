const PROOF_URL = '/data/cot_proof_latest.json'

let _cache = null
let _promise = null

export async function loadCotProofLatest() {
  if (_cache) return _cache
  if (!_promise) {
    _promise = fetch(PROOF_URL)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status} loading cot_proof_latest.json`)
        return r.json()
      })
      .then((doc) => {
        _cache = doc && typeof doc === 'object' ? doc : { instruments: {}, summary: {} }
        return _cache
      })
  }
  return _promise
}

export function proofInstrumentsList(doc) {
  const rows = Object.values(doc?.instruments || {})
  return rows.sort((a, b) => String(a.instrument_id).localeCompare(String(b.instrument_id)))
}

export function statusTone(status) {
  if (status === 'PASS') return 'pass'
  if (status === 'NEEDS_REVIEW') return 'review'
  return 'fail'
}
