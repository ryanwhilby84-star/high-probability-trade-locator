const LINEAGE_URL = '/data/cot_data_lineage_latest.json'

let _cache = null
let _promise = null

export async function loadDataLineageLatest() {
  if (_cache) return _cache
  if (!_promise) {
    _promise = fetch(LINEAGE_URL)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((doc) => {
        _cache = doc && typeof doc === 'object' ? doc : { instruments: {}, summary: {} }
        return _cache
      })
  }
  return _promise
}

export function lineageInstruments(doc) {
  return Object.values(doc?.instruments || {}).sort((a, b) =>
    String(a.instrument).localeCompare(String(b.instrument)),
  )
}

export function statusTone(s) {
  if (s === 'PASS') return 'pass'
  return 'fail'
}

export const LAYER_ORDER = ['source_truth', 'dashboard', 'scanner', 'thesis', 'scoring']
