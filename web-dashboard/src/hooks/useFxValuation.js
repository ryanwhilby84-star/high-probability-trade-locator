import React from 'react'

let _fxValuationCache = null
let _fxValuationPromise = null

function normalizeFxValuationDoc(raw) {
  if (!raw || typeof raw !== 'object') return null
  const pairs = Array.isArray(raw.pairs) ? raw.pairs.filter(Boolean) : []
  return { ...raw, pairs }
}

/** Shared loader for fx_valuation_latest.json (V2 — secondary / setup ranking only). */
export function useFxValuation() {
  const [doc, setDoc] = React.useState(() => normalizeFxValuationDoc(_fxValuationCache))
  React.useEffect(() => {
    if (_fxValuationCache) {
      setDoc(normalizeFxValuationDoc(_fxValuationCache))
      return
    }
    if (!_fxValuationPromise) {
      _fxValuationPromise = fetch('/data/fx_valuation_latest.json')
        .then((r) => (r.ok ? r.json() : null))
        .then((d) => {
          _fxValuationCache = d
          return normalizeFxValuationDoc(d)
        })
        .catch(() => null)
    }
    let active = true
    _fxValuationPromise.then((d) => {
      if (active) setDoc(d)
    })
    return () => {
      active = false
    }
  }, [])
  return doc
}
