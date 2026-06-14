import React from 'react'

let _v3Cache = null
let _v3Promise = null
let _foundCache = null
let _foundPromise = null

export function useFxValuationV3Latest() {
  const [doc, setDoc] = React.useState(_v3Cache)
  React.useEffect(() => {
    if (_v3Cache) {
      setDoc(_v3Cache)
      return
    }
    if (!_v3Promise) {
      _v3Promise = fetch('/data/fx_valuation_v3_latest.json')
        .then((r) => (r.ok ? r.json() : null))
        .then((d) => {
          _v3Cache = d
          return d
        })
        .catch(() => null)
    }
    let active = true
    _v3Promise.then((d) => {
      if (active) setDoc(d)
    })
    return () => {
      active = false
    }
  }, [])
  return doc
}

export function useFxValuationFoundationAudit() {
  const [doc, setDoc] = React.useState(_foundCache)
  React.useEffect(() => {
    if (_foundCache) {
      setDoc(_foundCache)
      return
    }
    if (!_foundPromise) {
      _foundPromise = fetch('/data/fx_valuation_data_foundation_audit.json')
        .then((r) => (r.ok ? r.json() : null))
        .then((d) => {
          _foundCache = d
          return d
        })
        .catch(() => null)
    }
    let active = true
    _foundPromise.then((d) => {
      if (active) setDoc(d)
    })
    return () => {
      active = false
    }
  }, [])
  return doc
}

export function foundationReadinessStatus(foundationPair, v3Pair) {
  const fPass = foundationPair?.overall_status === 'PASS'
  const v3Pass = v3Pair?.audit_status === 'PASS'
  if (fPass && v3Pass) return 'PASS'
  if (v3Pass && !fPass) return 'NEAR PASS'
  return 'FAIL'
}

export function pairFromFoundationAudit(auditDoc, pairId) {
  if (!auditDoc?.pairs || !pairId) return null
  return auditDoc.pairs[pairId] || null
}

export function pairFromV3Latest(v3Doc, pairId) {
  if (!v3Doc?.pairs || !pairId) return null
  return v3Doc.pairs[pairId] || null
}
