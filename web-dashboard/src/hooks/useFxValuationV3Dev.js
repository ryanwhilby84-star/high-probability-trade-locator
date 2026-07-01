import React from 'react'
import { fetchPublicJson } from './useCurrencyFuturesIVE.js'

export function useFxValuationV3Latest() {
  const [doc, setDoc] = React.useState(null)
  React.useEffect(() => {
    let active = true
    fetchPublicJson('/data/fx_valuation_v3_latest.json')
      .then((d) => {
        if (active) setDoc(d)
      })
      .catch(() => {
        if (active) setDoc(null)
      })
    return () => {
      active = false
    }
  }, [])
  return doc
}

export function useFxValuationFoundationAudit() {
  const [doc, setDoc] = React.useState(null)
  React.useEffect(() => {
    let active = true
    fetchPublicJson('/data/fx_valuation_data_foundation_audit.json')
      .then((d) => {
        if (active) setDoc(d)
      })
      .catch(() => {
        if (active) setDoc(null)
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
