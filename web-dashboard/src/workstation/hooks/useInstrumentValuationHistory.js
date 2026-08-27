import React from 'react'

const VAL_URL = '/data/instrument_valuation_history_latest.json'

let _cache = null
let _promise = null

export function useInstrumentValuationHistory(marketId) {
  const [doc, setDoc] = React.useState(_cache)
  const [error, setError] = React.useState(null)
  const [loading, setLoading] = React.useState(!_cache)

  React.useEffect(() => {
    if (_cache) {
      setDoc(_cache)
      setLoading(false)
      return
    }
    if (!_promise) {
      _promise = fetch(VAL_URL)
        .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
        .then((d) => {
          _cache = d
          return d
        })
        .catch((err) => {
          _promise = null
          throw err
        })
    }
    let cancelled = false
    _promise
      .then((d) => {
        if (!cancelled) {
          setDoc(d)
          setLoading(false)
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(String(err?.message || err))
          setLoading(false)
        }
      })
    return () => {
      cancelled = true
    }
  }, [])

  const block = React.useMemo(() => {
    const instruments = doc?.instruments || {}
    return instruments[marketId] || instruments[marketId?.trim?.()] || null
  }, [doc, marketId])

  const series = React.useMemo(() => {
    const raw = block?.series
    return Array.isArray(raw) ? raw : []
  }, [block])

  return {
    loading,
    error,
    doc,
    block,
    series,
    generatedAt: doc?.generated_at ?? null,
    exportNote: block?.note ?? null,
  }
}
