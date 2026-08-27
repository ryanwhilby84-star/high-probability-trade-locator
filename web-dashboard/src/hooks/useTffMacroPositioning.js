import React from 'react'

let _tffCache = null
let _tffPromise = null

/** Load TFF macro positioning export (DXY + Treasury futures). */
export function useTffMacroPositioning() {
  const [doc, setDoc] = React.useState(_tffCache)
  const [error, setError] = React.useState(null)
  const [loading, setLoading] = React.useState(!_tffCache)

  React.useEffect(() => {
    if (_tffCache) {
      setDoc(_tffCache)
      setLoading(false)
      return
    }
    if (!_tffPromise) {
      _tffPromise = fetch('/data/tff_macro_positioning_latest.json')
        .then((r) => {
          if (!r.ok) throw new Error(`HTTP ${r.status}`)
          return r.json()
        })
        .then((d) => {
          _tffCache = d
          return d
        })
        .catch((e) => {
          _tffPromise = null
          throw e
        })
    }
    let active = true
    _tffPromise
      .then((d) => {
        if (active) {
          setDoc(d)
          setError(null)
          setLoading(false)
        }
      })
      .catch((e) => {
        if (active) {
          setDoc(null)
          setError(String(e?.message || e))
          setLoading(false)
        }
      })
    return () => {
      active = false
    }
  }, [])

  return { data: doc, loading, error }
}

/** Prefetch TFF export (e.g. from useConfluenceData). */
export function prefetchTffMacroPositioning() {
  if (_tffCache) return Promise.resolve(_tffCache)
  if (!_tffPromise) {
    _tffPromise = fetch('/data/tff_macro_positioning_latest.json')
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((d) => {
        _tffCache = d
        return d
      })
      .catch((e) => {
        _tffPromise = null
        throw e
      })
  }
  return _tffPromise
}

export function getTffMacroPositioningCache() {
  return _tffCache
}
