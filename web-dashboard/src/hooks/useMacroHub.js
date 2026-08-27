import React from 'react'

let _macroHubCache = null
let _macroHubPromise = null

/** Load data/exports/macro_hub_latest.json (mirrored to public/data). */
export function useMacroHub() {
  const [doc, setDoc] = React.useState(_macroHubCache)
  const [error, setError] = React.useState(null)
  const [loading, setLoading] = React.useState(!_macroHubCache)

  React.useEffect(() => {
    if (_macroHubCache) {
      setDoc(_macroHubCache)
      setLoading(false)
      return
    }
    if (!_macroHubPromise) {
      _macroHubPromise = fetch('/data/macro_hub_latest.json')
        .then((r) => {
          if (!r.ok) throw new Error(`HTTP ${r.status}`)
          return r.json()
        })
        .then((d) => {
          _macroHubCache = d
          return d
        })
        .catch((e) => {
          _macroHubPromise = null
          throw e
        })
    }
    let active = true
    _macroHubPromise
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
