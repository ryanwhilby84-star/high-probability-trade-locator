import React from 'react'

let _cache = null
let _promise = null

/** Weekly location pillar export (`location_latest.json`). */
export function useLocationLatest() {
  const [doc, setDoc] = React.useState(_cache)
  React.useEffect(() => {
    if (_cache) {
      setDoc(_cache)
      return
    }
    if (!_promise) {
      _promise = fetch('/data/location_latest.json')
        .then((r) => (r.ok ? r.json() : null))
        .then((d) => {
          _cache = d
          return d
        })
        .catch(() => null)
    }
    let active = true
    _promise.then((d) => {
      if (active) setDoc(d)
    })
    return () => {
      active = false
    }
  }, [])
  return doc
}

export function locationBlockForMarket(doc, marketId) {
  if (!doc || !marketId) return null
  const instruments = doc.instruments || doc.markets || {}
  return instruments[marketId] || null
}
