import React from 'react'

/** Fetch a public JSON artifact without browser or module-level caching. */
export function fetchPublicJson(path) {
  const url = `${path}${path.includes('?') ? '&' : '?'}v=${Date.now()}`
  return fetch(url, { cache: 'no-store' }).then((r) => (r.ok ? r.json() : null))
}

/** Currency futures IVE export — primary valuation for CME FX futures. */
export function useCurrencyFuturesIVE() {
  const [doc, setDoc] = React.useState(null)
  React.useEffect(() => {
    let active = true
    fetchPublicJson('/data/currency_futures_ive_latest.json')
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

export function futuresIveBlockForMarket(doc, marketId) {
  if (!doc || !marketId) return null
  return doc.instruments?.[marketId] || null
}
