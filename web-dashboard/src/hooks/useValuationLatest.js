import React from 'react'
import { fetchPublicJson } from '../utils/fetchPublicJson.js'

/** Fundamental valuation export (`valuation_latest.json`) — FX V3 + agri pillar. */
export function useValuationLatest() {
  const [doc, setDoc] = React.useState(null)
  React.useEffect(() => {
    let active = true
    fetchPublicJson('/data/valuation_latest.json')
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
