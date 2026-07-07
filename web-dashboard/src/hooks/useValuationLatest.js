import React from 'react'
import { fetchPublicJson } from '../utils/fetchPublicJson.js'

/**
 * Fundamental valuation export.
 * Merges valuation_latest.json with metals_valuation_latest.json.
 */
export function useValuationLatest() {
  const [doc, setDoc] = React.useState(null)

  React.useEffect(() => {
    let active = true

    Promise.all([
      fetchPublicJson('/data/valuation_latest.json').catch(() => ({ instruments: {} })),
      fetchPublicJson('/data/metals_valuation_latest.json').catch(() => ({ instruments: {} })),
    ])
      .then(([valuationDoc, metalsDoc]) => {
        if (!active) return

        setDoc({
          ...valuationDoc,
          instruments: {
            ...(valuationDoc.instruments || {}),
            ...(metalsDoc.instruments || {}),
          },
        })
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