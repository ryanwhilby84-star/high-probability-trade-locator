import React from 'react'
import { fetchPublicJson } from '../utils/fetchPublicJson.js'

/**
 * Fundamental valuation export.
 * Merges valuation_latest + metals + Gold market-clearing tip (Gold overrides legacy metals).
 */
export function useValuationLatest() {
  const [doc, setDoc] = React.useState(null)

  React.useEffect(() => {
    let active = true

    Promise.all([
      fetchPublicJson('/data/valuation_latest.json').catch(() => ({ instruments: {} })),
      fetchPublicJson('/data/metals_valuation_latest.json').catch(() => ({ instruments: {} })),
      fetchPublicJson('/data/gold_valuation_latest.json').catch(() => null),
    ])
      .then(([valuationDoc, metalsDoc, goldDoc]) => {
        if (!active) return
        const goldInst = goldDoc?.instrument
        const instruments = {
          ...(valuationDoc.instruments || {}),
          ...(metalsDoc.instruments || {}),
        }
        if (goldInst?.wired) {
          instruments.Gold = {
            ...(instruments.Gold || {}),
            ...goldInst,
            wired: true,
            model_id: goldInst.model_id || goldDoc?.engine,
            valuation_pillar: 'gold_market_clearing',
          }
        }

        setDoc({
          ...valuationDoc,
          instruments,
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