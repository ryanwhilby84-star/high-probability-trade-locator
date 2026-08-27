import React from 'react'
import { loadLegacyCotLatest } from '../legacyCotData.js'
import { attentionSortedStates, buildMarketStatesIndex } from '../cot/marketStateEngine.js'

export function useMarketStates(marketIds, asOfDate) {
  const [legacyStore, setLegacyStore] = React.useState(null)
  const [loading, setLoading] = React.useState(true)
  const [error, setError] = React.useState(null)

  React.useEffect(() => {
    let cancelled = false
    setLoading(true)
    loadLegacyCotLatest()
      .then((doc) => {
        if (!cancelled) {
          setLegacyStore(doc)
          setError(null)
        }
      })
      .catch((e) => {
        if (!cancelled) {
          setLegacyStore({ instruments: {} })
          setError(e?.message || 'Failed to load legacy COT')
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [])

  const statesByMarket = React.useMemo(
    () => buildMarketStatesIndex(legacyStore, marketIds, asOfDate),
    [legacyStore, marketIds, asOfDate],
  )

  const attentionList = React.useMemo(() => attentionSortedStates(statesByMarket), [statesByMarket])

  return { statesByMarket, attentionList, loading, error }
}
