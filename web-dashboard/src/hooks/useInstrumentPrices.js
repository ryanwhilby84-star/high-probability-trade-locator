import React from 'react'
import { getInstrumentPrices, loadPriceStore } from '../priceData.js'

export function useInstrumentPrices(instrumentId) {
  const [store, setStore] = React.useState(null)
  const [loading, setLoading] = React.useState(true)
  const [error, setError] = React.useState(null)

  React.useEffect(() => {
    let cancelled = false
    setLoading(true)
    loadPriceStore()
      .then((doc) => {
        if (!cancelled) {
          setStore(doc)
          setError(null)
        }
      })
      .catch((e) => {
        if (!cancelled) {
          setStore({ instruments: {} })
          setError(e?.message || 'Failed to load prices')
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [])

  const data = React.useMemo(
    () => (store && instrumentId ? getInstrumentPrices(store, instrumentId) : null),
    [store, instrumentId],
  )

  return { data, loading, error, storeLoaded: Boolean(store) }
}
