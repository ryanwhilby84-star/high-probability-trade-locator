import React from 'react'

import { LivePriceStore } from '../prices/stores/LivePriceStore.js'
import { useLivePrice } from '../prices/usePriceStores.js'

export { LivePriceStore }
export { triggerLiveQuotesExport } from '../prices/stores/LivePriceStore.js'

/** @deprecated use LivePriceStore.clearCache */
export function clearLiveQuotesCache() {
  LivePriceStore.clearCache()
}

/** @deprecated use LivePriceStore.getSnapshot().fetchUrl */
export function getLiveQuotesLastFetchUrl() {
  return LivePriceStore.getSnapshot().fetchUrl
}

/** @deprecated use LivePriceStore.getSnapshot().fetchedAtMs */
export function getLiveQuotesLastFetchedAtMs() {
  return LivePriceStore.getSnapshot().fetchedAtMs
}

/** @deprecated use LivePriceStore.refresh */
export async function loadLiveQuotesStore(opts) {
  await LivePriceStore.refresh({ runExport: false })
  return LivePriceStore.getSnapshot().doc
}

/**
 * @deprecated prefer useLivePrice from usePriceStores.js
 * Kept for LiveQuotesProvider compatibility.
 */
export function useLiveQuotesInternal(marketId) {
  const live = useLivePrice(marketId)

  const quote = React.useMemo(() => {
    const q = live.quote
    if (!q) return null
    return {
      live_price: q.mid,
      live_bid: q.bid,
      live_ask: q.ask,
      live_price_source: q.source,
      live_price_as_of: q.asOf,
      canonical_symbol: q.symbol,
      live_fetch_ok: q.fetchOk,
    }
  }, [live.quote])

  return React.useMemo(
    () => ({
      quote,
      doc: live.doc,
      loaded: live.loaded,
      fetchUrl: live.fetchUrl,
      fetchedAtMs: live.fetchedAtMs,
      freshness: live.freshness,
      refreshing: live.refreshing,
      refreshError: live.refreshError,
      refresh: live.refresh,
      refetch: () => live.refresh({ runExport: false }),
      liveStatus: live.status,
      store: live.store,
    }),
    [quote, live],
  )
}
