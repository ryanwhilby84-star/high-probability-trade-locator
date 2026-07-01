import React from 'react'

import { WeeklyOHLCStore } from '../../prices/stores/WeeklyOHLCStore.js'
import { useWeeklyOHLC } from '../../prices/usePriceStores.js'

/**
 * Price OHLC for workstation candles — WeeklyOHLCStore only.
 */
export function useWorkstationOhlc(marketId) {
  const weekly = useWeeklyOHLC(marketId)

  return {
    exportBlock: weekly.exportBlock,
    exportDoc: weekly.exportDoc,
    exportLoaded: weekly.exportLoaded,
    exportGeneratedAt: weekly.exportGeneratedAt,
    fetchUrl: weekly.fetchUrl,
    weeklyBars: weekly.weeklyBars,
    completedWeekly: weekly.completedWeekly,
    priceSource: weekly.priceSource,
    store: weekly.store,
    /** @deprecated weekly OHLC no longer falls back to prices_latest */
    prices: null,
  }
}

export { WeeklyOHLCStore }
