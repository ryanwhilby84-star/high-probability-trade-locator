/**
 * React hooks — thin adapters over authoritative price stores.
 * No component should fetch price JSON directly.
 */

import React from 'react'

import { LivePriceStore } from './stores/LivePriceStore.js'
import { WeeklyOHLCStore } from './stores/WeeklyOHLCStore.js'
import { HistoricalCOTStore } from './stores/HistoricalCOTStore.js'
import { CurrentPriceStreamStore } from './stores/CurrentPriceStreamStore.js'

function useStoreSnapshot(store) {
  const subscribe = React.useCallback((onStoreChange) => store.subscribe(onStoreChange), [store])
  const getSnapshot = React.useCallback(() => store.getSnapshot(), [store])
  return React.useSyncExternalStore(subscribe, getSnapshot, getSnapshot)
}

export function useLivePriceStore() {
  const snap = useStoreSnapshot(LivePriceStore)
  return snap
}

export function useCurrentPriceStream() {
  return useStoreSnapshot(CurrentPriceStreamStore)
}

/**
 * Live price for one instrument from the Phase 2 Current Price Service.
 * connectionState drives LIVE / RECONNECTING / BACKEND OFFLINE display.
 */
export function useLivePrice(marketId) {
  const snap = useLivePriceStore()
  const streamSnap = useCurrentPriceStream()

  const quote = React.useMemo(() => LivePriceStore.getQuote(marketId), [snap, marketId])
  const freshness = React.useMemo(() => LivePriceStore.getFreshness(marketId), [snap, marketId])
  const status = React.useMemo(() => LivePriceStore.getStatus(marketId), [snap, marketId])
  const activeWeeklyCandle = React.useMemo(
    () => LivePriceStore.getActiveWeeklyCandle(marketId),
    [streamSnap, marketId],
  )
  const streamPrice = React.useMemo(
    () => CurrentPriceStreamStore.getPrice(marketId),
    [streamSnap, marketId],
  )

  const refresh = React.useCallback((opts) => LivePriceStore.refresh(opts), [])

  return React.useMemo(
    () => ({
      quote,
      freshness,
      status,
      activeWeeklyCandle,
      streamPrice,
      connectionState: streamSnap.connectionState,
      connected: streamSnap.connected,
      reconnecting: streamSnap.reconnecting,
      disconnected: streamSnap.disconnected,
      streamMeta: streamSnap.streamMeta,
      doc: snap.doc,
      loaded: snap.loaded,
      fetchUrl: snap.fetchUrl,
      fetchedAtMs: snap.fetchedAtMs,
      generatedAt: snap.generatedAt,
      refreshing: snap.refreshing,
      refreshError: snap.refreshError,
      refresh,
      store: LivePriceStore.STORE_NAME,
    }),
    [
      quote,
      freshness,
      status,
      activeWeeklyCandle,
      streamPrice,
      streamSnap,
      snap,
      refresh,
    ],
  )
}

export function useWeeklyOHLCStore() {
  return useStoreSnapshot(WeeklyOHLCStore)
}

export function useWeeklyOHLC(marketId) {
  const snap = useWeeklyOHLCStore()
  const exportBlock = React.useMemo(() => WeeklyOHLCStore.getExportBlock(marketId), [snap, marketId])
  const weeklyBars = React.useMemo(() => WeeklyOHLCStore.getWeeklyBars(marketId), [snap, marketId])
  const completedWeekly = React.useMemo(() => WeeklyOHLCStore.getCompletedWeekly(marketId), [snap, marketId])
  const priceSource = React.useMemo(() => WeeklyOHLCStore.getPriceSource(marketId), [snap, marketId])

  return React.useMemo(
    () => ({
      exportBlock,
      weeklyBars,
      completedWeekly,
      priceSource,
      exportDoc: snap.doc,
      exportLoaded: snap.loaded,
      exportGeneratedAt: snap.generatedAt,
      fetchUrl: snap.fetchUrl,
      store: WeeklyOHLCStore.STORE_NAME,
    }),
    [exportBlock, weeklyBars, completedWeekly, priceSource, snap],
  )
}

export function useHistoricalCOTStore() {
  return useStoreSnapshot(HistoricalCOTStore)
}

export function useHistoricalCOT(marketId) {
  const snap = useHistoricalCOTStore()
  const block = React.useMemo(() => HistoricalCOTStore.getBlock(marketId), [snap, marketId])
  const series = React.useMemo(() => HistoricalCOTStore.getSeries(marketId), [snap, marketId])

  return React.useMemo(
    () => ({
      block,
      series,
      doc: snap.doc,
      loading: snap.loading,
      loaded: snap.loaded,
      errored: snap.errored,
      error: snap.error,
      fetchUrl: snap.fetchUrl,
      generatedAt: snap.generatedAt,
      getHistoricalCloseAtDate: (date) => HistoricalCOTStore.getHistoricalCloseAtDate(marketId, date),
      store: HistoricalCOTStore.STORE_NAME,
    }),
    [block, series, snap, marketId],
  )
}

export { LivePriceStore, WeeklyOHLCStore, HistoricalCOTStore, CurrentPriceStreamStore }
