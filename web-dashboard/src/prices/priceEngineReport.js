/**
 * Single grouped [Price Engine] console report.
 */

import { LivePriceStore } from './stores/LivePriceStore.js'
import { WeeklyOHLCStore } from './stores/WeeklyOHLCStore.js'
import { HistoricalCOTStore } from './stores/HistoricalCOTStore.js'
import { validatePriceEngine } from './priceEngineValidation.js'

let _lastSignature = null

export function logPriceEngineReport(marketId, consumers = {}) {
  if (!marketId) return null

  const liveSnap = LivePriceStore.getSnapshot()
  const weeklySnap = WeeklyOHLCStore.getSnapshot()
  const cotSnap = HistoricalCOTStore.getSnapshot()
  const liveQuote = LivePriceStore.getQuote(marketId)
  const weekly = WeeklyOHLCStore.getCompletedWeekly(marketId)
  const validation = validatePriceEngine(marketId, consumers)

  const signature = JSON.stringify({
    marketId,
    liveMid: liveQuote?.mid,
    liveAsOf: liveQuote?.asOf,
    weeklyClose: weekly?.close,
    weeklyDate: weekly?.date,
    validationStatus: validation.status,
    consumers: {
      liveMarker: consumers.liveMarkerPrice,
      valuation: consumers.valuationLivePrice,
      weeklyChart: consumers.weeklyChartClose,
      historical: consumers.historicalHeaderPrice,
    },
  })

  if (signature === _lastSignature) return validation
  _lastSignature = signature

  const report = {
    marketId,
    validation,
    LivePriceStore: {
      loaded: liveSnap.loaded,
      fetchUrl: liveSnap.fetchUrl,
      fetchedAtMs: liveSnap.fetchedAtMs,
      generatedAt: liveSnap.generatedAt,
      refreshing: liveSnap.refreshing,
      subscribers: liveSnap.subscriberCount,
      quote: liveQuote,
      status: LivePriceStore.getStatus(marketId),
      freshness: LivePriceStore.getFreshness(marketId),
    },
    WeeklyOHLCStore: {
      loaded: weeklySnap.loaded,
      fetchUrl: weeklySnap.fetchUrl,
      generatedAt: weeklySnap.generatedAt,
      subscribers: weeklySnap.subscriberCount,
      completedWeekly: weekly,
    },
    HistoricalCOTStore: {
      loaded: cotSnap.loaded,
      loading: cotSnap.loading,
      fetchUrl: cotSnap.fetchUrl,
      generatedAt: cotSnap.generatedAt,
      subscribers: cotSnap.subscriberCount,
      seriesRows: HistoricalCOTStore.getSeries(marketId)?.length ?? 0,
    },
    subscribers: {
      live: liveSnap.subscriberCount,
      weekly: weeklySnap.subscriberCount,
      historical: cotSnap.subscriberCount,
    },
  }

  console.groupCollapsed(`[Price Engine] ${marketId} — ${validation.status}`)
  console.table(report.LivePriceStore)
  console.table(report.WeeklyOHLCStore)
  console.table(report.HistoricalCOTStore)
  if (validation.checks.length) console.table(validation.checks)
  if (validation.failures.length) console.warn('FAILURES', validation.failures)
  console.groupEnd()

  return validation
}
