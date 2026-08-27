import React from 'react'
import { useInstrumentPrices } from '../../hooks/useInstrumentPrices.js'
import { useInstrumentValuationHistory } from './useInstrumentValuationHistory.js'
import {
  alignValuationToWeekly,
  buildWeeklyTimelineRows,
  normalizeWeeklyOhlc,
  resolveWeeklyOhlc,
} from '../data/normalizeWeeklyTimeline.js'

/**
 * Aggregates price OHLC + point-in-time valuation history for the workstation.
 */
export function useWorkstationData(marketId) {
  const prices = useInstrumentPrices(marketId)
  const valuation = useInstrumentValuationHistory(marketId)

  const { weeklyBars, weeklySource } = React.useMemo(() => {
    try {
      const { weekly, source } = resolveWeeklyOhlc(prices.data)
      return { weeklyBars: normalizeWeeklyOhlc(weekly), weeklySource: source }
    } catch (err) {
      console.error('[workstation] weekly OHLC normalization failed', marketId, err)
      return { weeklyBars: [], weeklySource: 'error' }
    }
  }, [prices.data, marketId])

  const valuationAligned = React.useMemo(
    () => alignValuationToWeekly(valuation.series, weeklyBars),
    [valuation.series, weeklyBars],
  )

  const timelineRows = React.useMemo(
    () => buildWeeklyTimelineRows(weeklyBars, valuationAligned),
    [weeklyBars, valuationAligned],
  )

  const fairValuePoints = React.useMemo(
    () =>
      valuationAligned
        .filter((r) => r.fair_value != null)
        .map((r) => ({ time: r.time, value: r.fair_value })),
    [valuationAligned],
  )

  const hasValuationOverlay = fairValuePoints.length > 0

  return {
    marketId,
    loading: prices.loading,
    pricesLoading: prices.loading,
    valuationLoading: valuation.loading,
    error: prices.error || valuation.error,
    weeklyBars,
    timelineRows,
    fairValuePoints,
    valuationAligned,
    hasValuationOverlay,
    valuationMeta: {
      generatedAt: valuation.generatedAt,
      nWeeks: valuation.block?.n_weeks,
      nWithFairValue: valuation.block?.n_with_fair_value,
      exportNote: valuation.exportNote,
    },
    priceMeta: {
      asOf: prices.data?.price?.as_of,
      barCount: weeklyBars.length,
      weeklySource,
    },
  }
}
