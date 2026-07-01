/**
 * Enrich COT workstation series with location + valuation overlays (same date axis).
 */

import { resolveMarketBlock } from './marketBlockResolve.js'
import {
  computeLocationSeriesFromPrices,
  locationBiasFromPercentile,
  locationScoreFromPercentile,
} from '../location/computeLocationSeries.js'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

const FX_PAIR_BY_MARKET = {
  'Euro FX / 6E': 'EUR/USD',
  'British Pound / 6B': 'GBP/USD',
  'Japanese Yen / 6J': 'USD/JPY',
  'Swiss Franc / 6S': 'USD/CHF',
  'Australian Dollar / 6A': 'AUD/USD',
  'Canadian Dollar / 6C': 'USD/CAD',
  'NZ Dollar / 6N': 'NZD/USD',
}

/** Match Python pandas Timestamp.isocalendar().week (capped at 52). */
export function isoWeekNumber(isoDate) {
  if (!isoDate) return null
  const d = new Date(`${String(isoDate).slice(0, 10)}T12:00:00Z`)
  if (Number.isNaN(d.getTime())) return null
  const utc = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()))
  const isoDay = utc.getUTCDay() === 0 ? 7 : utc.getUTCDay()
  utc.setUTCDate(utc.getUTCDate() + 4 - isoDay)
  const yearStart = new Date(Date.UTC(utc.getUTCFullYear(), 0, 1))
  let week = Math.ceil(((utc - yearStart) / 86400000 + 1) / 7)
  if (week > 52) week = 52
  return week
}

function resolveBlock(doc, marketId) {
  if (!doc || !marketId) return null
  const instruments = doc.instruments || doc.markets || {}
  if (instruments[marketId]) return instruments[marketId]
  return resolveMarketBlock({ markets: instruments }, marketId).block
}

function resolveV3Pair(v3Doc, marketId) {
  const pairId = FX_PAIR_BY_MARKET[marketId]
  if (!pairId || !v3Doc?.pairs) return null
  return v3Doc.pairs[pairId] || null
}

function resolveSeasonBlock(seasonalityDoc, marketId) {
  return resolveMarketBlock(seasonalityDoc, marketId).block
}

function locationMetaForMarket(marketId, { locationBlock, locationStats }) {
  const wiredExport = locationBlock?.wired === true
  const pct =
    locationStats?.lastPercentile ??
    locationBlock?.price_percentile_52w ??
    null
  const score =
    locationStats?.lastScore ??
    locationBlock?.location_score ??
    locationScoreFromPercentile(pct)
  const bias =
    locationStats?.lastBias ??
    locationBlock?.location_bias ??
    locationBiasFromPercentile(pct)
  const reason =
    locationBlock?.location_reason ||
    (locationStats?.wired
      ? `Rolling 52-week percentile from workstation price (${locationStats.valuesAvailable} weeks).`
      : 'Need at least 12 COT weeks with matched canonical price.')

  if (locationStats?.wired || wiredExport) {
    const biasText = bias && String(bias).toUpperCase() !== 'UNAVAILABLE' ? bias : '—'
    const scoreText = isNum(score) ? `${score.toFixed(1)}/10` : '—'
    const pctText = isNum(pct) ? `${pct.toFixed(0)}th pct of 52w range` : '52-week range position'
    const rangeNote =
      locationStats?.firstDate && locationStats?.lastDate
        ? ` · ${locationStats.firstDate.slice(0, 7)}→${locationStats.lastDate.slice(0, 7)}`
        : ''
    return {
      locationMode: 'wired',
      hasLocation: true,
      locationSubtitle: `${pctText} · ${biasText} · ${scoreText}${rangeNote}`,
      locationNote: reason,
      locationLineKey: 'location_percentile_52w',
      locationStats,
    }
  }

  return {
    locationMode: 'unavailable',
    hasLocation: false,
    locationSubtitle: 'Location data pending',
    locationNote: reason,
    locationLineKey: 'location_percentile_52w',
    locationStats,
  }
}

function valuationMetaForMarket(marketId, { confluenceRow, valuationBlock, v3Doc }) {
  const fxPair = FX_PAIR_BY_MARKET[marketId]
  const v3Pair = resolveV3Pair(v3Doc, marketId)

  if (v3Pair?.audit_status === 'PASS' && v3Pair.fair_value != null && isNum(v3Pair.deviation_pct)) {
    const devText = `${v3Pair.deviation_pct >= 0 ? '+' : ''}${Number(v3Pair.deviation_pct).toFixed(1)}% vs V3 fair value`
    return {
      valuationMode: 'v3_dev',
      hasValuation: true,
      valuationPair: fxPair || v3Pair.pair,
      valuationSubtitle: `${v3Pair.valuation_state || '—'} · ${devText}`,
      valuationNote: v3Pair.driver_summary || v3Pair.valuation_reason || 'fx_carry_real_yield_v3 (audit pass)',
      valuationLineKey: 'valuation_fair',
    }
  }

  const reason =
    valuationBlock?.valuation_reason ||
    v3Pair?.valuation_reason ||
    v3Pair?.driver_summary ||
    'Valuation V3 in development — see Valuation V3 panel below. Location chart shows where price sits.'

  return {
    valuationMode: 'unavailable',
    hasValuation: false,
    valuationPair: fxPair || null,
    valuationSubtitle: 'In development',
    valuationNote: reason,
    valuationLineKey: 'valuation_fair',
  }
}

export function enrichChartWorkstationSeries(
  series,
  { marketId, seasonalityDoc, valHistDoc, confluenceHistory, confluenceRow, valuationDoc, locationDoc, v3Doc },
) {
  if (!Array.isArray(series) || !series.length) return series

  const { series: withLocation } = computeLocationSeriesFromPrices(series)
  const v3Pair = resolveV3Pair(v3Doc, marketId)
  const v3Dev = v3Pair?.deviation_pct

  return withLocation.map((row) => {
    let valuationFair = null
    if (v3Pair?.audit_status === 'PASS' && isNum(v3Dev)) {
      valuationFair = v3Dev
    }

    return {
      ...row,
      valuation_fair: isNum(valuationFair) ? valuationFair : null,
    }
  })
}

export function chartSupplementMeta(
  marketId,
  { seasonalityDoc, valHistDoc, confluenceRow, valuationDoc, locationDoc, confluenceHistory, v3Doc, priceSeries },
) {
  const seasonBlock = resolveSeasonBlock(seasonalityDoc, marketId)
  const locationBlock = resolveBlock(locationDoc, marketId)
  const { stats: locationStats } = computeLocationSeriesFromPrices(priceSeries || [])
  const locMeta = locationMetaForMarket(marketId, { locationBlock, locationStats })
  const valMeta = valuationMetaForMarket(marketId, {
    confluenceRow,
    valuationBlock: resolveBlock(valuationDoc, marketId),
    v3Doc,
  })

  const hasSeasonality = Boolean(seasonBlock?.available && seasonBlock?.trust_grade !== 'C')

  return {
    hasSeasonality,
    seasonalityNote: seasonBlock?.trust_notes || seasonBlock?.availability_note || null,
    seasonalityGrade: seasonBlock?.trust_grade || 'C',
    hasLocation: locMeta.hasLocation,
    locationMode: locMeta.locationMode,
    locationSubtitle: locMeta.locationSubtitle,
    locationNote: locMeta.locationNote,
    locationLineKey: locMeta.locationLineKey,
    locationStats: locMeta.locationStats,
    hasValuation: valMeta.hasValuation,
    valuationMode: valMeta.valuationMode,
    valuationPair: valMeta.valuationPair,
    valuationSubtitle: valMeta.valuationSubtitle,
    valuationNote: valMeta.valuationNote,
    valuationLineKey: valMeta.valuationLineKey,
  }
}
