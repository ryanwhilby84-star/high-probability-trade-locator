import { canonicalMarketId, isCotRowResolved, recordCotReportDate, normalizeReportDate } from './marketResolution.js'
import {
  POSITIONING_BAND_WEEKS_13,
  POSITIONING_BAND_WEEKS_52,
  POSITIONING_CHART_WEEKS_DEFAULT,
  cotPositioningProfile,
} from './cotPositioningConfig.js'

const rowDate = (r) => normalizeReportDate(recordCotReportDate(r) || r?.date || r?.latest_report_date || '')

function num(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

function blockFromExport(row, groupId) {
  const g = row?.cot_positioning_groups
  if (!g || typeof g !== 'object') return null
  return g[groupId] && typeof g[groupId] === 'object' ? g[groupId] : null
}

function groupSlice(row, groupId) {
  const block = blockFromExport(row, groupId)
  if (block) {
    return {
      available: !!block.available,
      long: num(block.long),
      short: num(block.short),
      net: num(block.net),
      pct_long: num(block.pct_long),
      pct_short: num(block.pct_short),
    }
  }

  const profile = cotPositioningProfile(row?.market, row)
  if ((profile === 'legacy' || profile === 'commodity') && groupId === 'managed_money') {
    const L = num(row?.long_value)
    const S = num(row?.short_value)
    const N = num(row?.net_value)
    if (L == null && S == null) return { available: false }
    const sum = L != null && S != null ? L + S : null
    return {
      available: true,
      long: L,
      short: S,
      net: N != null ? N : L != null && S != null ? L - S : null,
      pct_long: sum > 0 && L != null ? (100 * L) / sum : null,
      pct_short: sum > 0 && S != null ? (100 * S) / sum : null,
    }
  }

  return { available: false }
}

function rollingMinMax(values, window) {
  const out = []
  for (let i = 0; i < values.length; i++) {
    const slice = values.slice(Math.max(0, i - window + 1), i + 1).filter((x) => Number.isFinite(x))
    if (!slice.length) {
      out.push({ min: null, max: null })
    } else {
      out.push({ min: Math.min(...slice), max: Math.max(...slice) })
    }
  }
  return out
}

/** Deduped chronological COT weeks for one market (up to maxWeeks). */
export function buildCotPositioningHistory(allRows, market, maxWeeks = POSITIONING_CHART_WEEKS_DEFAULT) {
  const id = canonicalMarketId(market)
  const byCot = new Map()
  for (const r of allRows || []) {
    if (canonicalMarketId(r.market || r.raw_cftc_market_name) !== id) continue
    if (!isCotRowResolved(r) && r.long_value == null) continue
    const cotKey = recordCotReportDate(r) || rowDate(r)
    if (!cotKey) continue
    const prev = byCot.get(cotKey)
    if (!prev || rowDate(r).localeCompare(rowDate(prev)) >= 0) byCot.set(cotKey, r)
  }
  const rows = [...byCot.values()].sort((a, b) => rowDate(a).localeCompare(rowDate(b)))
  return rows.slice(-maxWeeks)
}

function priceIndexByDate(relationshipMap) {
  if (!relationshipMap?.available || !Array.isArray(relationshipMap.dates)) return null
  const m = new Map()
  const prices = relationshipMap.price_rebased_pct || []
  relationshipMap.dates.forEach((d, i) => {
    const p = num(prices[i])
    if (d && p != null) m.set(String(d).slice(0, 10), p)
  })
  return m.size ? m : null
}

function nearestPricePct(priceByDate, cotDate) {
  if (!priceByDate || !cotDate) return null
  const direct = priceByDate.get(String(cotDate).slice(0, 10))
  if (direct != null) return direct
  const target = Date.parse(cotDate)
  if (!Number.isFinite(target)) return null
  let best = null
  let bestDiff = Infinity
  for (const [d, p] of priceByDate.entries()) {
    const diff = Math.abs(Date.parse(d) - target)
    if (diff < bestDiff) {
      bestDiff = diff
      best = p
    }
  }
  return bestDiff <= 10 * 86400000 ? best : null
}

const SINGLE_GROUP_MAP = {
  managed_money: 'managed_money',
  commercial: 'commercial',
  nonreportable: 'nonreportable',
}

/**
 * @param {object[]} historyRows
 * @param {string} groupId
 * @param {object|null} relationshipMap
 */
export function buildPositioningChartSeries(historyRows, groupId, relationshipMap = null) {
  const priceByDate = priceIndexByDate(relationshipMap)
  const hasPrice = !!priceByDate
  const profile =
    historyRows.length > 0
      ? cotPositioningProfile(historyRows[historyRows.length - 1]?.market, historyRows[historyRows.length - 1])
      : 'commodity'

  const points = historyRows.map((row) => {
    const date = recordCotReportDate(row) || rowDate(row)
    const pick = (id) => {
      const g = groupSlice(row, id)
      return {
        long: num(g.long),
        short: num(g.short),
        net: num(g.net),
        pctLong: num(g.pct_long),
        pctShort: num(g.pct_short),
        available: !!g.available,
      }
    }

    const mm = pick('managed_money')
    const comm = pick('commercial')
    const nr = pick('nonreportable')
    const oi = num(row?.cot_positioning_groups?.open_interest)

    const singleId = SINGLE_GROUP_MAP[groupId] || groupId
    let active = pick(singleId)
    if (groupId === 'combined') {
      active = { available: true, long: null, short: null, net: null, pctLong: null, pctShort: null }
    }

    return {
      date,
      label: date?.slice(0, 10) || '—',
      long: active.long,
      short: active.short,
      net: active.net,
      pctLong: active.pctLong,
      pctShort: active.pctShort,
      available: active.available,
      mmNet: mm.net,
      commNet: comm.net,
      nrNet: nr.net,
      mmAvailable: mm.available,
      commAvailable: comm.available,
      nrAvailable: nr.available,
      openInterest: oi,
      pricePct: hasPrice ? nearestPricePct(priceByDate, date) : null,
      profile,
    }
  })

  const nets = points.map((p) => p.net).filter(Number.isFinite)
  const longs = points.map((p) => p.long).filter(Number.isFinite)
  const shorts = points.map((p) => p.short).filter(Number.isFinite)
  const band13Net = rollingMinMax(
    points.map((p) => p.net),
    POSITIONING_BAND_WEEKS_13,
  )
  const band52Net = rollingMinMax(
    points.map((p) => p.net),
    POSITIONING_BAND_WEEKS_52,
  )

  const series = points.map((p, i) => ({
    ...p,
    band13NetMin: band13Net[i].min,
    band13NetMax: band13Net[i].max,
    band52NetMin: band52Net[i].min,
    band52NetMax: band52Net[i].max,
  }))

  let anyAvailable = false
  if (groupId === 'combined') {
    anyAvailable = series.some((p) => p.mmAvailable || p.commAvailable || p.nrAvailable)
  } else {
    anyAvailable = series.some((p) => p.available)
  }

  return {
    series,
    hasPrice,
    priceLabel: relationshipMap?.price_series_display || relationshipMap?.price_series_id || 'Price (rebased %)',
    profile,
    stats: {
      netMin: nets.length ? Math.min(...nets) : null,
      netMax: nets.length ? Math.max(...nets) : null,
      longMin: longs.length ? Math.min(...longs) : null,
      longMax: longs.length ? Math.max(...longs) : null,
    },
    anyAvailable,
    weeks: series.length,
  }
}
