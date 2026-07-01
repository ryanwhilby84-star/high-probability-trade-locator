/**
 * COT workstation series — full weekly history: price + NC net + NR net.
 * Source: cot_3y_series_latest.json market block (all available weeks).
 */

/** Minimum acceptable history for analysis (5 calendar years). */
export const COT_WS_MIN_WEEKS = 260
/** Default visible window (5Y). */
export const COT_WS_DEFAULT_WEEKS = 260
/** Rolling window for optional 3Y extreme bands. */
export const COT_WS_EXTREME_WEEKS = 156

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function weeklyChange(series, key, i, exportedKey) {
  const pt = series[i]
  if (exportedKey && isNum(pt?.[exportedKey])) return pt[exportedKey]
  if (i <= 0) return null
  const cur = pt?.[key]
  const prev = series[i - 1]?.[key]
  if (!isNum(cur) || !isNum(prev)) return null
  return cur - prev
}

function extremesForKey(series, key, windowWeeks = COT_WS_EXTREME_WEEKS) {
  const slice = series.slice(-Math.min(windowWeeks, series.length))
  const vals = slice.map((p) => p[key]).filter(isNum)
  if (!vals.length) return { high: null, low: null, rows: 0 }
  return {
    high: Math.max(...vals),
    low: Math.min(...vals),
    rows: vals.length,
  }
}

export function fmtHistoryMonth(iso) {
  if (!iso) return '—'
  return String(iso).slice(0, 7)
}

export function priceSourceLabel(audit, storeKey = null) {
  if (!audit && !storeKey) return 'none'
  const parts = []
  const store = storeKey || audit?.price_store_key
  if (store) parts.push(String(store))
  if (audit?.fred_fallback_series) parts.push(`fred:${audit.fred_fallback_series}`)
  if (audit?.oanda_fallback_symbol) parts.push(`oanda:${audit.oanda_fallback_symbol}`)
  return parts.length ? parts.join('+') : 'none'
}

export function computePriceCoverage(block, model) {
  const cotWeeks = model?.cotWeeks ?? block?.weeks ?? 0
  const matched = model?.priceWeeksMatched ?? block?.price_weeks ?? 0
  const matchPct = cotWeeks > 0 ? Math.round((matched / cotWeeks) * 1000) / 10 : null
  const audit = model?.priceAudit || block?.price_audit || null
  const priceSource = priceSourceLabel(audit, model?.priceStoreKey)

  let status = 'NO_COT'
  let reason = 'No COT history for this instrument.'
  if (cotWeeks > 0) {
    if (matchPct == null) {
      status = 'FAIL'
      reason = 'Unable to compute price match percentage.'
    } else if (matchPct >= 95) {
      status = 'OK'
      reason = null
    } else if (matchPct >= 50) {
      status = 'PARTIAL'
      reason = `Price matched for ${matchPct}% of COT weeks (target >= 95%).`
    } else {
      status = 'FAIL'
      reason = `Price matched for ${matchPct}% of COT weeks (target >= 95%).`
    }
  }

  return {
    cotWeeks,
    priceMatched: matched,
    matchPct,
    priceSource,
    status,
    reason,
    priceIncomplete: status === 'PARTIAL' || status === 'FAIL',
  }
}

export function buildCotWorkstation(block) {
  if (!block || !Array.isArray(block.series) || !block.series.length) {
    return { available: false, reason: 'No COT series for this market.' }
  }

  const source = block.series
  const series = source.map((p, i) => ({
    label: String(p.date || '').slice(0, 10),
    date: String(p.date || '').slice(0, 10),
    price: isNum(p.price) ? p.price : null,
    price_date: p.price_date || null,
    institutional_net: isNum(p.institutional_net) ? p.institutional_net : null,
    institutional_wow: weeklyChange(source, 'institutional_net', i, 'one_week_net_change'),
    retail_net: isNum(p.retail_net) ? p.retail_net : null,
    retail_wow: weeklyChange(source, 'retail_net', i, null),
    commercial_net: isNum(p.commercial_net) ? p.commercial_net : null,
    commercial_wow: weeklyChange(source, 'commercial_net', i, null),
  }))

  const price = series.map((p) => p.price)
  const institutional = series.map((p) => p.institutional_net)
  const retail = series.map((p) => p.retail_net)
  const commercial = series.map((p) => p.commercial_net)
  const rowCount = series.length
  const earliestDate = block.earliest_date || series[0]?.date || null
  const latestDate = block.latest_date || series[series.length - 1]?.date || null
  const historyIncomplete = rowCount < COT_WS_MIN_WEEKS

  const base = {
    available: true,
    market: block.market,
    weeks: rowCount,
    historyLabel: `${fmtHistoryMonth(earliestDate)} → ${fmtHistoryMonth(latestDate)}`,
    historyIncomplete,
    historyWarning: historyIncomplete
      ? `COT history incomplete — only ${rowCount} weeks available (target ${COT_WS_MIN_WEEKS}+). Run: python -m hptl.cot.run_legacy_cot && python -m hptl.confluence.cot_tracked_backfill && python -m hptl.cot.run_cot_3y_series`
      : null,
    earliestDate,
    latestDate,
    series,
    hasPrice: price.some(isNum),
    hasInstitutional: institutional.some(isNum),
    hasRetail: block.has_retail !== false && retail.some(isNum),
    hasCommercial: block.has_commercial !== false && commercial.some(isNum),
    priceWeeks: block.price_weeks ?? price.filter(isNum).length,
    cotWeeks: block.weeks ?? rowCount,
    priceAudit: block.price_audit || null,
    priceWeeksMatched: (block.series || []).filter((p) => isNum(p.price)).length,
    priceStoreKey: block.price_audit?.price_store_key || null,
    institutionalGroup: block.institutional_group || 'Non-Commercial',
    retailGroup: block.retail_group || 'Non-Reportable',
    extremes: {
      institutional: extremesForKey(series, 'institutional_net'),
      retail: extremesForKey(series, 'retail_net'),
      commercial: extremesForKey(series, 'commercial_net'),
    },
  }

  return {
    ...base,
    priceCoverage: computePriceCoverage(block, base),
  }
}

export const COT_WS_RANGE_PRESETS = [
  { id: '52', weeks: 52, label: '1Y' },
  { id: '104', weeks: 104, label: '2Y' },
  { id: '156', weeks: 156, label: '3Y' },
  { id: '260', weeks: 260, label: '5Y' },
  { id: '520', weeks: 520, label: '10Y' },
  { id: 'all', weeks: null, label: 'All' },
]

export function sliceCotWorkstationRange(series, startIndex, endIndex) {
  const len = series.length
  if (!len) return []
  const start = Math.max(0, Math.min(startIndex, len - 1))
  const end = Math.max(start, Math.min(endIndex, len - 1))
  return series.slice(start, end + 1)
}

export function presetRange(seriesLength, weeks) {
  if (!seriesLength) return { startIndex: 0, endIndex: 0 }
  if (weeks == null || weeks >= seriesLength) {
    return { startIndex: 0, endIndex: seriesLength - 1 }
  }
  return { startIndex: Math.max(0, seriesLength - weeks), endIndex: seriesLength - 1 }
}
