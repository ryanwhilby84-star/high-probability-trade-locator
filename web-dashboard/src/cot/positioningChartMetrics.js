const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

export const POSITIONING_RANGE_PRESETS = [
  { id: '3m', label: '3M', weeks: 13 },
  { id: '6m', label: '6M', weeks: 26 },
  { id: '1y', label: '1Y', weeks: 52 },
  { id: '3y', label: '3Y', weeks: 156 },
  { id: '5y', label: '5Y', weeks: 260 },
  { id: '10y', label: '10Y', weeks: 520 },
  { id: 'all', label: 'All', weeks: null },
]

export const POSITIONING_DEFAULT_RANGE_ID = '3y'

export function rangePresetById(id) {
  return POSITIONING_RANGE_PRESETS.find((p) => p.id === id) || POSITIONING_RANGE_PRESETS.find((p) => p.id === POSITIONING_DEFAULT_RANGE_ID)
}

export function sliceSeriesByWeeks(series, weeks) {
  if (!Array.isArray(series) || !series.length) return []
  if (!weeks || weeks >= series.length) return series
  return series.slice(series.length - weeks)
}

function deltaAt(series, key, wowKey, weeksBack) {
  const last = series[series.length - 1]
  if (!last) return null
  if (weeksBack === 1 && isNum(last[wowKey])) return last[wowKey]
  const idx = series.length - 1 - weeksBack
  if (idx < 0) return null
  const cur = last[key]
  const prev = series[idx]?.[key]
  if (!isNum(cur) || !isNum(prev)) return null
  return cur - prev
}

export function seriesMetrics(series, key, wowKey) {
  const last = series?.[series.length - 1]
  if (!last) {
    return { value: null, wow: null, w4: null, w13: null, date: null }
  }
  return {
    value: isNum(last[key]) ? last[key] : null,
    wow: deltaAt(series, key, wowKey, 1),
    w4: deltaAt(series, key, wowKey, 4),
    w13: deltaAt(series, key, wowKey, 13),
    date: last.date || last.label || null,
  }
}

export function latestMovementSummary(series) {
  const prev = series.length >= 2 ? series[series.length - 2] : null
  const last = series[series.length - 1] || null
  return {
    latestDate: last?.date || last?.label || '—',
    previousDate: prev?.date || prev?.label || '—',
    ncWow: deltaAt(series, 'institutional_net', 'institutional_wow', 1),
    commercialWow: deltaAt(series, 'commercial_net', 'commercial_wow', 1),
    retailWow: deltaAt(series, 'retail_net', 'retail_wow', 1),
  }
}

export function fmtDelta(v) {
  if (!isNum(v)) return '—'
  const sign = v > 0 ? '+' : ''
  return `${sign}${Math.round(v).toLocaleString()}`
}

export function fmtValue(v) {
  if (!isNum(v)) return '—'
  if (Math.abs(v) < 10 && !Number.isInteger(v)) return Number(v).toFixed(4)
  return Math.round(v).toLocaleString()
}

export function priceSeriesMetrics(series) {
  const last = series?.[series.length - 1]
  if (!last) return { value: null, wow: null, w4: null, w13: null, date: null }
  const wow = series.length >= 2 && isNum(last.price) && isNum(series[series.length - 2]?.price)
    ? last.price - series[series.length - 2].price
    : null
  const w4 = series.length >= 5 && isNum(last.price) && isNum(series[series.length - 5]?.price)
    ? last.price - series[series.length - 5].price
    : null
  const w13 = series.length >= 14 && isNum(last.price) && isNum(series[series.length - 14]?.price)
    ? last.price - series[series.length - 14].price
    : null
  return { value: isNum(last.price) ? last.price : null, wow, w4, w13, date: last.date || last.label }
}
