import { enrichChartAnalytics } from '../charts/chartAnalytics.js'
import { buildPositioningWorkstationSeries } from './data/buildPositioningWorkstationSeries.js'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

export function filterValidCandleBars(bars) {
  if (!Array.isArray(bars)) return []
  return bars.filter(
    (b) =>
      b &&
      isNum(b.time) &&
      isNum(b.open) &&
      isNum(b.high) &&
      isNum(b.low) &&
      isNum(b.close),
  )
}

export function countSeriesQuality(rows) {
  const list = Array.isArray(rows) ? rows : []
  let nullPrice = 0
  let nullClose = 0
  let nullCommercial = 0
  let nullInstitutional = 0
  let nullRetail = 0
  for (const r of list) {
    if (!isNum(r?.price)) nullPrice += 1
    if (!isNum(r?.close)) nullClose += 1
    if (!isNum(r?.commercial_net)) nullCommercial += 1
    if (!isNum(r?.institutional_net)) nullInstitutional += 1
    if (!isNum(r?.retail_net)) nullRetail += 1
  }
  return {
    rows: list.length,
    nullPrice,
    nullClose,
    nullCommercial,
    nullInstitutional,
    nullRetail,
    firstDate: list[0]?.date ?? null,
    lastDate: list[list.length - 1]?.date ?? null,
  }
}

export function safeBuildPositioningWorkstationSeries(model, priceRec) {
  try {
    const bound = buildPositioningWorkstationSeries(model, priceRec)
    return { ok: true, error: null, ...bound }
  } catch (err) {
    console.error('[workstation] binding failed', err)
    return {
      ok: false,
      error: err,
      rows: [],
      weeklyBars: [],
      priceSource: 'error',
      meta: { error: String(err?.message || err) },
    }
  }
}

export function safeEnrichChartAnalytics(series) {
  if (!Array.isArray(series) || !series.length) {
    return { ok: true, error: null, data: [] }
  }
  try {
    return { ok: true, error: null, data: enrichChartAnalytics(series) }
  } catch (err) {
    console.error('[workstation] enrichChartAnalytics failed', err)
    return { ok: false, error: err, data: series }
  }
}

export function logWorkstationRenderDiagnostics(marketId, payload) {
  console.info('[workstation-render]', {
    market: marketId,
    ...payload,
  })
}
