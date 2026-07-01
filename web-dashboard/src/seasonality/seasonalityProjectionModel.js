/** Seasonality projection chart — data prep & decision labels (display only). */

import { yearlyForwardReturns } from '../components/SeasonalityV2Panel.jsx'
import { confidenceLabel, fmtPct, horizonConfidence } from './seasonalityDecision.js'
import { dataSourceLabel } from './seasonalityControls.js'

export const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

export const WEEK_TICKS = [1, 5, 9, 14, 18, 23, 27, 31, 36, 40, 45, 49, 52]
const MONTH_LABELS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', '']

export function weekTickLabel(week) {
  const monthIdx = [1, 5, 9, 14, 18, 23, 27, 31, 36, 40, 45, 49]
  const i = monthIdx.indexOf(week)
  return i >= 0 ? MONTH_LABELS[i] : week === 52 ? 'Dec' : ''
}

/** Merge legacy export blocks into unified chart_series when needed. */
export function chartRowsFromBlock(block) {
  if (!block || typeof block !== 'object') return []
  if (Array.isArray(block.chart_series) && block.chart_series.length) {
    return block.chart_series
  }
  const current = block.current_path || []
  const forward = block.forward_projection || []
  const byWeek = new Map()
  for (const r of current) {
    byWeek.set(r.week, { week: r.week, actual: r.index, close: r.close, divergence: null })
  }
  for (const r of forward) {
    const row = byWeek.get(r.week) || { week: r.week }
    if (isNum(r.anchor)) row.actual = r.anchor
    row.proj_3y = r.proj_3y
    row.proj_5y = r.proj_5y
    row.proj_10y = r.proj_10y
    byWeek.set(r.week, row)
  }
  return Array.from({ length: 52 }, (_, i) => byWeek.get(i + 1) || { week: i + 1 })
}

function projectionKey(toggles) {
  if (toggles?.show10y) return 'proj_10y'
  if (toggles?.show5y) return 'proj_5y'
  if (toggles?.show3y) return 'proj_3y'
  return 'proj_10y'
}

function seasonalKey(toggles) {
  if (toggles?.show10y) return 'seasonal_10y'
  if (toggles?.show5y) return 'seasonal_5y'
  if (toggles?.show3y) return 'seasonal_3y'
  return 'seasonal_10y'
}

/** Bernd-style paths: actual YTD + forward projection from current indexed level. */
export function buildProjectionChartRows(block, toggles) {
  const rows = chartRowsFromBlock(block)
  const currentWeek = isNum(block?.current_week) ? block.current_week : 1
  const projField = projectionKey(toggles)
  const primarySeasonal = seasonalKey(toggles)

  return rows.map((r) => {
    const week = r.week
    const actual = isNum(r.actual) ? r.actual : null
    const proj = isNum(r[projField]) ? r[projField] : null
    const s10 = isNum(r.seasonal_10y) ? r.seasonal_10y : null
    const s5 = isNum(r.seasonal_5y) ? r.seasonal_5y : null
    const s3 = isNum(r.seasonal_3y) ? r.seasonal_3y : null
    const primary = isNum(r[primarySeasonal]) ? r[primarySeasonal] : s10 ?? s5 ?? s3

    return {
      ...r,
      currentYearPath: week <= currentWeek && actual != null ? actual : null,
      forwardSeasonalPath:
        toggles?.forwardProjection !== false && week >= currentWeek && proj != null ? proj : null,
      primarySeasonal: primary,
      divergencePts: actual != null && primary != null ? actual - primary : r.divergence ?? null,
    }
  })
}

function isReliabilityLabel(direction) {
  const d = String(direction || '')
  return d === 'Low sample reliability' || d === 'Insufficient history'
}

function directionTone(direction) {
  if (isReliabilityLabel(direction)) return 'warn'
  const d = String(direction || 'Neutral')
  if (d === 'Bullish') return 'bull'
  if (d === 'Bearish') return 'bear'
  return 'neutral'
}

function normalizeDirection(row, grade) {
  if (!row) return '—'
  const dir = String(row.direction || 'Neutral')
  if (grade !== 'A' || (row.sample_years ?? 0) < 5 || !row.available || isReliabilityLabel(dir)) {
    if ((row.sample_years ?? 0) < 1) return 'Insufficient history'
    return 'Low sample reliability'
  }
  return dir
}

export function medianForwardReturn(histPaths, currentWeek, horizonWeeks) {
  const outcomes = yearlyForwardReturns(histPaths, currentWeek, horizonWeeks)
  if (!outcomes.length) return null
  const vals = outcomes.map((o) => o.return_pct).filter(isNum).sort((a, b) => a - b)
  if (!vals.length) return null
  const mid = Math.floor(vals.length / 2)
  return vals.length % 2 ? vals[mid] : (vals[mid - 1] + vals[mid]) / 2
}

export function buildHorizonCards(block, toggles) {
  const grade = block?.trust_grade || 'C'
  const forwardRead = block?.forward_read || {}
  const overall = confidenceLabel(block?.confidence, forwardRead.next_8w)
  const hist = block?.hist_year_paths || []
  const currentWeek = block?.current_week ?? 1

  const horizons = [
    { key: '4W', weeks: 4, data: forwardRead.next_4w },
    { key: '8W', weeks: 8, data: forwardRead.next_8w },
    { key: '12W', weeks: 12, data: forwardRead.next_12w },
  ]

  return horizons.map(({ key, weeks, data }) => {
    const gatedDir = normalizeDirection(data, grade)
    const reliable =
      grade === 'A' && data?.available && (data.sample_years ?? 0) >= 5 && !isReliabilityLabel(data?.direction)
    const conf = reliable
      ? horizonConfidence(data, overall) || (overall === 'High' ? 'High' : overall === 'Medium' ? 'Medium' : 'Low')
      : '—'
    const median = medianForwardReturn(hist, currentWeek, weeks)

    return {
      key,
      direction: gatedDir,
      directionTone: directionTone(gatedDir),
      avgReturn: reliable ? fmtPct(data?.avg_return_pct) : '—',
      medianReturn: reliable && isNum(median) ? fmtPct(median) : '—',
      winRate: reliable && isNum(data?.win_rate_pct) ? `${data.win_rate_pct}%` : '—',
      sampleYears: data?.sample_years ?? '—',
      confidence: conf,
      reliable,
    }
  })
}

export function buildDecisionLabel(block) {
  const grade = block?.trust_grade || 'C'
  const f4 = block?.forward_read?.next_4w
  const f8 = block?.forward_read?.next_8w
  const f12 = block?.forward_read?.next_12w

  if (!block?.available) {
    return { label: 'Insufficient seasonal history', tone: 'warn', detail: block?.reason || 'Unavailable' }
  }
  if (grade === 'C') {
    return {
      label: 'Insufficient seasonal history',
      tone: 'warn',
      detail: block.trust_notes || block.reason || 'Trust grade C',
    }
  }
  if (!f8?.available || (f8.sample_years ?? 0) < 5 || isReliabilityLabel(f8.direction)) {
    return {
      label: 'Low sample reliability',
      tone: 'warn',
      detail: `Forward read sample n=${f8?.sample_years ?? 0} (minimum 5 required).`,
    }
  }
  if (grade === 'B') {
    return {
      label: 'Context only',
      tone: 'neutral',
      detail: block.trust_notes || 'Sparse curve — directional reads are indicative only.',
    }
  }

  const d8 = String(f8.direction || 'Neutral')
  const d4 = f4?.available && !isReliabilityLabel(f4.direction) ? String(f4.direction) : null
  const d12 = f12?.available && !isReliabilityLabel(f12.direction) ? String(f12.direction) : null

  const dirs = [d4, d8, d12].filter(Boolean)
  const unique = [...new Set(dirs)]
  if (unique.length > 1) {
    return {
      label: 'Seasonality mixed',
      tone: 'neutral',
      detail: `4W/8W/12W windows disagree (${dirs.filter(Boolean).join(', ')}).`,
    }
  }

  if (d8 === 'Bullish') {
    return { label: 'Seasonality supports longs', tone: 'bull', detail: `8W avg ${fmtPct(f8.avg_return_pct)}, n=${f8.sample_years}.` }
  }
  if (d8 === 'Bearish') {
    return { label: 'Seasonality supports shorts', tone: 'bear', detail: `8W avg ${fmtPct(f8.avg_return_pct)}, n=${f8.sample_years}.` }
  }
  return {
    label: 'Seasonality neutral',
    tone: 'neutral',
    detail: `8W avg ${fmtPct(f8.avg_return_pct)}, n=${f8.sample_years}.`,
  }
}

export function buildDivergenceLabel(block) {
  const div = block?.divergence_read
  if (div?.available) {
    const pos = div.position
    if (pos === 'above') return 'Current year stronger than seasonal path'
    if (pos === 'below') return 'Current year weaker than seasonal path'
    if (Math.abs(div.divergence ?? 0) >= 8) return 'Current year diverging from seasonal path'
    return 'Current year tracking seasonal path'
  }
  const align = String(block?.path_alignment || '')
  if (/diverg/i.test(align)) return 'Current year diverging from seasonal path'
  if (/above|stronger/i.test(align)) return 'Current year stronger than seasonal path'
  if (/below|weaker/i.test(align)) return 'Current year weaker than seasonal path'
  if (/follow|track|inline/i.test(align)) return 'Current year tracking seasonal path'
  return align || '—'
}

export function buildMetadata(block, generatedAt) {
  return {
    dataSource: dataSourceLabel(block),
    canonicalSource: block?.canonical_source || '—',
    canonicalSymbol: block?.canonical_symbol || '—',
    historyYears: block?.years_of_history ?? block?.years_used ?? '—',
    trustGrade: block?.trust_grade || 'C',
    trustNotes: block?.trust_notes || '',
    sampleYears8w: block?.forward_read?.next_8w?.sample_years ?? '—',
    lastUpdated: generatedAt || block?.latest_price?.date || '—',
    latestPrice: block?.latest_price,
  }
}
