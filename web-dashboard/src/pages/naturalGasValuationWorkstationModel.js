/** Pure helpers for Natural Gas Valuation Workstation (research UI).
 *  Frontend presentation only — no valuation maths or research regeneration.
 */

export const SERIES_WALKFORWARD = 'walk_forward'
export const SERIES_FROZEN = 'frozen_v2'

export const BOTTOM_SERIES = {
  fair: 'fair',
  deviation: 'deviation',
  bucket: 'bucket',
}

export const FOCUS_SCALE_LIMIT = 40
export const DEFAULT_SCALE_MODE = 'focus' // focus | full

export const BUCKET_LEVEL = {
  materially_undervalued: -2,
  undervalued: -1,
  near_fair: 0,
  overvalued: 1,
  materially_overvalued: 2,
}

export const BUCKET_LABELS = {
  materially_undervalued: 'Materially undervalued',
  undervalued: 'Undervalued',
  near_fair: 'Near fair value',
  overvalued: 'Overvalued',
  materially_overvalued: 'Materially overvalued',
}

export const BUCKET_SHORT = {
  materially_undervalued: 'MATERIAL UNDERVALUATION',
  undervalued: 'UNDERVALUED',
  near_fair: 'NEAR FAIR VALUE',
  overvalued: 'OVERVALUED',
  materially_overvalued: 'MATERIAL OVERVALUATION',
}

export const BUCKET_STRIP_COLORS = {
  materially_undervalued: '#065f46',
  undervalued: '#16a34a',
  near_fair: '#64748b',
  overvalued: '#dc2626',
  materially_overvalued: '#7f1d1d',
  unavailable: '#1e293b',
}

export const FORWARD_HORIZONS = [1, 2, 4, 8, 12]

export const SIGN_CONVENTION =
  'Valuation deviation: Positive = market above model fair value = overvalued · Negative = market below model fair value = undervalued'

export const DEVIATION_BAND_LINES = [
  { price: 15, color: 'rgba(248, 113, 113, 0.55)', title: '', axisLabelVisible: false, lineWidth: 1 },
  { price: 5, color: 'rgba(248, 113, 113, 0.35)', title: '', axisLabelVisible: false, lineWidth: 1 },
  { price: -5, color: 'rgba(52, 211, 153, 0.35)', title: '', axisLabelVisible: false, lineWidth: 1 },
  { price: -15, color: 'rgba(52, 211, 153, 0.55)', title: '', axisLabelVisible: false, lineWidth: 1 },
]

/** Zone overlays for Focus scale (−40…+40). Top of plot = +40. */
export const ZONE_LABELS = [
  { id: 'mat_over', text: 'MATERIAL OVERVALUATION', top: '0%', height: '31.25%' },
  { id: 'over', text: 'OVERVALUED', top: '31.25%', height: '12.5%' },
  { id: 'near', text: 'NEAR FAIR VALUE', top: '43.75%', height: '12.5%' },
  { id: 'under', text: 'UNDERVALUED', top: '56.25%', height: '12.5%' },
  { id: 'mat_under', text: 'MATERIAL UNDERVALUATION', top: '68.75%', height: '31.25%' },
]

export function seriesKey(mode) {
  return mode === 'frozen' ? SERIES_FROZEN : SERIES_WALKFORWARD
}

export function isoToBarTime(iso) {
  const d = String(iso || '').slice(0, 10)
  if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return null
  const [y, m, day] = d.split('-').map(Number)
  return Math.floor(Date.UTC(y, m - 1, day) / 1000)
}

export function formatReportDate(iso) {
  const d = String(iso || '').slice(0, 10)
  if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return '—'
  const [y, m, day] = d.split('-').map(Number)
  const dt = new Date(Date.UTC(y, m - 1, day))
  return dt
    .toLocaleDateString('en-GB', {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
      timeZone: 'UTC',
    })
    .toUpperCase()
}

export function valuationBucket(deviationPct) {
  if (deviationPct == null || !Number.isFinite(Number(deviationPct))) return null
  const d = Number(deviationPct)
  if (d <= -15) return 'materially_undervalued'
  if (d < -5) return 'undervalued'
  if (d <= 5) return 'near_fair'
  if (d < 15) return 'overvalued'
  return 'materially_overvalued'
}

export function bucketLabel(id) {
  return BUCKET_LABELS[id] || id || '—'
}

export function confidenceFromQuality(qualityStatus) {
  const q = String(qualityStatus || '').toUpperCase()
  if (q === 'OK') return 'OK'
  if (q === 'INSUFFICIENT_TRAIN') return 'Insufficient history'
  if (q === 'FALLBACK_V1_ELIGIBLE') return 'Fallback eligible'
  if (q === 'FIT_FAILED') return 'Fit failed'
  if (q === 'UNAVAILABLE') return 'Unavailable'
  return qualityStatus || '—'
}

export function modelTypeLabel(mode) {
  return mode === 'frozen' ? 'Frozen v2 diagnostic' : 'Walk-forward point-in-time'
}

export function signInterpretation(deviationPct) {
  if (deviationPct == null || !Number.isFinite(Number(deviationPct))) {
    return { side: 'unknown', text: 'Valuation deviation unavailable for this week.' }
  }
  const d = Number(deviationPct)
  if (d < 0) {
    return {
      side: 'undervalued',
      text: 'Negative valuation deviation — market below model fair value (undervalued).',
    }
  }
  if (d > 0) {
    return {
      side: 'overvalued',
      text: 'Positive valuation deviation — market above model fair value (overvalued).',
    }
  }
  return {
    side: 'fair',
    text: 'Zero valuation deviation — market at model fair value.',
  }
}

/** Decisive, threshold-aware interpretation for the large card + inspector. */
export function decisiveInterpretation(deviationPct) {
  if (deviationPct == null || !Number.isFinite(Number(deviationPct))) {
    return {
      bucket: null,
      headline: 'UNAVAILABLE',
      strength: 'none',
      detail: 'Valuation deviation is unavailable for this observation.',
    }
  }
  const d = Number(deviationPct)
  const bucket = valuationBucket(d)
  if (bucket === 'materially_overvalued') {
    return {
      bucket,
      headline: 'MATERIALLY OVERVALUED',
      strength: 'strong_contradiction',
      detail:
        'Historically associated with weaker forward returns. Strong contradiction for new longs. Not a guarantee.',
    }
  }
  if (bucket === 'overvalued') {
    return {
      bucket,
      headline: 'MILDLY OVERVALUED',
      strength: 'weak_contradiction',
      detail: 'Weak contradiction for a bullish trade, not a reversal signal.',
    }
  }
  if (bucket === 'near_fair') {
    return {
      bucket,
      headline: 'NEAR FAIR VALUE',
      strength: 'neutral',
      detail: 'Valuation is neutral and should not materially affect the trade thesis.',
    }
  }
  if (bucket === 'undervalued') {
    return {
      bucket,
      headline: 'UNDERVALUED',
      strength: 'weak_support',
      detail: 'Mild historically supportive association. Weak evidence only, not a reversal signal.',
    }
  }
  return {
    bucket,
    headline: 'MATERIALLY UNDERVALUED',
    strength: 'strong_support',
    detail:
      'Historically associated with stronger forward returns. Supportive confluence for bullish setups. Not a guarantee.',
  }
}

export function historicalInterpretation(bucket) {
  return decisiveInterpretation(
    bucket === 'materially_undervalued'
      ? -20
      : bucket === 'undervalued'
        ? -10
        : bucket === 'near_fair'
          ? 0
          : bucket === 'overvalued'
            ? 10
            : bucket === 'materially_overvalued'
              ? 20
              : null,
  ).detail
}

/** Clip displayed deviation for Focus scale; preserve true value separately. */
export function applyFocusScale(points, scaleMode = DEFAULT_SCALE_MODE, limit = FOCUS_SCALE_LIMIT) {
  if (scaleMode !== 'focus') {
    return {
      displayPoints: points || [],
      overflowMarkers: [],
      scaleMin: null,
      scaleMax: null,
      clipped: false,
    }
  }
  const displayPoints = []
  const overflowMarkers = []
  for (const p of points || []) {
    if (p?.time == null) continue
    const v = Number(p.value)
    if (!Number.isFinite(v)) {
      // Preserve whitespace so linked panes keep identical logical indices.
      displayPoints.push({ time: p.time })
      continue
    }
    if (v > limit) {
      displayPoints.push({ time: p.time, value: limit })
      overflowMarkers.push({ time: p.time, trueValue: v, direction: 'up' })
    } else if (v < -limit) {
      displayPoints.push({ time: p.time, value: -limit })
      overflowMarkers.push({ time: p.time, trueValue: v, direction: 'down' })
    } else {
      displayPoints.push({ time: p.time, value: v })
    }
  }
  return {
    displayPoints,
    overflowMarkers,
    scaleMin: -limit,
    scaleMax: limit,
    clipped: true,
  }
}

export function buildSharedTimeline(weeks) {
  const rows = []
  const times = []
  for (const w of weeks || []) {
    const time = isoToBarTime(w.model_week)
    if (time == null) continue
    rows.push({ time, date: w.model_week, label: w.model_week })
    times.push(time)
  }
  return { timelineRows: rows, times }
}

export function buildPricePoints(weeks) {
  return (weeks || [])
    .map((w) => {
      const time = isoToBarTime(w.model_week)
      const v = Number(w.market_price)
      if (time == null || !Number.isFinite(v)) return null
      return { time, value: v }
    })
    .filter(Boolean)
}

export function buildFairPoints(weeks, mode) {
  const key = seriesKey(mode)
  return (weeks || [])
    .map((w) => {
      const time = isoToBarTime(w.model_week)
      const v = Number((w[key] || {}).fair_value)
      if (time == null || !Number.isFinite(v)) return null
      return { time, value: v }
    })
    .filter(Boolean)
}

export function buildDeviationPoints(weeks, mode) {
  const key = seriesKey(mode)
  return (weeks || [])
    .map((w) => {
      const time = isoToBarTime(w.model_week)
      const v = Number((w[key] || {}).deviation_pct)
      if (time == null || !Number.isFinite(v)) return null
      return { time, value: v }
    })
    .filter(Boolean)
}

export function buildBucketPoints(weeks, mode) {
  const key = seriesKey(mode)
  return (weeks || [])
    .map((w) => {
      const time = isoToBarTime(w.model_week)
      const block = w[key] || {}
      const bucket = block.valuation_bucket || valuationBucket(block.deviation_pct)
      const level = BUCKET_LEVEL[bucket]
      if (time == null || level == null) return null
      return { time, value: level }
    })
    .filter(Boolean)
}

export function buildBucketStripCells(weeks, mode) {
  const key = seriesKey(mode)
  return (weeks || []).map((w, index) => {
    const block = w[key] || {}
    const bucket =
      block.fair_value != null
        ? block.valuation_bucket || valuationBucket(block.deviation_pct)
        : null
    return {
      index,
      time: isoToBarTime(w.model_week),
      week: w.model_week,
      bucket,
      color: BUCKET_STRIP_COLORS[bucket] || BUCKET_STRIP_COLORS.unavailable,
    }
  })
}

export function forwardReturnsAtIndex(weeks, index) {
  const out = {}
  const p0 = Number(weeks?.[index]?.market_price)
  for (const h of FORWARD_HORIZONS) {
    if (!Number.isFinite(p0) || p0 <= 0) {
      out[h] = null
      continue
    }
    const p1 = Number(weeks?.[index + h]?.market_price)
    out[h] = Number.isFinite(p1) ? (100 * (p1 - p0)) / p0 : null
  }
  return out
}

export function mfeMaeAtIndex(weeks, index, horizon = 12) {
  const p0 = Number(weeks?.[index]?.market_price)
  if (!Number.isFinite(p0) || p0 <= 0) return { mfe: null, mae: null }
  const end = Math.min((weeks || []).length - 1, index + horizon)
  if (end <= index) return { mfe: null, mae: null }
  let mfe = 0
  let mae = 0
  for (let i = index; i <= end; i += 1) {
    const p = Number(weeks[i]?.market_price)
    if (!Number.isFinite(p)) continue
    const ret = (100 * (p - p0)) / p0
    if (ret > mfe) mfe = ret
    if (ret < mae) mae = ret
  }
  return { mfe, mae }
}

export function driverContributions(week, mode) {
  const block = (week && week[seriesKey(mode)]) || {}
  const coef = block.coefficients || {}
  const s = Number(week?.storage_surplus_bcf)
  const y = Number(week?.production_yoy_pct)
  const intercept = Number(coef.intercept)
  const bS = Number(coef.storage_surplus_bcf)
  const bY = Number(coef.production_yoy_pct)
  const storageLog = Number.isFinite(s) && Number.isFinite(bS) ? bS * s : null
  const productionLog = Number.isFinite(y) && Number.isFinite(bY) ? bY * y : null
  const recon =
    Number.isFinite(intercept) && storageLog != null && productionLog != null
      ? intercept + storageLog + productionLog
      : null
  return {
    storage_log_contribution: storageLog,
    production_log_contribution: productionLog,
    intercept: Number.isFinite(intercept) ? intercept : null,
    storage_coef: Number.isFinite(bS) ? bS : null,
    production_coef: Number.isFinite(bY) ? bY : null,
    log_price_recon: recon,
  }
}

export function deviationPercentile(weeks, mode, index) {
  const key = seriesKey(mode)
  const vals = []
  for (const w of weeks || []) {
    const v = Number(w?.[key]?.deviation_pct)
    if (Number.isFinite(v)) vals.push(v)
  }
  const target = Number(weeks?.[index]?.[key]?.deviation_pct)
  if (!Number.isFinite(target) || !vals.length) return null
  const below = vals.filter((v) => v <= target).length
  return (100 * below) / vals.length
}

export function bucketRunContext(weeks, mode, index) {
  const key = seriesKey(mode)
  const bucket =
    weeks?.[index]?.[key]?.valuation_bucket ||
    valuationBucket(weeks?.[index]?.[key]?.deviation_pct)
  if (!bucket) return { weeks_in_bucket: null, median_duration: null }
  let start = index
  while (start > 0) {
    const prev =
      weeks[start - 1]?.[key]?.valuation_bucket ||
      valuationBucket(weeks[start - 1]?.[key]?.deviation_pct)
    if (prev !== bucket) break
    start -= 1
  }
  let end = index
  while (end < (weeks || []).length - 1) {
    const next =
      weeks[end + 1]?.[key]?.valuation_bucket ||
      valuationBucket(weeks[end + 1]?.[key]?.deviation_pct)
    if (next !== bucket) break
    end += 1
  }
  const durations = []
  let i = 0
  while (i < (weeks || []).length) {
    const b =
      weeks[i]?.[key]?.valuation_bucket || valuationBucket(weeks[i]?.[key]?.deviation_pct)
    if (b !== bucket) {
      i += 1
      continue
    }
    let j = i
    while (
      j < weeks.length &&
      (weeks[j]?.[key]?.valuation_bucket || valuationBucket(weeks[j]?.[key]?.deviation_pct)) ===
        bucket
    ) {
      j += 1
    }
    durations.push(j - i)
    i = j
  }
  const sorted = [...durations].sort((a, b) => a - b)
  const mid = Math.floor(sorted.length / 2)
  const median =
    sorted.length === 0
      ? null
      : sorted.length % 2
        ? sorted[mid]
        : (sorted[mid - 1] + sorted[mid]) / 2
  return {
    weeks_in_bucket: index - start + 1,
    run_length: end - start + 1,
    median_duration: median,
  }
}

export function timelinesAreSynchronized(weeks, mode = 'walkforward') {
  const { times } = buildSharedTimeline(weeks)
  const priceTimes = new Set(buildPricePoints(weeks).map((p) => p.time))
  const fairTimes = new Set(buildFairPoints(weeks, mode).map((p) => p.time))
  if (!times.length) return false
  for (const t of fairTimes) {
    if (!priceTimes.has(t) || !times.includes(t)) return false
  }
  return true
}

export function assertLinkedVisibleRanges(priceRange, valuationRange, selectedWeek, stripRange = null) {
  const pf = priceRange?.from
  const pt = priceRange?.to
  const vf = valuationRange?.from
  const vt = valuationRange?.to
  let ok =
    pf != null &&
    pt != null &&
    vf != null &&
    vt != null &&
    Number(pf) === Number(vf) &&
    Number(pt) === Number(vt)
  if (stripRange && ok) {
    ok =
      Number(stripRange.from) === Number(pf) && Number(stripRange.to) === Number(pt)
  }
  return {
    ok,
    price_visible_from: pf ?? null,
    price_visible_to: pt ?? null,
    valuation_visible_from: vf ?? null,
    valuation_visible_to: vt ?? null,
    selected_price_week: selectedWeek ?? null,
    selected_valuation_week: selectedWeek ?? null,
    independent_navigation_forbidden: true,
  }
}

export function deriveBucketEvents(weeks, mode, cooldown = 4) {
  const key = seriesKey(mode)
  const buckets = Object.keys(BUCKET_LABELS)
  const streakOutside = Object.fromEntries(buckets.map((b) => [b, cooldown]))
  const events = []
  let prev = null
  for (let i = 0; i < (weeks || []).length; i += 1) {
    const block = weeks[i]?.[key] || {}
    const b =
      block.valuation_bucket ||
      (block.fair_value != null ? valuationBucket(block.deviation_pct) : null)
    if (b == null || block.fair_value == null) {
      for (const k of buckets) streakOutside[k] += 1
      prev = null
      continue
    }
    const entered = b !== prev
    if (entered && streakOutside[b] >= cooldown) {
      events.push({
        index: i,
        bucket: b,
        week: weeks[i].model_week,
        time: isoToBarTime(weeks[i].model_week),
      })
    }
    for (const k of buckets) {
      streakOutside[k] = k === b ? 0 : streakOutside[k] + 1
    }
    prev = b
  }
  return events
}

export function findAdjacentIndex(indexes, currentIndex, direction) {
  if (!indexes?.length) return null
  if (currentIndex == null || currentIndex < 0) {
    return direction > 0 ? indexes[0] : indexes[indexes.length - 1]
  }
  if (direction > 0) {
    const next = indexes.find((i) => i > currentIndex)
    return next == null ? null : next
  }
  for (let k = indexes.length - 1; k >= 0; k -= 1) {
    if (indexes[k] < currentIndex) return indexes[k]
  }
  return null
}

export function returnTone(v) {
  if (v == null || !Number.isFinite(Number(v))) return 'neutral'
  const n = Number(v)
  if (Math.abs(n) < 0.25) return 'neutral'
  return n > 0 ? 'positive' : 'negative'
}

export function inspectorForWeek(week, mode, weeks = null, weekIndex = -1, bucketOutcomes = null) {
  if (!week) return null
  const block = week[seriesKey(mode)] || {}
  const idx =
    weekIndex >= 0
      ? weekIndex
      : Array.isArray(weeks)
        ? weeks.findIndex((w) => w.model_week === week.model_week)
        : -1
  const forward = idx >= 0 ? forwardReturnsAtIndex(weeks, idx) : {}
  const path = idx >= 0 ? mfeMaeAtIndex(weeks, idx, 12) : { mfe: null, mae: null }
  const contrib = driverContributions(week, mode)
  const bucket = block.valuation_bucket ?? valuationBucket(block.deviation_pct)
  const sign = signInterpretation(block.deviation_pct)
  const decisive = decisiveInterpretation(block.deviation_pct)
  const run = idx >= 0 ? bucketRunContext(weeks, mode, idx) : {}
  const pct = idx >= 0 ? deviationPercentile(weeks, mode, idx) : null
  const stats4 = bucketOutcomes?.bucket_forward_stats?.[bucket]?.['4'] || {}
  return {
    week: week.model_week,
    report_date: week.model_week,
    report_date_label: formatReportDate(week.model_week),
    market_price: week.market_price,
    fair_value: block.fair_value ?? null,
    deviation_pct: block.deviation_pct ?? null,
    valuation_bucket: bucket,
    valuation_classification: bucketLabel(bucket),
    state_headline: decisive.headline,
    interpretation_strength: decisive.strength,
    storage_surplus_bcf: week.storage_surplus_bcf ?? null,
    storage_observation_date: week.storage_observation_date ?? null,
    production_yoy_pct: week.production_yoy_pct ?? null,
    production_observation_date: week.production_observation_date ?? null,
    model_type: block.model_type ?? modelTypeLabel(mode),
    historical_model_version: block.model_type ?? modelTypeLabel(mode),
    mode_label: modelTypeLabel(mode),
    coefficients: block.coefficients ?? null,
    training_window: block.training_window ?? null,
    quality_status: week.quality_status ?? null,
    confidence: confidenceFromQuality(week.quality_status),
    in_sample_r2: block.training_window?.in_sample_r2 ?? null,
    forward_returns: forward,
    mfe: path.mfe,
    mae: path.mae,
    storage_log_contribution: contrib.storage_log_contribution,
    production_log_contribution: contrib.production_log_contribution,
    intercept: contrib.intercept,
    log_price_recon: contrib.log_price_recon,
    sign,
    interpretation: decisive.detail,
    decisive,
    sign_convention: SIGN_CONVENTION,
    deviation_percentile: pct,
    weeks_in_bucket: run.weeks_in_bucket ?? null,
    bucket_median_duration: run.median_duration ?? null,
    bucket_mean_forward_4w: stats4.mean_forward_return_pct ?? null,
    bucket_hit_rate_4w: stats4.positive_return_frequency ?? null,
  }
}

export function bottomSeriesPoints(weeks, bottomSeries, mode) {
  if (bottomSeries === BOTTOM_SERIES.fair) return buildFairPoints(weeks, mode)
  if (bottomSeries === BOTTOM_SERIES.bucket) return buildBucketPoints(weeks, mode)
  return buildDeviationPoints(weeks, mode)
}

export function currentStateFromWeeks(weeks, mode, livePrice = null) {
  const key = seriesKey(mode)
  let latest = null
  let latestIndex = -1
  for (let i = (weeks || []).length - 1; i >= 0; i -= 1) {
    if (weeks[i]?.[key]?.fair_value != null) {
      latest = weeks[i]
      latestIndex = i
      break
    }
  }
  if (!latest) return null
  const block = latest[key] || {}
  const decisive = decisiveInterpretation(block.deviation_pct)
  return {
    index: latestIndex,
    report_date: latest.model_week,
    report_date_label: formatReportDate(latest.model_week),
    market_price: latest.market_price,
    live_price: livePrice,
    fair_value: block.fair_value ?? null,
    deviation_pct: block.deviation_pct ?? null,
    bucket: decisive.bucket,
    bucket_label: bucketLabel(decisive.bucket),
    state_headline: decisive.headline,
    interpretation: decisive.detail,
    strength: decisive.strength,
    quality_status: latest.quality_status ?? null,
    confidence: confidenceFromQuality(latest.quality_status),
    model: modelTypeLabel(mode),
    published_model_id: 'ng_storage_production_v2',
    sign: signInterpretation(block.deviation_pct),
  }
}

export function resolveActiveSelection({ lockedTime, hoverTime, weekByTime }) {
  const time = lockedTime != null ? lockedTime : hoverTime
  if (time == null) return { time: null, locked: false, hit: null }
  return {
    time,
    locked: lockedTime != null,
    hit: weekByTime.get(time) || null,
  }
}

export function selectedWeekCardModel({ locked, inspector, current }) {
  if (locked && inspector) {
    return {
      title: `SELECTED WEEK: ${inspector.report_date_label}`,
      market: inspector.market_price,
      fair: inspector.fair_value,
      deviation: inspector.deviation_pct,
      headline: inspector.state_headline,
      detail: inspector.interpretation,
      strength: inspector.interpretation_strength,
      locked: true,
    }
  }
  if (current) {
    return {
      title: `CURRENT VALUATION: ${current.report_date_label}`,
      market: current.live_price ?? current.market_price,
      fair: current.fair_value,
      deviation: current.deviation_pct,
      headline: current.state_headline,
      detail: current.interpretation,
      strength: current.strength,
      locked: false,
    }
  }
  return null
}
