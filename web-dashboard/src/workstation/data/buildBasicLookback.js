const DEFAULT_HORIZONS = [1, 2, 4, 8, 12, 26]

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function percentileBand(percentile) {
  if (!isNum(percentile)) return null
  if (percentile >= 90) return { low: 90, high: 100, label: 'Commercial ≥ 90th percentile' }
  if (percentile <= 10) return { low: 0, high: 10, label: 'Commercial ≤ 10th percentile' }

  const low = Math.floor(percentile / 10) * 10
  const high = Math.min(100, low + 10)
  return {
    low,
    high,
    label: `Commercial ${low}th–${high}th percentile`,
  }
}

function inBand(value, band) {
  if (!isNum(value) || !band) return false
  return value >= band.low && value <= band.high
}

function median(values) {
  if (!values.length) return null
  const sorted = [...values].sort((a, b) => a - b)
  const mid = Math.floor(sorted.length / 2)
  return sorted.length % 2
    ? sorted[mid]
    : (sorted[mid - 1] + sorted[mid]) / 2
}

function mean(values) {
  if (!values.length) return null
  return values.reduce((sum, value) => sum + value, 0) / values.length
}

function summarizeReturns(values) {
  if (!values.length) {
    return {
      sampleCount: 0,
      medianReturnPct: null,
      meanReturnPct: null,
      positiveRatePct: null,
      negativeRatePct: null,
      bestReturnPct: null,
      worstReturnPct: null,
    }
  }

  const positives = values.filter((v) => v > 0).length
  const negatives = values.filter((v) => v < 0).length

  return {
    sampleCount: values.length,
    medianReturnPct: median(values),
    meanReturnPct: mean(values),
    positiveRatePct: (positives / values.length) * 100,
    negativeRatePct: (negatives / values.length) * 100,
    bestReturnPct: Math.max(...values),
    worstReturnPct: Math.min(...values),
  }
}

/**
 * Basic point-in-time historical lookback.
 *
 * Version 1 deliberately uses one transparent state variable only: the selected
 * week's Commercial expanding percentile bucket. Every analogue must be earlier
 * than the selected week, and each forward return must finish on or before the
 * selected week. That makes historical clicks genuinely point-in-time and avoids
 * leaking information that was not yet known on the selected date.
 *
 * This is intentionally the foundation, not the final analogue model. Later
 * versions can add NC/NR state, trajectory, price location, seasonality and macro.
 */
export function buildBasicLookback({
  weeklyView,
  dates = [],
  selectedDate,
  horizons = DEFAULT_HORIZONS,
} = {}) {
  if (!weeklyView || !selectedDate) return null

  const orderedDates = (Array.isArray(dates) ? dates : [])
    .filter((date, index, all) => date && weeklyView[date] && all.indexOf(date) === index)
  const selectedIndex = orderedDates.indexOf(selectedDate)
  if (selectedIndex < 0) return null

  const selectedWeek = weeklyView[selectedDate]
  const selectedPercentile = selectedWeek?.commercial?.percentile
  const band = percentileBand(selectedPercentile)

  if (!band) {
    return {
      available: false,
      version: 'basic-v1',
      reason: 'Commercial percentile is unavailable for this week.',
      selectedDate,
    }
  }

  const priorMatches = []
  for (let index = 0; index < selectedIndex; index += 1) {
    const date = orderedDates[index]
    const week = weeklyView[date]
    if (!week || !inBand(week?.commercial?.percentile, band)) continue
    if (!isNum(week?.price?.close) || week.price.close === 0) continue
    priorMatches.push({ date, index, week })
  }

  const outcomes = {}
  for (const horizon of horizons) {
    const returns = []
    const samples = []

    for (const match of priorMatches) {
      const futureIndex = match.index + horizon
      // Strict point-in-time guard: never use a forward price beyond the selected week.
      if (futureIndex > selectedIndex) continue
      const futureDate = orderedDates[futureIndex]
      const futureWeek = weeklyView[futureDate]
      const startClose = match.week?.price?.close
      const endClose = futureWeek?.price?.close
      if (!isNum(startClose) || startClose === 0 || !isNum(endClose)) continue

      const returnPct = ((endClose / startClose) - 1) * 100
      if (!Number.isFinite(returnPct)) continue
      returns.push(returnPct)
      samples.push({
        date: match.date,
        futureDate,
        returnPct,
      })
    }

    outcomes[horizon] = {
      horizonWeeks: horizon,
      ...summarizeReturns(returns),
      samples,
    }
  }

  return {
    available: true,
    version: 'basic-v1',
    basis: 'commercial_expanding_percentile',
    selectedDate,
    selectedPercentile,
    bandLow: band.low,
    bandHigh: band.high,
    cohortLabel: band.label,
    priorMatchCount: priorMatches.length,
    priorMatchDates: priorMatches.map((m) => m.date),
    outcomes,
    horizons: [...horizons],
    pointInTime: true,
    methodology:
      'Prior weeks in the same Commercial percentile band. Forward returns are only included when the full horizon was already observable by the selected week.',
  }
}

export function attachBasicLookbacks({ weeklyView, dates = [], horizons = DEFAULT_HORIZONS } = {}) {
  if (!weeklyView) return weeklyView
  for (const date of dates) {
    const week = weeklyView[date]
    if (!week) continue
    week.basicLookback = buildBasicLookback({
      weeklyView,
      dates,
      selectedDate: date,
      horizons,
    })
  }
  return weeklyView
}

export { DEFAULT_HORIZONS }
