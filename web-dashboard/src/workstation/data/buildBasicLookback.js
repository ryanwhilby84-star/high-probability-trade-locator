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
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2
}

function mean(values) {
  if (!values.length) return null
  return values.reduce((sum, value) => sum + value, 0) / values.length
}

function confidenceForSample(n) {
  if (n < 6) return { grade: 'REJECT', label: 'Too few episodes', tone: 'reject' }
  if (n < 10) return { grade: 'LOW', label: 'Small sample', tone: 'low' }
  if (n < 20) return { grade: 'FAIR', label: 'Developing evidence', tone: 'fair' }
  if (n < 40) return { grade: 'SOLID', label: 'Useful sample', tone: 'solid' }
  return { grade: 'STRONG', label: 'Large sample', tone: 'strong' }
}

function expectedDirection(percentile) {
  return percentile >= 50 ? 'up' : 'down'
}

function directionalReturn(rawReturnPct, direction) {
  return direction === 'down' ? -rawReturnPct : rawReturnPct
}

function priceExtreme(week, side) {
  const price = week?.price || {}
  if (side === 'high') {
    if (isNum(price.high)) return price.high
    if (isNum(price.close)) return price.close
  } else {
    if (isNum(price.low)) return price.low
    if (isNum(price.close)) return price.close
  }
  return null
}

function buildEpisodes({ orderedDates, weeklyView, selectedIndex, band }) {
  const episodes = []
  let current = null

  for (let index = 0; index < selectedIndex; index += 1) {
    const date = orderedDates[index]
    const week = weeklyView[date]
    const qualifies = week && inBand(week?.commercial?.percentile, band) && isNum(week?.price?.close) && week.price.close !== 0

    if (!qualifies) {
      if (current) episodes.push(current)
      current = null
      continue
    }

    if (!current) {
      current = {
        startIndex: index,
        endIndex: index,
        startDate: date,
        endDate: date,
        startWeek: week,
        weeks: [{ date, index, week }],
      }
    } else {
      current.endIndex = index
      current.endDate = date
      current.weeks.push({ date, index, week })
    }
  }
  if (current) episodes.push(current)

  return episodes.map((episode) => {
    const percentiles = episode.weeks.map((x) => x.week?.commercial?.percentile).filter(isNum)
    return {
      ...episode,
      durationWeeks: episode.weeks.length,
      peakPercentile: percentiles.length ? Math.max(...percentiles) : null,
      troughPercentile: percentiles.length ? Math.min(...percentiles) : null,
    }
  })
}

function episodeOutcome({ episode, horizon, orderedDates, weeklyView, selectedIndex, direction }) {
  const startClose = episode.startWeek?.price?.close
  const futureIndex = episode.startIndex + horizon
  if (!isNum(startClose) || startClose === 0 || futureIndex > selectedIndex) return null

  const futureDate = orderedDates[futureIndex]
  const futureWeek = weeklyView[futureDate]
  const endClose = futureWeek?.price?.close
  if (!isNum(endClose)) return null

  const rawReturnPct = ((endClose / startClose) - 1) * 100
  if (!Number.isFinite(rawReturnPct)) return null
  const dirReturnPct = directionalReturn(rawReturnPct, direction)

  let mfePct = 0
  let maePct = 0
  let weeksTo5 = null
  let weeksTo10 = null

  for (let i = episode.startIndex + 1; i <= futureIndex; i += 1) {
    const week = weeklyView[orderedDates[i]]
    const high = priceExtreme(week, 'high')
    const low = priceExtreme(week, 'low')
    if (!isNum(high) || !isNum(low)) continue

    const favorable = direction === 'down'
      ? ((startClose - low) / startClose) * 100
      : ((high / startClose) - 1) * 100
    const adverse = direction === 'down'
      ? ((startClose - high) / startClose) * 100
      : ((low / startClose) - 1) * 100

    if (favorable > mfePct) mfePct = favorable
    if (adverse < maePct) maePct = adverse

    const elapsed = i - episode.startIndex
    if (weeksTo5 == null && favorable >= 5) weeksTo5 = elapsed
    if (weeksTo10 == null && favorable >= 10) weeksTo10 = elapsed
  }

  return {
    date: episode.startDate,
    futureDate,
    durationWeeks: episode.durationWeeks,
    rawReturnPct,
    directionalReturnPct: dirReturnPct,
    won: dirReturnPct > 0,
    mfePct,
    maePct,
    weeksTo5,
    weeksTo10,
  }
}

function summarizeEpisodeOutcomes(samples) {
  if (!samples.length) {
    return {
      sampleCount: 0,
      hitRatePct: null,
      medianReturnPct: null,
      meanReturnPct: null,
      expectancyPct: null,
      medianWinnerPct: null,
      medianLoserPct: null,
      rewardRiskMedian: null,
      medianMfePct: null,
      medianMaePct: null,
      hit5RatePct: null,
      hit10RatePct: null,
      medianWeeksTo5: null,
      medianWeeksTo10: null,
      confidence: confidenceForSample(0),
    }
  }

  const directional = samples.map((s) => s.directionalReturnPct)
  const winners = directional.filter((v) => v > 0)
  const losers = directional.filter((v) => v <= 0)
  const winMedian = median(winners)
  const lossMedian = median(losers)
  const fiveHits = samples.filter((s) => s.weeksTo5 != null)
  const tenHits = samples.filter((s) => s.weeksTo10 != null)

  return {
    sampleCount: samples.length,
    hitRatePct: (winners.length / samples.length) * 100,
    medianReturnPct: median(directional),
    meanReturnPct: mean(directional),
    expectancyPct: mean(directional),
    medianWinnerPct: winMedian,
    medianLoserPct: lossMedian,
    rewardRiskMedian: isNum(winMedian) && isNum(lossMedian) && lossMedian !== 0 ? winMedian / Math.abs(lossMedian) : null,
    medianMfePct: median(samples.map((s) => s.mfePct)),
    medianMaePct: median(samples.map((s) => s.maePct)),
    hit5RatePct: (fiveHits.length / samples.length) * 100,
    hit10RatePct: (tenHits.length / samples.length) * 100,
    medianWeeksTo5: median(fiveHits.map((s) => s.weeksTo5)),
    medianWeeksTo10: median(tenHits.map((s) => s.weeksTo10)),
    confidence: confidenceForSample(samples.length),
  }
}

/**
 * Episode-based point-in-time COT lookback.
 *
 * Consecutive weeks in the same Commercial percentile band are treated as ONE
 * historical positioning episode, preventing a prolonged extreme from inflating
 * the sample count. Every forward outcome must be fully observable by the selected
 * week. Seasonality is deliberately excluded from this engine.
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
      version: 'evidence-v2',
      reason: 'Commercial percentile is unavailable for this week.',
      selectedDate,
    }
  }

  const direction = expectedDirection(selectedPercentile)
  const episodes = buildEpisodes({ orderedDates, weeklyView, selectedIndex, band })
  const outcomes = {}

  for (const horizon of horizons) {
    const samples = episodes
      .map((episode) => episodeOutcome({ episode, horizon, orderedDates, weeklyView, selectedIndex, direction }))
      .filter(Boolean)

    outcomes[horizon] = {
      horizonWeeks: horizon,
      ...summarizeEpisodeOutcomes(samples),
      samples,
    }
  }

  const primaryHorizon = outcomes[12]?.sampleCount ? 12 : [...horizons].reverse().find((h) => outcomes[h]?.sampleCount) || horizons[0]
  const primary = outcomes[primaryHorizon] || summarizeEpisodeOutcomes([])

  return {
    available: true,
    version: 'evidence-v2',
    basis: 'commercial_percentile_episode',
    selectedDate,
    selectedPercentile,
    expectedDirection: direction,
    bandLow: band.low,
    bandHigh: band.high,
    cohortLabel: band.label,
    priorMatchCount: episodes.reduce((sum, e) => sum + e.durationWeeks, 0),
    priorEpisodeCount: episodes.length,
    priorEpisodeDates: episodes.map((e) => e.startDate),
    outcomes,
    horizons: [...horizons],
    primaryHorizon,
    primaryEvidence: primary,
    sampleConfidence: primary.confidence,
    pointInTime: true,
    seasonalityIncluded: false,
    methodology:
      'Consecutive matching Commercial-percentile weeks are collapsed into one episode. Statistics use only completed historical episodes and only forward prices observable by the selected week. Seasonality is excluded.',
  }
}

export function attachBasicLookbacks({ weeklyView, dates = [], horizons = DEFAULT_HORIZONS } = {}) {
  if (!weeklyView) return weeklyView
  for (const date of dates) {
    const week = weeklyView[date]
    if (!week) continue
    week.basicLookback = buildBasicLookback({ weeklyView, dates, selectedDate: date, horizons })
  }
  return weeklyView
}

export { DEFAULT_HORIZONS }
