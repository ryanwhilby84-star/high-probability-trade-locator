const DEFAULT_HORIZONS = [1, 2, 4, 8, 12, 26]

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function percentileBand(percentile, label = 'Commercial') {
  if (!isNum(percentile)) return null
  if (percentile >= 95) return { low: 95, high: 100, label: `${label} ≥ 95th percentile` }
  if (percentile <= 5) return { low: 0, high: 5, label: `${label} ≤ 5th percentile` }
  if (percentile >= 90) return { low: 90, high: 100, label: `${label} ≥ 90th percentile` }
  if (percentile <= 10) return { low: 0, high: 10, label: `${label} ≤ 10th percentile` }

  const low = Math.floor(percentile / 10) * 10
  const high = Math.min(100, low + 10)
  return { low, high, label: `${label} ${low}th–${high}th percentile` }
}

function oppositionBand(commercialPercentile, ncPercentile) {
  if (!isNum(commercialPercentile) || !isNum(ncPercentile)) return null
  if (commercialPercentile <= 10 && ncPercentile >= 90) {
    if (commercialPercentile <= 5 && ncPercentile >= 95) {
      return { low: 95, high: 100, label: 'Non-Commercial ≥ 95th percentile' }
    }
    return { low: 90, high: 100, label: 'Non-Commercial ≥ 90th percentile' }
  }
  if (commercialPercentile >= 90 && ncPercentile <= 10) {
    if (commercialPercentile >= 95 && ncPercentile <= 5) {
      return { low: 0, high: 5, label: 'Non-Commercial ≤ 5th percentile' }
    }
    return { low: 0, high: 10, label: 'Non-Commercial ≤ 10th percentile' }
  }
  return null
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
  if (n < 20) return { grade: 'FAIR', label: 'Sample quality: fair', tone: 'fair' }
  if (n < 40) return { grade: 'SOLID', label: 'Sample quality: solid', tone: 'solid' }
  return { grade: 'STRONG', label: 'Sample quality: strong', tone: 'strong' }
}

function currentSetupState(commercialPercentile, ncPercentile) {
  const cExtreme = commercialPercentile <= 5 || commercialPercentile >= 95
  const cStrong = commercialPercentile <= 10 || commercialPercentile >= 90
  const crossExtreme = (commercialPercentile <= 5 && ncPercentile >= 95) || (commercialPercentile >= 95 && ncPercentile <= 5)
  const crossStrong = (commercialPercentile <= 10 && ncPercentile >= 90) || (commercialPercentile >= 90 && ncPercentile <= 10)

  if (crossExtreme) return { grade: 'MAX EXTREME', label: 'Commercial and Non-Commercial are at opposing extremes', tone: 'max' }
  if (crossStrong) return { grade: 'CROSS-GROUP EXTREME', label: 'Commercial and Non-Commercial are strongly opposed', tone: 'cross' }
  if (cExtreme) return { grade: 'MAX COMMERCIAL EXTREME', label: 'Commercial positioning is at a historical tail', tone: 'max' }
  if (cStrong) return { grade: 'COMMERCIAL EXTREME', label: 'Commercial positioning is historically extreme', tone: 'cross' }
  return { grade: 'NORMAL', label: 'Current Commercial positioning is not in an extreme tail', tone: 'normal' }
}

function expectedDirection(percentile) {
  return percentile >= 50 ? 'up' : 'down'
}

function directionalReturn(rawReturnPct, direction) {
  return direction === 'down' ? -rawReturnPct : rawReturnPct
}

function priceExtreme(week, side) {
  const price = week?.price || {}
  if (side === 'high') return isNum(price.high) ? price.high : (isNum(price.close) ? price.close : null)
  return isNum(price.low) ? price.low : (isNum(price.close) ? price.close : null)
}

function buildEpisodes({ orderedDates, weeklyView, selectedIndex, commercialBand, ncBand }) {
  const episodes = []
  let current = null

  for (let index = 0; index < selectedIndex; index += 1) {
    const date = orderedDates[index]
    const week = weeklyView[date]
    const commercialMatches = week && inBand(week?.commercial?.percentile, commercialBand)
    const ncMatches = !ncBand || inBand(week?.non_commercial?.percentile, ncBand)
    const qualifies = commercialMatches && ncMatches && isNum(week?.price?.close) && week.price.close !== 0

    if (!qualifies) {
      if (current) episodes.push(current)
      current = null
      continue
    }

    if (!current) {
      current = { startIndex: index, endIndex: index, startDate: date, endDate: date, startWeek: week, weeks: [{ date, index, week }] }
    } else {
      current.endIndex = index
      current.endDate = date
      current.weeks.push({ date, index, week })
    }
  }
  if (current) episodes.push(current)

  return episodes.map((episode) => ({ ...episode, durationWeeks: episode.weeks.length }))
}

function episodeOutcome({ episode, horizon, orderedDates, weeklyView, selectedIndex, direction }) {
  const startClose = episode.startWeek?.price?.close
  const futureIndex = episode.startIndex + horizon
  if (!isNum(startClose) || startClose === 0 || futureIndex > selectedIndex) return null

  const futureDate = orderedDates[futureIndex]
  const endClose = weeklyView[futureDate]?.price?.close
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

    const favorable = direction === 'down' ? ((startClose - low) / startClose) * 100 : ((high / startClose) - 1) * 100
    const adverse = direction === 'down' ? ((startClose - high) / startClose) * 100 : ((low / startClose) - 1) * 100

    if (favorable > mfePct) mfePct = favorable
    if (adverse < maePct) maePct = adverse

    const elapsed = i - episode.startIndex
    if (weeksTo5 == null && favorable >= 5) weeksTo5 = elapsed
    if (weeksTo10 == null && favorable >= 10) weeksTo10 = elapsed
  }

  return { date: episode.startDate, futureDate, durationWeeks: episode.durationWeeks, rawReturnPct, directionalReturnPct: dirReturnPct, won: dirReturnPct > 0, mfePct, maePct, weeksTo5, weeksTo10 }
}

function summarizeEpisodeOutcomes(samples) {
  if (!samples.length) {
    return { sampleCount: 0, hitRatePct: null, medianReturnPct: null, meanReturnPct: null, expectancyPct: null, medianWinnerPct: null, medianLoserPct: null, rewardRiskMedian: null, medianMfePct: null, medianMaePct: null, hit5RatePct: null, hit10RatePct: null, medianWeeksTo5: null, medianWeeksTo10: null, confidence: confidenceForSample(0) }
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

export function buildBasicLookback({ weeklyView, dates = [], selectedDate, horizons = DEFAULT_HORIZONS } = {}) {
  if (!weeklyView || !selectedDate) return null

  const orderedDates = (Array.isArray(dates) ? dates : []).filter((date, index, all) => date && weeklyView[date] && all.indexOf(date) === index)
  const selectedIndex = orderedDates.indexOf(selectedDate)
  if (selectedIndex < 0) return null

  const selectedWeek = weeklyView[selectedDate]
  const selectedPercentile = selectedWeek?.commercial?.percentile
  const selectedNcPercentile = selectedWeek?.non_commercial?.percentile
  const commercialBand = percentileBand(selectedPercentile, 'Commercial')
  const ncBand = oppositionBand(selectedPercentile, selectedNcPercentile)

  if (!commercialBand) {
    return { available: false, version: 'evidence-v3', reason: 'Commercial percentile is unavailable for this week.', selectedDate }
  }

  const direction = expectedDirection(selectedPercentile)
  const episodes = buildEpisodes({ orderedDates, weeklyView, selectedIndex, commercialBand, ncBand })
  const outcomes = {}

  for (const horizon of horizons) {
    const samples = episodes.map((episode) => episodeOutcome({ episode, horizon, orderedDates, weeklyView, selectedIndex, direction })).filter(Boolean)
    outcomes[horizon] = { horizonWeeks: horizon, ...summarizeEpisodeOutcomes(samples), samples }
  }

  const primaryHorizon = outcomes[12]?.sampleCount ? 12 : [...horizons].reverse().find((h) => outcomes[h]?.sampleCount) || horizons[0]
  const primary = outcomes[primaryHorizon] || summarizeEpisodeOutcomes([])
  const setupState = currentSetupState(selectedPercentile, selectedNcPercentile)

  return {
    available: true,
    version: 'evidence-v3',
    basis: ncBand ? 'commercial_plus_noncommercial_extreme_episode' : 'commercial_percentile_episode',
    selectedDate,
    selectedPercentile,
    selectedNcPercentile,
    expectedDirection: direction,
    bandLow: commercialBand.low,
    bandHigh: commercialBand.high,
    cohortLabel: ncBand ? `${commercialBand.label} + ${ncBand.label}` : commercialBand.label,
    currentSetupState: setupState,
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
    methodology: ncBand
      ? 'Historical episodes require both the current Commercial extreme and the opposing Non-Commercial extreme. Consecutive qualifying weeks count as one episode; only point-in-time observable forward prices are used.'
      : 'Historical episodes match the current Commercial percentile band. Consecutive qualifying weeks count as one episode; only point-in-time observable forward prices are used.',
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
