/** Seasonal Roadmap view helpers (UI only). */
export const ROADMAP_METHOD_LABEL = 'Seasonal Roadmap'
export const ROADMAP_METHOD_DESCRIPTION =
  'Historical daily moves are normalised by each year’s typical daily volatility, aggregated by trading day, then rescaled to the market’s recent daily volatility. No synthetic noise, interpolation or smoothing.'

export const ROADMAP_HORIZON_WEEKS = [4, 8, 12]

export function classifyRoadmapHorizon(row) {
  if (!row || row.n == null || row.n <= 0) return 'Mixed'
  const { mean, median, bullish_frequency: bull, bearish_frequency: bear } = row
  if (mean != null && median != null && bull != null && mean > 0 && median > 0 && bull > 0.5) return 'Bullish'
  if (mean != null && median != null && bear != null && mean < 0 && median < 0 && bear > 0.5) return 'Bearish'
  return 'Mixed'
}

export function resolveRoadmapSeriesSource(roadmap, useSmoothed) {
  if (!roadmap?.available) return { sourcePath: null, datasetName: null }
  if (useSmoothed && roadmap.smoothed?.full_year?.length) {
    return {
      sourcePath: 'payload.seasonal_roadmap.smoothed.full_year',
      datasetName: roadmap.method?.version || 'volatility_normalised_daily_texture_v4',
      valueKey: 'price',
    }
  }
  if (roadmap.unsmoothed?.full_year?.length) {
    return {
      sourcePath: 'payload.seasonal_roadmap.unsmoothed.full_year',
      datasetName: roadmap.method?.version || 'volatility_normalised_daily_texture_v4',
      valueKey: 'price',
    }
  }
  return { sourcePath: null, datasetName: roadmap.method?.version || 'volatility_normalised_daily_texture_v4' }
}

export function defaultSeasonalView(displayDefaults) {
  return displayDefaults?.seasonal_view || 'roadmap'
}

/** Temporary calendar view: reuse the published pattern, never invent fresh prices
 * or relabel old historical-return probabilities as current statistics. */
export function calendarRoadmaps(roadmap, weekly, useSmoothed, now = new Date()) {
  const today = now.toISOString().slice(0, 10)
  const year = today.slice(0, 4)
  const referencePrice = roadmap?.asof_price ?? roadmap?.anchor_price
  const redate = (points) => (points || []).map(p => ({ ...p, date: year + p.date.slice(4) }))
    .filter(p => Number.isFinite(p.price)).sort((a, b) => a.date.localeCompare(b.date))
  const rebase = (pack) => {
    const points = redate(pack?.full_year)
    const base = points.filter(p => p.date <= today).at(-1)
    if (!base || !referencePrice || base.price <= 0) return pack
    const full = points.map(p => ({ ...p, price: p.price * referencePrice / base.price,
      segment: p.date < today ? 'historical' : p.date === today ? 'today' : 'forward' }))
    // On non-trading days carry the preceding seasonal level to the calendar marker.
    if (!full.some(p => p.date === today)) full.push({ ...base, date: today, price: referencePrice, segment: 'today' })
    full.sort((a, b) => a.date.localeCompare(b.date))
    return { ...pack, full_year: full, historical: full.filter(p => p.date <= today), forward: full.filter(p => p.date >= today) }
  }
  const raw = rebase(roadmap?.unsmoothed)
  const smooth = roadmap?.smoothed ? rebase(roadmap.smoothed) : null
  const selected = (useSmoothed && smooth?.full_year?.length ? smooth : raw)?.full_year || []
  const classify = (a, b) => !Number.isFinite(a) || !Number.isFinite(b) || a <= 0 ? 'Unavailable'
    : Math.abs(b / a - 1) < 0.0001 ? 'Neutral' : b > a ? 'Bullish' : 'Bearish'
  const start = selected.find(p => p.date === today)
  const horizons = Object.fromEntries(ROADMAP_HORIZON_WEEKS.map(w => {
    const end = new Date(today + 'T12:00:00Z'); end.setUTCDate(end.getUTCDate() + w * 7)
    const endDate = end.toISOString().slice(0, 10)
    const target = selected.find(p => p.date >= endDate)
    return [`${w}w`, { direction: classify(start?.price, target?.price), end_date: endDate,
      change_pct: start && target ? (target.price / start.price - 1) * 100 : null }]
  }))
  const calendarRoadmap = roadmap?.available && start ? { ...roadmap, asof: today,
    asof_trading_day: undefined, unsmoothed: raw, smoothed: smooth, full_year: selected,
    calendar_view: true, calendar_horizons: horizons, price_reference_date: roadmap.asof } : roadmap
  const thursday = new Date(today + 'T12:00:00Z')
  thursday.setUTCDate(thursday.getUTCDate() + 4 - (thursday.getUTCDay() || 7))
  const week = Math.ceil(((thursday - new Date(Date.UTC(thursday.getUTCFullYear(), 0, 1))) / 86400000 + 1) / 7)
  const points = weekly?.weekly_points || []
  const weeklyBase = points.find(p => p.week === week)
  if (!calendarRoadmap?.calendar_view || !weekly?.available || !weeklyBase?.price) return { roadmap: calendarRoadmap, weekly }
  const weeklyPoints = points.map(p => ({ ...p, price: p.price * referencePrice / weeklyBase.price,
    cumulative_index: p.price / weeklyBase.price, cumulative_return: p.price / weeklyBase.price - 1,
    segment: p.week < week ? 'historical' : p.week === week ? 'today' : 'forward' }))
  const direction = classify(weeklyBase.price, points.find(p => p.week === week + 4)?.price)
  const monthlyDirection = horizons['4w'].direction
  const priceDate = weekly.actual_price?.latest_price_date || roadmap.asof
  return { roadmap: calendarRoadmap, weekly: { ...weekly, weekly_points: weeklyPoints,
    as_of_date: today, current_week: week, current_direction: direction,
    comparison: { summary_lines: [
      `Seasonal Roadmap (next 4 weeks): ${monthlyDirection}`,
      `Weekly Roadmap (next 4 weeks): ${direction}`,
      `Seasonal agreement: ${monthlyDirection === direction ? monthlyDirection : 'Mixed'}`,
      `Calendar projection: ${today} · Price reference: ${priceDate} (last available close)`,
    ] },
    actual_price: { ...weekly.actual_price, stale: { ...weekly.actual_price?.stale,
      as_of_calendar_date: today, latest_price_date: priceDate,
      stale: (new Date(today) - new Date(priceDate)) / 86400000 > 7 } },
  } }
}
