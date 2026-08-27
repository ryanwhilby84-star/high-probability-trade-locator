/**
 * Rolling 52-week location from workstation price series.
 * Mirrors hptl.location.engine.price_percentile + _score_from_percentile.
 * Uses only the price column already aligned to COT weeks (no alternate series).
 */

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

const BIAS_BULLISH = 'Bullish'
const BIAS_NEUTRAL = 'Neutral'
const BIAS_BEARISH = 'Bearish'

/** Empirical percentile rank within window (0–100), same as Python price_percentile. */
export function pricePercentile52(closes, window = 52) {
  if (!closes?.length || closes.length < 12) return null
  const windowCloses = closes.length >= window ? closes.slice(-window) : closes
  const current = windowCloses[windowCloses.length - 1]
  if (!isNum(current)) return null
  const rank = windowCloses.filter((c) => c <= current).length / windowCloses.length
  return rank * 100
}

export function locationScoreFromPercentile(pct) {
  if (!isNum(pct)) return null
  return Math.round(Math.min(10, Math.max(0, Math.abs(pct - 50) / 5)) * 10) / 10
}

export function locationBiasFromPercentile(pct) {
  if (!isNum(pct)) return null
  if (pct <= 33) return BIAS_BULLISH
  if (pct >= 67) return BIAS_BEARISH
  return BIAS_NEUTRAL
}

/**
 * @param {object[]} series — COT workstation rows with `date` / `label` and `price`
 * @returns {{ series: object[], stats: object }}
 */
export function computeLocationSeriesFromPrices(series) {
  if (!Array.isArray(series) || !series.length) {
    return {
      series: [],
      stats: {
        source: 'workstation_price',
        historyLength: 0,
        valuesAvailable: 0,
        valuesRendered: 0,
        firstDate: null,
        lastDate: null,
        wired: false,
      },
    }
  }

  const closes = []
  let valuesAvailable = 0
  let firstDate = null
  let lastDate = null
  let lastPct = null
  let lastScore = null
  let lastBias = null

  const enriched = series.map((row) => {
    const dateKey = String(row.date || row.label || '').slice(0, 10)
    if (isNum(row.price)) closes.push(row.price)

    const pct = pricePercentile52(closes)
    const score = locationScoreFromPercentile(pct)
    const bias = locationBiasFromPercentile(pct)

    if (isNum(pct)) {
      valuesAvailable += 1
      if (!firstDate) firstDate = dateKey
      lastDate = dateKey
      lastPct = pct
      lastScore = score
      lastBias = bias
    }

    return {
      ...row,
      location_percentile_52w: isNum(pct) ? pct : null,
      location: isNum(score) ? score : null,
      location_bias: bias,
    }
  })

  return {
    series: enriched,
    stats: {
      source: 'workstation_price',
      snapshotSource: 'location_latest.json (subtitle fallback only)',
      historyLength: series.length,
      priceWeeks: closes.length,
      valuesAvailable,
      valuesRendered: valuesAvailable,
      firstDate,
      lastDate,
      lastPercentile: lastPct,
      lastScore,
      lastBias,
      wired: valuesAvailable > 0,
    },
  }
}
