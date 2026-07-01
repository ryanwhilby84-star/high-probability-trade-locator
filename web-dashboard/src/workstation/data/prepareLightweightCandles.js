const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

/**
 * Normalize weekly OHLC bars for lightweight-charts setData.
 * Filters invalid rows, dedupes by time (last wins), sorts ascending.
 */
export function prepareLightweightCandles(bars) {
  const rejected = []
  const byTime = new Map()

  for (const bar of bars || []) {
    if (!bar) {
      rejected.push({ reason: 'null bar' })
      continue
    }

    const time = isNum(bar.time) ? bar.time : null
    if (time == null) {
      rejected.push({ reason: 'invalid time', date: bar.date })
      continue
    }

    const open = Number(bar.open)
    const high = Number(bar.high)
    const low = Number(bar.low)
    const close = Number(bar.close)

    if (![open, high, low, close].every(isNum)) {
      rejected.push({ reason: 'non-finite ohlc', date: bar.date, time })
      continue
    }

    const hi = Math.max(open, high, low, close)
    const lo = Math.min(open, high, low, close)

    byTime.set(time, { time, open, high: hi, low: lo, close })
  }

  const data = [...byTime.values()].sort((a, b) => a.time - b.time)

  if (rejected.length) {
    console.warn('[workstation] candle bars filtered', {
      rejected: rejected.length,
      kept: data.length,
      sample: rejected.slice(0, 5),
    })
  }

  return data
}

/** Normalize fair-value overlay points for lightweight-charts line series. */
export function prepareLightweightLinePoints(points) {
  const rejected = []
  const byTime = new Map()

  for (const pt of points || []) {
    if (!pt) continue
    const time = isNum(pt.time) ? pt.time : null
    const value = Number(pt.value)
    if (time == null || !isNum(value)) {
      rejected.push({ reason: 'invalid point', time: pt.time, value: pt.value })
      continue
    }
    byTime.set(time, { time, value })
  }

  const data = [...byTime.values()].sort((a, b) => a.time - b.time)

  if (rejected.length) {
    console.warn('[workstation] fair-value points filtered', {
      rejected: rejected.length,
      kept: data.length,
      sample: rejected.slice(0, 5),
    })
  }

  return data
}

export function filterValidCandleBars(bars) {
  return prepareLightweightCandles(bars).map((b) => ({
    ...b,
    date: bars?.find((x) => x?.time === b.time)?.date ?? null,
  }))
}
