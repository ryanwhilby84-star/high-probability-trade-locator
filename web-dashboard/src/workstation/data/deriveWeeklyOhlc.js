/**
 * Derive ISO-week OHLC from daily bars (visualization layer only).
 * Matches backend ISO week bucketing: last bar date per %G-W%V week wins for close.
 */

function isoWeekKey(dateStr) {
  const d = new Date(`${dateStr}T12:00:00Z`)
  if (Number.isNaN(d.getTime())) return dateStr.slice(0, 7)
  const th = new Date(d)
  th.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7))
  const yearStart = new Date(Date.UTC(th.getUTCFullYear(), 0, 1))
  const week = Math.ceil(((th - yearStart) / 86400000 + 1) / 7)
  return `${th.getUTCFullYear()}-W${String(week).padStart(2, '0')}`
}

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

/**
 * @param {Array<{date, open, high, low, close, volume}>} daily
 * @returns {Array<{date, open, high, low, close, volume}>}
 */
export function deriveWeeklyOhlcFromDaily(daily) {
  if (!Array.isArray(daily) || !daily.length) return []
  /** @type {Map<string, {date, open, high, low, close, volume}>} */
  const buckets = new Map()

  for (const bar of daily) {
    const date = String(bar?.date || '').slice(0, 10)
    if (!date) continue
    const open = Number(bar.open)
    const high = Number(bar.high)
    const low = Number(bar.low)
    const close = Number(bar.close)
    if (![open, high, low, close].every(isNum)) continue

    const wk = isoWeekKey(date)
    const prev = buckets.get(wk)
    if (!prev) {
      buckets.set(wk, {
        date,
        open,
        high,
        low,
        close,
        volume: Number(bar.volume) || 0,
      })
      continue
    }
    if (date < prev.date) {
      buckets.set(wk, {
        ...prev,
        open,
        high: Math.max(prev.high, high),
        low: Math.min(prev.low, low),
      })
    } else {
      buckets.set(wk, {
        date,
        open: prev.open,
        high: Math.max(prev.high, high),
        low: Math.min(prev.low, low),
        close,
        volume: (prev.volume || 0) + (Number(bar.volume) || 0),
      })
    }
  }

  return [...buckets.values()].sort((a, b) => a.date.localeCompare(b.date))
}

/**
 * Prefer native weekly OHLC; fall back to daily-derived ISO weeks.
 */
export function resolveWeeklyOhlc(priceRec) {
  if (!priceRec) return { weekly: [], source: 'none' }
  const native = Array.isArray(priceRec.weekly) ? priceRec.weekly : []
  if (native.length > 0) return { weekly: native, source: 'native_weekly' }
  const daily = Array.isArray(priceRec.daily) ? priceRec.daily : []
  if (daily.length > 0) {
    return { weekly: deriveWeeklyOhlcFromDaily(daily), source: 'derived_from_daily' }
  }
  return { weekly: [], source: 'none' }
}
