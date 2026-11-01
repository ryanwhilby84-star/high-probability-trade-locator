/**
 * Fill missing COT-week price observations from the VPS daily candle store.
 * Never interpolate across gaps or alter COT positioning/event values.
 * COT reports use Tuesday observation dates; choose the latest daily close
 * at or before that date, no older than seven calendar days.
 */
export function mergeHistoricalPrices(series, daily) {
  if (!Array.isArray(series) || !Array.isArray(daily) || !daily.length) return series
  const candles = daily
    .filter((bar) => /^\d{4}-\d{2}-\d{2}$/.test(String(bar?.date || '')) &&
      Number.isFinite(Number(bar.close)) && bar.close != null)
    .map((bar) => ({ date: bar.date, close: Number(bar.close) }))
    .sort((a, b) => a.date.localeCompare(b.date))

  if (!candles.length) return series
  let index = -1
  return series.map((row) => {
    const date = String(row.date || '').slice(0, 10)
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return row
    while (index + 1 < candles.length && candles[index + 1].date <= date) index++
    const candle = candles[index]
    if (!candle) return row
    const age = (Date.parse(date + 'T00:00:00Z') - Date.parse(candle.date + 'T00:00:00Z')) / 86400000
    if (age < 0 || age > 7) return row
    // Prefer the independently refreshed daily candle to stale embedded COT prices.
    return { ...row, price: candle.close, price_date: candle.date }
  })
}
