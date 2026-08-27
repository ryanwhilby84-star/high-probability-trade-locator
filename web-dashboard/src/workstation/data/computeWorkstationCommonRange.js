const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function sortDates(dates) {
  return [...dates].filter(Boolean).sort()
}

/**
 * Compute overlapping date window between COT series and plottable weekly OHLC.
 */
export function computeWorkstationCommonRange(cotRows, weeklyBars) {
  const cotDates = sortDates((cotRows || []).map((r) => String(r.date || r.label || '').slice(0, 10)))
  const ohlcDates = sortDates((weeklyBars || []).map((b) => String(b.date || '').slice(0, 10)))

  const cotFirst = cotDates[0] ?? null
  const cotLast = cotDates[cotDates.length - 1] ?? null
  const ohlcFirst = ohlcDates[0] ?? null
  const ohlcLast = ohlcDates[ohlcDates.length - 1] ?? null

  let commonFirst = null
  let commonLast = null
  if (cotFirst && cotLast && ohlcFirst && ohlcLast) {
    commonFirst = cotFirst > ohlcFirst ? cotFirst : ohlcFirst
    commonLast = cotLast < ohlcLast ? cotLast : ohlcLast
    if (commonFirst > commonLast) {
      commonFirst = null
      commonLast = null
    }
  }

  const cotRowsInCommon =
    commonFirst && commonLast
      ? (cotRows || []).filter((r) => {
          const d = String(r.date || r.label || '').slice(0, 10)
          return d >= commonFirst && d <= commonLast
        })
      : []

  const barsInCommon =
    commonFirst && commonLast
      ? (weeklyBars || []).filter((b) => {
          const d = String(b.date || '').slice(0, 10)
          return d >= commonFirst && d <= commonLast
        })
      : []

  let missingOhlcWeeks = 0
  if (commonFirst && commonLast && cotRowsInCommon.length) {
    for (const row of cotRowsInCommon) {
      const d = String(row.date || row.label || '').slice(0, 10)
      const hasBar = barsInCommon.some((b) => b.date === d || b.time === row.time)
      if (!hasBar && !isNum(row.close)) missingOhlcWeeks += 1
    }
  }

  const incompleteHistory = Boolean(
    cotFirst && ohlcFirst && commonFirst && commonFirst > cotFirst,
  )

  return {
    cotFirst,
    cotLast,
    ohlcFirst,
    ohlcLast,
    commonFirst,
    commonLast,
    cotRows: cotDates.length,
    ohlcRows: ohlcDates.length,
    commonRows: cotRowsInCommon.length,
    missingOhlcWeeks,
    incompleteHistory,
  }
}

export function sliceRowsToDateRange(rows, start, end) {
  if (!start || !end || !Array.isArray(rows)) return rows || []
  return rows.filter((r) => {
    const d = String(r.date || r.label || '').slice(0, 10)
    return d >= start && d <= end
  })
}

export function sliceBarsToDateRange(bars, start, end) {
  if (!start || !end || !Array.isArray(bars)) return bars || []
  return bars.filter((b) => {
    const d = String(b.date || '').slice(0, 10)
    return d >= start && d <= end
  })
}
