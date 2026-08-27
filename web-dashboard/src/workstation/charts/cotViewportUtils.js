import { WS_PRICE_SCALE_WIDTH } from './workstationChartOptions.js'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)
const DEFAULT_RIGHT_OFFSET = 8

export function rangesEqual(a, b, epsilon = 0.0001) {
  if (!a || !b) return false
  return Math.abs(a.from - b.from) < epsilon && Math.abs(a.to - b.to) < epsilon
}

export function scalesEqual(a, b, epsilon = 0.0001) {
  if (!a || !b) return false
  return (
    Math.abs(a.barSpacing - b.barSpacing) < epsilon &&
    Math.abs(a.rightOffset - b.rightOffset) < epsilon
  )
}

export function readTimeScale(chart) {
  try {
    const opts = chart.timeScale().options()
    return { barSpacing: opts.barSpacing, rightOffset: opts.rightOffset }
  } catch {
    return { barSpacing: 7, rightOffset: 8 }
  }
}

/** Push master range + scale to every pane (single source of truth). */
export function applyMasterViewport(panes, master) {
  if (!master?.logicalRange) return
  const range =
    master.rowCount > 0
      ? clampLogicalRange(master.logicalRange, master.rowCount)
      : master.logicalRange
  for (const pane of panes.values()) {
    try {
      pane.chart.timeScale().applyOptions(master.scale)
      pane.chart.timeScale().setVisibleLogicalRange({
        from: range.from,
        to: range.to,
      })
    } catch {
      /* ignore stale chart */
    }
  }
}

export function logicalRangeForWeeks(rowCount, weeks) {
  if (!rowCount || rowCount <= 0) return null
  const last = rowCount - 1
  const span = weeks == null || weeks >= rowCount ? rowCount : weeks
  return { from: Math.max(0, rowCount - span), to: last }
}

export function logicalRangeFitAll(rowCount) {
  if (!rowCount || rowCount <= 0) return null
  return { from: 0, to: Math.max(0, rowCount - 1) }
}

/** Map master logical indices to bar timestamps on the shared COT timeline. */
export function logicalRangeToTimeRange(rows, logicalRange) {
  if (!rows?.length || !logicalRange) return null
  const fromIdx = Math.max(0, Math.min(rows.length - 1, Math.floor(logicalRange.from)))
  const toIdx = Math.max(0, Math.min(rows.length - 1, Math.ceil(logicalRange.to)))
  const fromTime = rows[fromIdx]?.time
  const toTime = rows[toIdx]?.time
  if (!isNum(fromTime) || !isNum(toTime)) return null
  return { from: fromTime, to: toTime }
}

function fitBarSpacingForRange(chart, range) {
  const el = chart?.chartElement?.()
  const chartWidth = el?.clientWidth ?? 800
  const plotWidth = Math.max(chartWidth - WS_PRICE_SCALE_WIDTH - 20, 80)
  const barCount = Math.max(range.to - range.from + 1, 1)
  return Math.max(0.35, Math.min(24, plotWidth / barCount))
}

/** Keep pan/zoom inside data; enforce a minimum visible span so zoom cannot lose context. */
export function clampLogicalRange(range, rowCount, { minBars = 6 } = {}) {
  if (!range || !rowCount || rowCount <= 0) return range
  const last = Math.max(0, rowCount - 1)
  const minSpan = Math.min(Math.max(1, minBars - 1), last)

  let from = Number(range.from)
  let to = Number(range.to)
  if (!Number.isFinite(from) || !Number.isFinite(to)) return range

  if (to < from) [from, to] = [to, from]

  if (to - from < minSpan) {
    const center = (from + to) / 2
    from = center - minSpan / 2
    to = center + minSpan / 2
  }

  if (from < 0) {
    to -= from
    from = 0
  }
  if (to > last) {
    from -= to - last
    to = last
  }

  from = Math.max(0, from)
  to = Math.min(last, to)

  if (to - from < minSpan) {
    if (from <= 0) to = Math.min(last, minSpan)
    if (to >= last) from = Math.max(0, last - minSpan)
  }

  return { from, to }
}

/** Set bar spacing so `range` fits the lead chart width, then apply the range. */
export function applyLogicalRangeToLead(
  leadChart,
  range,
  rowCount,
  { rightOffset = DEFAULT_RIGHT_OFFSET, timelineRows = null, forceRequestedRange = false } = {},
) {
  if (!leadChart || !range) {
    return { range, scale: { barSpacing: 7, rightOffset } }
  }

  const clamped = rowCount > 0 ? clampLogicalRange(range, rowCount) : range
  let barSpacing = fitBarSpacingForRange(leadChart, clamped)

  try {
    const ts = leadChart.timeScale()
    const timeRange = logicalRangeToTimeRange(timelineRows, clamped)

    for (let attempt = 0; attempt < 6; attempt += 1) {
      ts.applyOptions({ barSpacing, rightOffset })
      if (timeRange) {
        ts.setVisibleRange(timeRange)
      } else {
        ts.setVisibleLogicalRange(clamped)
      }

      const raw = ts.getVisibleLogicalRange() ?? clamped
      const finalRange = rowCount > 0 ? clampLogicalRange(raw, rowCount) ?? clamped : raw
      const spanOk =
        Math.abs(finalRange.from - clamped.from) < 1.25 &&
        Math.abs(finalRange.to - clamped.to) < 1.25
      if (spanOk) {
        return { range: finalRange, scale: readTimeScale(leadChart) }
      }

      if (forceRequestedRange) {
        return {
          range: clamped,
          scale: { ...readTimeScale(leadChart), barSpacing, rightOffset },
        }
      }

      const nextSpacing = Math.max(0.35, barSpacing * 0.5)
      if (nextSpacing >= barSpacing - 0.01) break
      barSpacing = nextSpacing
    }

    if (forceRequestedRange) {
      return {
        range: clamped,
        scale: { ...readTimeScale(leadChart), barSpacing, rightOffset },
      }
    }

    const raw = leadChart.timeScale().getVisibleLogicalRange() ?? clamped
    const finalRange = rowCount > 0 ? clampLogicalRange(raw, rowCount) ?? clamped : raw
    return { range: finalRange, scale: readTimeScale(leadChart) }
  } catch {
    return { range: clamped, scale: { barSpacing, rightOffset } }
  }
}

/** Snap crosshair to nearest COT week bar time for pixel-perfect alignment. */
export function snapToTimelineTime(time, timelineRows) {
  if (!isNum(time) || !timelineRows?.length) return time
  let best = timelineRows[0].time
  let bestDist = Math.abs(best - time)
  for (let i = 1; i < timelineRows.length; i += 1) {
    const t = timelineRows[i].time
    if (!isNum(t)) continue
    const d = Math.abs(t - time)
    if (d < bestDist) {
      bestDist = d
      best = t
    }
  }
  return best
}
