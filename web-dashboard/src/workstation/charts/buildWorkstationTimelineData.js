import { barTimeToDate } from '../../charts/positioningTimelineAlign.js'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

/** Line points for a COT panel from aligned workstation rows. */
export function rowsToLinePoints(rows, valueKey) {
  return (rows || [])
    .filter((r) => isNum(r.time) && isNum(r[valueKey]))
    .map((r) => ({ time: r.time, value: r[valueKey], date: r.date }))
}

/** ISO date label from unix bar time using timeline rows. */
export function labelFromTimelineTime(timelineRows, time) {
  if (!isNum(time) || !timelineRows?.length) return barTimeToDate(time)
  const hit = timelineRows.find((r) => r.time === time)
  return hit?.label || hit?.date || barTimeToDate(time)
}
