/** Data-coordinate helpers — drawings persist in timeline dates + panel values only. */

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

/** Resolve bar time from a stored week/date label at paint time. */
export function resolveTimelineTime(drawing, dateKey, dateToTime) {
  const date = drawing?.[dateKey]
  if (date && dateToTime) {
    const t = dateToTime(date)
    if (t != null) return t
  }
  // Legacy drawings may still carry ephemeral bar times.
  if (dateKey === 'date' && isNum(drawing?.time)) return drawing.time
  if (dateKey === 'dateStart' && isNum(drawing?.timeStart)) return drawing.timeStart
  if (dateKey === 'dateEnd' && isNum(drawing?.timeEnd)) return drawing.timeEnd
  return null
}

/** Strip screen/ephemeral fields before localStorage persistence. */
export function normalizeDrawingForPersist(drawing) {
  if (!drawing) return drawing
  if (drawing.type === 'vline') {
    const { time, ...rest } = drawing
    return rest
  }
  if (drawing.type === 'rect') {
    const { timeStart, timeEnd, ...rest } = drawing
    return rest
  }
  return drawing
}

export function normalizeDrawingsForPersist(drawings) {
  return (drawings || []).map(normalizeDrawingForPersist)
}
