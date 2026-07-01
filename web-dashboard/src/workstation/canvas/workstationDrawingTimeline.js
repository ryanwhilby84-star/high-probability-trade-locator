/** Shared timeline snap helpers for workstation drawings. */



export function snapTimeToTimeline(timelineRows, time) {

  if (time == null || !timelineRows?.length) return time

  let best = timelineRows[0].time

  let bestDist = Infinity

  for (const row of timelineRows) {

    if (row.time == null) continue

    const dist = Math.abs(row.time - time)

    if (dist < bestDist) {

      bestDist = dist

      best = row.time

    }

  }

  return best

}



export function snapRectTimes(timelineRows, timeStart, timeEnd) {

  let start = snapTimeToTimeline(timelineRows, timeStart)

  let end = snapTimeToTimeline(timelineRows, timeEnd)

  if (start === end && timelineRows.length > 1) {

    const idx = timelineRows.findIndex((r) => r.time === start)

    if (idx >= 0 && idx < timelineRows.length - 1) {

      end = timelineRows[idx + 1].time

    } else if (idx > 0) {

      end = timelineRows[idx - 1].time

    }

  }

  return { timeStart: start, timeEnd: end }

}


