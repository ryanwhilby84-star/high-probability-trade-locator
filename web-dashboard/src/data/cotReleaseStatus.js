// Official 2026 CFTC COT publication dates, including US holiday delays.
// https://www.cftc.gov/MarketReports/CommitmentsofTraders/ReleaseSchedule/index.htm
// This is a display warning only. Never disable a valid historical snapshot.
// Refresh this calendar when CFTC changes cadence (semiweekly planned for late 2026).
export const CFTC_RELEASE_DATES_2026 = Object.freeze([
  '2026-01-05','2026-01-09','2026-01-16','2026-01-23','2026-01-30',
  '2026-02-06','2026-02-13','2026-02-20','2026-02-27',
  '2026-03-06','2026-03-13','2026-03-20','2026-03-27',
  '2026-04-03','2026-04-10','2026-04-17','2026-04-24',
  '2026-05-01','2026-05-08','2026-05-15','2026-05-22','2026-05-29',
  '2026-06-05','2026-06-12','2026-06-22','2026-06-26',
  '2026-07-06','2026-07-10','2026-07-17','2026-07-24','2026-07-31',
  '2026-08-07','2026-08-14','2026-08-21','2026-08-28',
  '2026-09-04','2026-09-11','2026-09-18','2026-09-25',
  '2026-10-02','2026-10-09','2026-10-16','2026-10-23','2026-10-30',
  '2026-11-06','2026-11-16','2026-11-20','2026-11-30',
  '2026-12-04','2026-12-11','2026-12-18','2026-12-28',
])

function easternClock(now) {
  const parts = Object.fromEntries(new Intl.DateTimeFormat('en-GB', {
    timeZone: 'America/New_York',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
  }).formatToParts(now).filter(p => p.type !== 'literal').map(p => [p.type, p.value]))
  return parts.year + '-' + parts.month + '-' + parts.day + 'T' + parts.hour + ':' + parts.minute
}

function reportTuesday(releaseDate) {
  const day = new Date(releaseDate + 'T00:00:00Z')
  const dow = day.getUTCDay()
  if (dow !== 1 && dow !== 5) return null
  day.setUTCDate(day.getUTCDate() - (dow === 1 ? 6 : 3))
  return day.toISOString().slice(0, 10)
}

export function cotReleaseStatus(latestDate, now = new Date()) {
  if (!/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/.test(latestDate || '') || !Number.isFinite(now.getTime())) {
    return { needsRefresh: false, expectedReportDate: null, releaseDate: null }
  }
  const nowEastern = easternClock(now)
  const releaseDate = CFTC_RELEASE_DATES_2026.filter(date => date + 'T15:30' <= nowEastern).at(-1) || null
  const expectedReportDate = releaseDate ? reportTuesday(releaseDate) : null
  return {
    needsRefresh: Boolean(expectedReportDate && latestDate < expectedReportDate),
    expectedReportDate,
    releaseDate,
  }
}
