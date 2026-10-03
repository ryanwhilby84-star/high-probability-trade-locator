const DAY = 86400000
const utc = (s) => Date.parse(`${s}T00:00:00Z`)
const iso = (n) => new Date(n).toISOString().slice(0, 10)
export const monthDay = (n) => iso(n).slice(5)

export function prepareSugarArchive(doc) {
  if (doc?.schema_version !== 1 || !Array.isArray(doc.rows)) throw new Error('Invalid sugar archive data.')
  const rows = doc.rows.map(([date, previous, value, eligible, contract]) => ({ date, previous, value, eligible: eligible === 1, contract, time: utc(date) }))
  let last = ''
  for (const r of rows) {
    if (!Number.isFinite(r.time) || !Number.isFinite(r.value) || r.value <= -1 || r.date <= last || r.previous >= r.date) throw new Error('Invalid sugar return series.')
    last = r.date
  }
  // First session has no return but remains a valid entry close.
  const sessions = [...new Set([doc.first_date, ...rows.map((r) => r.date)])].sort()
  return { doc, rows, sessions, byDate: new Map(rows.map((r) => [r.date, r])) }
}

function firstSession(sessions, date) {
  let lo = 0; let hi = sessions.length
  while (lo < hi) { const mid = (lo + hi) >> 1; if (sessions[mid] < date) lo = mid + 1; else hi = mid }
  const actual = sessions[lo]
  return actual && utc(actual) - utc(date) <= 7 * DAY ? actual : null
}
const median = (a) => { if (!a.length) return null; const s = [...a].sort((x, y) => x - y); const m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2 }

export function summarizeSugar(outcomes, direction = 'long') {
  const sign = direction === 'short' ? -1 : 1
  const values = outcomes.map((o) => o.returnPct * sign)
  const hits = values.filter((r) => r > 0).length
  return { n: values.length, hits, hitRate: values.length ? hits / values.length * 100 : null,
    median: median(values), mean: values.length ? values.reduce((a, b) => a + b, 0) / values.length : null,
    worst: values.length ? Math.min(...values) : null }
}

export function evaluateSugarWindow(archive, start, end, { strict = true, direction = 'long' } = {}) {
  const base = 2001 // Non-leap reference calendar, used only for month/day selection.
  const startTime = utc(`${base}-${start}`)
  const endTime = utc(`${base + (end <= start ? 1 : 0)}-${end}`)
  if (!Number.isFinite(startTime) || !Number.isFinite(endTime) || start === end || endTime - startTime > 180 * DAY) return { error: 'Choose a window of 1–180 calendar days.' }
  const outcomes = []; const exclusions = []
  for (const year of archive.doc.full_years) {
    const entryRequested = `${year}-${start}`
    const exitRequested = `${year + (end <= start ? 1 : 0)}-${end}`
    // Never use partial 2002 in a window advertised as full-year archive research.
    if (exitRequested > `${archive.doc.full_years.at(-1)}-12-31`) { exclusions.push({ year, reason: 'partial_final_year' }); continue }
    const entry = firstSession(archive.sessions, entryRequested); const exit = firstSession(archive.sessions, exitRequested)
    if (!entry || !exit || entry >= exit) { exclusions.push({ year, reason: 'missing_boundary' }); continue }
    const after = (date) => {
      let lo = 0; let hi = archive.rows.length
      while (lo < hi) { const mid = (lo + hi) >> 1; if (archive.rows[mid].date <= date) lo = mid + 1; else hi = mid }
      return lo
    }
    const rows = archive.rows.slice(after(entry), after(exit))
    let previous = entry; let product = 1; let low = 0; let high = 0; let flagged = 0; let missing = false
    for (const r of rows) {
      if (r.previous !== previous) missing = true
      product *= 1 + r.value; previous = r.date
      high = Math.max(high, (product - 1) * 100); low = Math.min(low, (product - 1) * 100)
      if (!r.eligible) flagged += 1
    }
    if (!rows.length || missing || previous !== exit) { exclusions.push({ year, reason: 'missing_interval' }); continue }
    const outcome = { year, entry, exit, returnPct: (product - 1) * 100, minPathPct: low, maxPathPct: high, flagged }
    outcomes.push(outcome)
  }
  const clean = outcomes.filter((o) => !o.flagged)
  const accepted = strict ? clean : outcomes
  return { start, end, calendarDays: (endTime - startTime) / DAY, outcomes: accepted,
    exclusions: [...exclusions, ...(strict ? outcomes.filter((o) => o.flagged).map((o) => ({ year: o.year, reason: 'quality_flag', flagged: o.flagged })) : [])],
    all: summarizeSugar(outcomes, direction), filtered: summarizeSugar(clean, direction), ...summarizeSugar(accepted, direction) }
}

export function findSugarWindows(archive, start, options = {}) {
  const anchor = utc(`2001-${start}`); const results = []
  for (let offset = 0; offset <= 56; offset += 7) {
    const entry = anchor + offset * DAY
    for (const days of [7, 14, 21, 28, 42, 56, 84]) {
      const row = evaluateSugarWindow(archive, monthDay(entry), monthDay(entry + days * DAY), options)
      if (!row.error && row.n >= 15) results.push(row)
    }
  }
  return results.sort((a, b) => b.hitRate - a.hitRate || b.median - a.median || b.n - a.n)
}
