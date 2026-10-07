import { evaluateSugarWindow, monthDay } from './sugarArchiveWindows.js'
import { directionalStats, selectDistinctWindows, validationPass } from './seasonalWindowSelection.js'
const DAY = 86400000
export const archiveDateLabel = (md) => new Date(`2001-${md}T00:00:00Z`).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', timeZone: 'UTC' })

// Rank on the earlier two-thirds only. Later years never choose direction,
// dates, ranking or clusters; retain failed validation rather than hiding it.
export function buildSugarArchiveEdges(archive, asof = new Date(), strict = true) {
  const day = asof.getMonth() === 1 ? Math.min(asof.getDate(), 28) : asof.getDate()
  const anchor = Date.UTC(2001, asof.getMonth(), day)
  const years = [...archive.doc.full_years].sort((a, b) => a - b)
  const cut = Math.floor(years.length * 2 / 3)
  const discoveryYears = years.slice(0, cut)
  const validationYears = years.slice(cut)
  const discovery = { ...archive, doc: { ...archive.doc, full_years: discoveryYears } }
  const candidates = []
  for (let offset = 0; offset <= 28; offset++) {
    for (let days = 7; days <= 90; days++) {
      const start = monthDay(anchor + offset * DAY)
      const end = monthDay(anchor + (offset + days) * DAY)
      const result = evaluateSugarWindow(discovery, start, end, { strict })
      if (result.error || result.n < 12) continue
      const bullish = result.outcomes.filter(r => r.returnPct > 0).length
      const bearish = result.outcomes.filter(r => r.returnPct < 0).length
      const direction = bullish > bearish ? 'Bullish' : bearish > bullish ? 'Bearish' : 'Mixed'
      const stats = directionalStats(result.outcomes, direction)
      const sign = direction === 'Bullish' ? 1 : -1
      if (direction === 'Mixed' || stats.frequency <= .6 || stats.mean * sign <= 0 || stats.median * sign <= 0) continue
      candidates.push({ instrument_id: 'Sugar', window: `${archiveDateLabel(start)} → ${archiveDateLabel(end)}`, start_md: start, end_md: end, kind: 'archive_window', days_until_start: offset, end_offset: offset + days, calendar_days: days, direction, discovery: stats })
    }
  }
  candidates.sort((a, b) => b.discovery.frequency - a.discovery.frequency || Math.abs(b.discovery.median) - Math.abs(a.discovery.median) || b.discovery.n - a.discovery.n || a.days_until_start - b.days_until_start || a.calendar_days - b.calendar_days)
  const selected = selectDistinctWindows(candidates)
  const enriched = selected.map(edge => {
    const result = evaluateSugarWindow(archive, edge.start_md, edge.end_md, { strict })
    const validation = directionalStats(result.outcomes.filter(row => validationYears.includes(row.year)), edge.direction)
    const allResult = strict ? evaluateSugarWindow(archive, edge.start_md, edge.end_md, { strict: false }) : result
    const all = directionalStats(allResult.outcomes, edge.direction)
    const full = directionalStats(result.outcomes, edge.direction)
    const bullish = result.outcomes.filter(row => row.returnPct > 0).length
    const bearish = result.outcomes.filter(row => row.returnPct < 0).length
    const passes = validationPass(validation, edge.direction)
    return { ...edge, grade: passes ? 'VALIDATED' : 'UNCONFIRMED', validation: { ...validation, passed: passes, first_year: validationYears[0], last_year: validationYears.at(-1) }, discovery: { ...edge.discovery, first_year: discoveryYears[0], last_year: discoveryYears.at(-1) }, total_years: years.length, exclusions: result.exclusions, all_observations: all, quality_sensitive: strict && (Math.abs((all.frequency || 0) - (full.frequency || 0)) >= .1 || (all.mean || 0) * (edge.direction === 'Bullish' ? 1 : -1) <= 0), lookbacks: { ARCHIVE: { n: result.n, bullish, bearish, directional_frequency: full.frequency, mean_pct: full.mean, median_pct: full.median, returns: result.outcomes.map(row => ({ year: row.year, entry: row.entry, exit: row.exit, return_pct: row.returnPct, flagged: row.flagged })) } } }
  })
  return enriched.map(edge => ({ ...edge, opposing_windows: edge.opposing_windows.map(other => {
    const counterpart = enriched.find(row => row.window === other.window && row.direction === other.direction)
    const otherYears = new Map(counterpart.lookbacks.ARCHIVE.returns.map(row => [row.year, row.return_pct]))
    const common = edge.lookbacks.ARCHIVE.returns.filter(row => otherYears.has(row.year))
    const own = directionalStats(common.map(row => ({ returnPct: row.return_pct })), edge.direction)
    const opposite = directionalStats(common.map(row => ({ returnPct: otherYears.get(row.year) })), other.direction)
    return { ...other, common_years: common.length, own_frequency: own.frequency, other_frequency: opposite.frequency, other_validated: counterpart.validation.passed }
  }) }))
}
