import { evaluateSugarWindow, monthDay } from './sugarArchiveWindows.js'
const DAY = 86400000
export const archiveDateLabel = (md) => new Date(`2001-${md}T00:00:00Z`).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', timeZone: 'UTC' })

// Use the existing finder's daily entry grid and 7–90 day durations.
// Archive grades are deliberately separate from recent 5Y/10Y/15Y robustness grades.
export function buildSugarArchiveEdges(archive, asof = new Date(), strict = true) {
  const anchor = Date.UTC(2001, asof.getMonth(), asof.getDate())
  const candidates = []
  for (let offset = 0; offset <= 28; offset++) {
    for (let days = 7; days <= 90; days++) {
      const start = monthDay(anchor + offset * DAY)
      const end = monthDay(anchor + (offset + days) * DAY)
      const result = evaluateSugarWindow(archive, start, end, { strict })
      if (result.error || result.n < 15) continue
      const bullish = result.outcomes.filter(r => r.returnPct > 0).length
      const bearish = result.outcomes.filter(r => r.returnPct < 0).length
      const direction = bullish > bearish ? 'Bullish' : bearish > bullish ? 'Bearish' : 'Mixed'
      const frequency = Math.max(bullish, bearish) / result.n
      if (frequency <= .6 || !(direction === 'Bullish' ? result.mean > 0 && result.median > 0 : direction === 'Bearish' && result.mean < 0 && result.median < 0)) continue
      candidates.push({ instrument_id: 'Sugar', window: `${archiveDateLabel(start)} → ${archiveDateLabel(end)}`, kind: 'archive_window', days_until_start: offset, direction, grade: 'ARCHIVE', exclusions: result.exclusions, lookbacks: { ARCHIVE: { n: result.n, bullish, bearish, directional_frequency: frequency, mean_pct: result.mean, median_pct: result.median, returns: result.outcomes.map(r => ({ year: r.year, return_pct: r.returnPct })) } } })
    }
  }
  candidates.sort((a,b) => b.lookbacks.ARCHIVE.directional_frequency - a.lookbacks.ARCHIVE.directional_frequency || Math.abs(b.lookbacks.ARCHIVE.median_pct) - Math.abs(a.lookbacks.ARCHIVE.median_pct))
  // Keep both directions visible; avoid filling the cards with variants of one entry date.
  const kept = []; const seen = new Set(); const counts = { Bullish: 0, Bearish: 0 }
  for (const edge of candidates) {
    const key = `${edge.direction}-${edge.days_until_start}`
    if (seen.has(key) || counts[edge.direction] >= 4) continue
    seen.add(key); counts[edge.direction]++; kept.push(edge)
  }
  return kept
}
