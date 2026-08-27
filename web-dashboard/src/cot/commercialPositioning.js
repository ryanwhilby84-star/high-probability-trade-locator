/**
 * Commercial positioning — Legacy COT commercials group.
 * Source: legacy_cot_latest.json → instruments[id].groups.commercials.weeks
 */

import {
  extremeZoneLabel,
  isExtremePercentile,
  percentileRank,
} from '../charts/chartAnalytics.js'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function unavailableReason(instrumentData) {
  if (!instrumentData) {
    return 'No Legacy COT export for this instrument — run python -m hptl.cot.run_legacy_cot'
  }
  const weeks = instrumentData?.groups?.commercials?.weeks
  if (!Array.isArray(weeks) || !weeks.length) {
    return 'Legacy COT export has no commercials group weeks for this instrument.'
  }
  const latest = weeks[weeks.length - 1]
  if (!isNum(latest?.net) && !isNum(latest?.long) && !isNum(latest?.short)) {
    return 'Commercial cohort fields are empty in the latest Legacy COT week.'
  }
  return 'Commercial data unavailable.'
}

/** Chronological commercial weeks from Legacy COT instrument block. */
export function commercialWeeksFromLegacy(instrumentData) {
  const weeks = instrumentData?.groups?.commercials?.weeks
  if (!Array.isArray(weeks) || !weeks.length) return []
  return [...weeks].sort((a, b) =>
    String(a.report_date || '').localeCompare(String(b.report_date || '')),
  )
}

/**
 * Raw table rows for Commercial Positioning page.
 * @param {object|null} instrumentData
 * @returns {{ available: boolean, reason: string|null, rows: object[] }}
 */
export function buildCommercialTableRows(instrumentData) {
  const sorted = commercialWeeksFromLegacy(instrumentData)
  if (!sorted.length) {
    return { available: false, reason: unavailableReason(instrumentData), rows: [] }
  }

  const nets = []
  const rows = sorted.map((w, i) => {
    const commercial_net = isNum(w.net) ? w.net : null
    const historyNets = sorted.slice(0, i + 1).map((x) => x.net).filter(isNum)
    const percentile = percentileRank(historyNets, commercial_net)
    const extreme_flag = isExtremePercentile(percentile)
      ? extremeZoneLabel(percentile) || true
      : false

    nets.push(commercial_net)

    return {
      report_date: String(w.report_date || '').slice(0, 10),
      commercial_long: isNum(w.long) ? w.long : null,
      commercial_short: isNum(w.short) ? w.short : null,
      commercial_net,
      weekly_change_net: isNum(w.net_week_change) ? w.net_week_change : null,
      percentile,
      extreme_flag,
    }
  })

  const hasData = rows.some((r) => r.commercial_net != null)
  if (!hasData) {
    return { available: false, reason: unavailableReason(instrumentData), rows: [] }
  }

  return { available: true, reason: null, rows }
}

/**
 * @param {object|null} instrumentData
 * @returns {{ available: boolean, reason: string|null }}
 */
export function commercialAvailability(instrumentData) {
  const result = buildCommercialTableRows(instrumentData)
  return { available: result.available, reason: result.reason }
}
