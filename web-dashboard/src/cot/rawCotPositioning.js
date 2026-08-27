/**
 * Raw COT positioning rows from legacy_cot_latest.json only.
 * Source: instruments[id].groups.{noncommercials|commercials|nonreportables}.weeks
 */

import { normalizeReportDate } from '../marketResolution.js'
import { POSITIONING_SHEET_TABS } from './groupPositioningView.js'

function num(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

function pctLong(long, short) {
  const L = num(long)
  const S = num(short)
  if (L == null || S == null || L + S <= 0) return null
  return (100 * L) / (L + S)
}

export function mapLegacyWeekToRawRow(week) {
  const long = num(week?.long)
  const short = num(week?.short)
  const net = num(week?.net) ?? (long != null && short != null ? long - short : null)
  const reportDate = normalizeReportDate(week?.report_date)
  if (!reportDate) return null

  return {
    report_date: reportDate,
    long,
    short,
    net,
    open_interest: num(week?.open_interest),
    percent_long: num(week?.percent_long) ?? pctLong(long, short),
    percent_short: num(week?.percent_short) ?? (pctLong(long, short) != null ? 100 - pctLong(long, short) : null),
    weekly_change_long: num(week?.long_week_change),
    weekly_change_short: num(week?.short_week_change),
    weekly_change_net: num(week?.net_week_change),
    positioning_state: null,
  }
}

/** Full chronological archive for one legacy group, capped by as-of date. */
export function buildLegacyGroupRawRows(legacyInstrument, legacyGroupId, asOfDate = null) {
  const weeks = legacyInstrument?.groups?.[legacyGroupId]?.weeks
  if (!Array.isArray(weeks) || !weeks.length) return []

  const asOf = normalizeReportDate(asOfDate)
  return weeks
    .map(mapLegacyWeekToRawRow)
    .filter(Boolean)
    .filter((row) => !asOf || row.report_date <= asOf)
    .sort((a, b) => a.report_date.localeCompare(b.report_date))
}

export function buildRawRowsForGroup(legacyInstrument, groupId, asOfDate = null) {
  const tab = POSITIONING_SHEET_TABS.find((t) => t.id === groupId)
  if (!tab || !legacyInstrument) return []
  return buildLegacyGroupRawRows(legacyInstrument, tab.legacyGroupId, asOfDate)
}

/** Map legacy weeks to confluence-shaped rows for chart series builder. */
export function buildLegacyGroupChartHistory(legacyInstrument, groupId, asOfDate = null) {
  const tab = POSITIONING_SHEET_TABS.find((t) => t.id === groupId)
  if (!tab || !legacyInstrument) return []

  const market = legacyInstrument.instrument_id || null
  const confluenceKey = tab.confluenceKey
  const rows = buildLegacyGroupRawRows(legacyInstrument, tab.legacyGroupId, asOfDate)

  return rows.map((r) => ({
    market,
    latest_report_date: r.report_date,
    long_value: r.long,
    short_value: r.short,
    net_value: r.net,
    positioning_state: groupId === 'noncommercials' ? null : null,
    cot_positioning_groups: {
      open_interest: r.open_interest,
      [confluenceKey]: {
        available: true,
        long: r.long,
        short: r.short,
        net: r.net,
        pct_long: r.percent_long,
        pct_short: r.percent_short,
      },
    },
  }))
}
