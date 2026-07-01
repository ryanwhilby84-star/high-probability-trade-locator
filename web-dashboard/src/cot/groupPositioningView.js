import { buildMarketHistoryForMarket } from '../legacy/dashboardLegacy.jsx'
import { canonicalMarketId, isCotRowResolved, normalizeReportDate, recordCotReportDate } from '../marketResolution.js'
import { buildRolling3yContextFromWeeks } from './rolling3yFromLegacyWeeks.js'

export const POSITIONING_SHEET_TABS = [
  {
    id: 'noncommercials',
    label: 'Non-Commercials',
    confluenceKey: 'managed_money',
    chartGroupId: 'managed_money',
    legacyGroupId: 'noncommercials',
  },
  {
    id: 'commercials',
    label: 'Commercials',
    confluenceKey: 'commercial',
    chartGroupId: 'commercial',
    legacyGroupId: 'commercials',
  },
  {
    id: 'nonreportables',
    label: 'Non-Reportables',
    confluenceKey: 'nonreportable',
    chartGroupId: 'nonreportable',
    legacyGroupId: 'nonreportables',
  },
]

const rowDate = (r) =>
  normalizeReportDate(recordCotReportDate(r) || r?.latest_report_date || r?.date || '')

function num(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

function mapRecordToGroupRow(record, confluenceKey) {
  const groups = record?.cot_positioning_groups
  const block = groups?.[confluenceKey]
  if (!block?.available && block?.long == null && block?.short == null) return null

  const long = num(block?.long)
  const short = num(block?.short)
  const net = num(block?.net) ?? (long != null && short != null ? long - short : null)
  if (long == null && short == null && net == null) return null

  return {
    ...record,
    long_value: long,
    short_value: short,
    net_value: net,
    open_interest: num(groups?.open_interest),
    one_week_long_change: null,
    one_week_short_change: null,
    one_week_net_change: null,
    positioning_state: confluenceKey === 'managed_money' ? record.positioning_state : null,
    cot_positioning_groups: groups,
  }
}

/** Chronological weekly rows for one COT cohort (newest capped by asOfDate). */
export function buildGroupPositioningHistory(allRows, market, groupId, asOfDate, maxWeeks = 52) {
  const tab = POSITIONING_SHEET_TABS.find((t) => t.id === groupId)
  if (!tab) return []

  if (groupId === 'noncommercials') {
    return buildMarketHistoryForMarket(allRows, market, asOfDate, maxWeeks)
  }

  const id = canonicalMarketId(market)
  const asOf = normalizeReportDate(asOfDate)
  const byCot = new Map()

  for (const record of allRows || []) {
    if (canonicalMarketId(record.market || record.raw_cftc_market_name) !== id) continue
    if (!isCotRowResolved(record) && record.long_value == null) continue
    const cotKey = recordCotReportDate(record) || rowDate(record)
    if (!cotKey || rowDate(record) > asOf) continue
    const mapped = mapRecordToGroupRow(record, tab.confluenceKey)
    if (!mapped) continue
    const prev = byCot.get(cotKey)
    if (!prev || rowDate(record).localeCompare(rowDate(prev)) >= 0) {
      byCot.set(cotKey, mapped)
    }
  }

  return [...byCot.values()]
    .sort((a, b) => rowDate(a).localeCompare(rowDate(b)))
    .slice(-maxWeeks)
}

export function currentGroupSnapshotRow(row, groupId) {
  const tab = POSITIONING_SHEET_TABS.find((t) => t.id === groupId)
  if (!tab || !row) return row
  if (groupId === 'noncommercials') return row
  return mapRecordToGroupRow(row, tab.confluenceKey) || row
}

export function rolling3yContextForGroup({
  groupId,
  headlineRow,
  legacyInstrument,
}) {
  if (groupId === 'noncommercials') {
    return {
      ctx: headlineRow?.rolling_3y_history_context || null,
      multiyear: headlineRow?.institutional_context?.multiyear_positioning || null,
    }
  }

  const tab = POSITIONING_SHEET_TABS.find((t) => t.id === groupId)
  const weeks = legacyInstrument?.groups?.[tab?.legacyGroupId]?.weeks
  if (!Array.isArray(weeks) || !weeks.length) {
    return { ctx: null, multiyear: null }
  }

  const sorted = [...weeks].sort((a, b) =>
    String(a.report_date || '').localeCompare(String(b.report_date || '')),
  )
  return { ctx: buildRolling3yContextFromWeeks(sorted), multiyear: null }
}
