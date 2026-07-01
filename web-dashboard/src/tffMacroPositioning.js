/**
 * CFTC TFF (Traders in Financial Futures) — leveraged-money positioning.
 * Single source of truth: /data/tff_macro_positioning_latest.json
 */

/** Instrument IDs with TFF leveraged-money series in the macro export. */
export const TFF_MACRO_INSTRUMENT_IDS = [
  'US Dollar Index / DX',
  'US 2-Year T-Note / ZT',
  'US 5-Year T-Note / ZF',
  'US 10-Year T-Note / ZN',
  'Ultra 10-Year T-Note / TN',
  'US 30-Year T-Bond / ZB',
]

const TFF_SET = new Set(TFF_MACRO_INSTRUMENT_IDS)

export function isTffMacroInstrument(marketId) {
  return TFF_SET.has(String(marketId || '').trim())
}

export function tffInstrumentBlock(doc, marketId) {
  if (!doc || !marketId) return null
  return (doc.instruments || []).find((x) => x.instrument_id === marketId) || null
}

function finiteNum(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

function strictPos(v) {
  const n = finiteNum(v)
  return n !== null && n > 0
}

function strictNeg(v) {
  const n = finiteNum(v)
  return n !== null && n < 0
}

/** Mirror backend _compute_positioning_state (simplified). */
export function computePositioningState(net, oneWeekNet, fourWeekNet, longWeekly, shortWeekly) {
  const n = finiteNum(net)
  if (n === null) return 'N/A'
  if (n === 0) return 'Neutral'

  if (n < 0 && strictPos(longWeekly) && strictNeg(shortWeekly)) return 'Accumulation'
  if (n > 0 && strictNeg(longWeekly) && strictPos(shortWeekly)) return 'Distribution'

  const w1 = finiteNum(oneWeekNet)
  const w4 = finiteNum(fourWeekNet)

  if (n < 0 && strictNeg(w1) && strictNeg(w4)) return 'Bearish Strengthening'
  if (n < 0 && strictPos(w1) && strictPos(w4)) return 'Short Covering'
  if (n < 0 && (strictPos(w1) || strictPos(w4))) return 'Bearish Improving'
  if (n > 0 && strictPos(w1) && strictPos(w4)) return 'Bullish Strengthening'
  if (n > 0 && strictNeg(w1) && strictNeg(w4)) return 'Bullish Weakening'
  if (n > 0 && (strictNeg(w1) || strictNeg(w4))) return 'Bullish Softening'

  const bits = []
  if (w1 !== null) bits.push(`1w net ${w1 > 0 ? 'up' : w1 < 0 ? 'down' : 'flat'}`)
  if (w4 !== null) bits.push(`4w net ${w4 > 0 ? 'up' : w4 < 0 ? 'down' : 'flat'}`)
  if (bits.length) return bits.join('; ')
  return n > 0 ? 'Net long' : 'Net short'
}

/** Directional bias from net (macro futures: long = bullish on the contract). */
export function cotBiasFromNet(net) {
  const n = finiteNum(net)
  if (n === null) return 'N/A'
  if (n > 500) return 'Bullish'
  if (n < -500) return 'Bearish'
  return 'Neutral'
}

function pctLongNumber(pos) {
  const l = finiteNum(pos?.long)
  const oi = finiteNum(pos?.open_interest)
  if (pos?.pct_long != null && finiteNum(pos.pct_long) <= 100) return finiteNum(pos.pct_long)
  if (l !== null && oi !== null && oi > 0) return (l / oi) * 100
  return null
}

function pctShortNumber(pos) {
  const s = finiteNum(pos?.short)
  const oi = finiteNum(pos?.open_interest)
  if (pos?.pct_short != null && finiteNum(pos.pct_short) <= 100) return finiteNum(pos.pct_short)
  if (s !== null && oi !== null && oi > 0) return (s / oi) * 100
  return null
}

function historyStatsFromWeeks(weeks) {
  const longs = weeks.map((w) => finiteNum(w.long)).filter((x) => x !== null)
  const shorts = weeks.map((w) => finiteNum(w.short)).filter((x) => x !== null)
  const nets = weeks.map((w) => finiteNum(w.net)).filter((x) => x !== null)
  const stat = (arr) =>
    arr.length
      ? { max: Math.max(...arr), min: Math.min(...arr) }
      : { max: null, min: null }
  return {
    long: stat(longs),
    short: stat(shorts),
    net: stat(nets),
    rows_used: weeks.length,
  }
}

/** Map one TFF week to a confluence-compatible row. */
export function confluenceRowFromTffWeek(pos, marketId, { weeks = [] } = {}) {
  if (!pos || !marketId) return null
  const net = finiteNum(pos.net)
  const w1 = finiteNum(pos.one_week_net_change)
  const w4 = finiteNum(pos.four_week_net_change)
  const reportDate = String(pos.date || '').slice(0, 10)
  const stats = historyStatsFromWeeks(weeks.length ? weeks : [pos])
  const pctNet = finiteNum(pos.net_percentile_13w)

  const flowSummary =
    net !== null
      ? `TFF Leveraged Money net ${net.toLocaleString()} (${reportDate || '—'}). ` +
        `Source: CFTC fut_fin_txt · trader group: leveraged money.`
      : 'TFF positioning row missing net.'

  return {
    market: marketId,
    market_key: marketId,
    date: reportDate,
    cot_report_date: reportDate,
    latest_report_date: reportDate,
    raw_cftc_market_name: pos.market_name || marketId,
    long_value: finiteNum(pos.long),
    short_value: finiteNum(pos.short),
    net_value: net,
    open_interest: finiteNum(pos.open_interest),
    percent_long: pctLongNumber(pos),
    percent_short: pctShortNumber(pos),
    one_week_net_change: w1,
    four_week_net_change: w4,
    positioning_state: computePositioningState(net, w1, w4, null, null),
    cot_bias: cotBiasFromNet(net),
    cot_score: pctNet !== null ? Math.round(pctNet - 50) : null,
    positioning_status: 'tff_positioning',
    positioning_source: 'tff_leveraged_money',
    cot_status: 'tff_mapped',
    cot_status_label: 'TFF Leveraged Money (mapped)',
    trader_group_used: 'Leveraged Money (TFF)',
    position_source_family: 'tff_leveraged_money',
    institutional_flow_summary: flowSummary,
    positioning_interpretation: flowSummary,
    four_week_positioning_story: flowSummary,
    next_data_watch: 'Monitor weekly TFF leveraged-money positioning (CFTC fut_fin_txt).',
    setup_type: 'TFF macro positioning',
    invalidation_note: 'TFF net flips sign or 13-week percentile reaches crowded extremes.',
    tff_positioning: true,
    tff_cftc_code: pos.cftc_code || null,
    current_net_percentile: pctNet,
    current_net_rank_label:
      pctNet !== null
        ? pctNet >= 75
          ? 'High'
          : pctNet <= 25
            ? 'Low'
            : 'Mid'
        : null,
    all_time_net_max: stats.net.max,
    all_time_net_min: stats.net.min,
    all_time_long_max: stats.long.max,
    all_time_long_min: stats.long.min,
    all_time_short_max: stats.short.max,
    all_time_short_min: stats.short.min,
    historical_percentile_n_joint: stats.rows_used,
    historical_series_report_date: reportDate,
    _data_source: 'tff_macro_positioning_latest.json',
  }
}

/** Latest TFF row for an instrument. */
export function latestTffRow(doc, marketId) {
  const block = tffInstrumentBlock(doc, marketId)
  if (!block?.available || !block.positioning) return null
  const weeks = Array.isArray(block.weeks) ? block.weeks : []
  return confluenceRowFromTffWeek(block.positioning, marketId, { weeks })
}

/** Chronological history rows from TFF weeks (for instrument detail table). */
export function tffHistoryRows(doc, marketId, maxWeeks = 13) {
  const block = tffInstrumentBlock(doc, marketId)
  if (!block?.available) return []
  const weeks = Array.isArray(block.weeks) ? block.weeks : []
  const sorted = [...weeks].sort((a, b) => String(a.date).localeCompare(String(b.date)))
  const slice = sorted.slice(-maxWeeks)
  return slice.map((w) => confluenceRowFromTffWeek(w, marketId, { weeks: slice }))
}

/** Overlay TFF data onto a confluence row when export is available. */
export function mergeRowWithTff(confluenceRow, tffDoc, marketId) {
  if (!isTffMacroInstrument(marketId)) return confluenceRow
  const tffRow = latestTffRow(tffDoc, marketId)
  if (!tffRow) return confluenceRow
  return {
    ...(confluenceRow || {}),
    ...tffRow,
    market: marketId,
    market_key: marketId,
    _tff_overlay: true,
    instrument_meta: {
      ...(confluenceRow?.instrument_meta || {}),
      positioning_status: 'tff_positioning',
      cot_report_type: 'financial_futures_tff',
      has_cot_mapping: true,
    },
  }
}

/** Macro Hub / CotBlock-compatible shape. */
export function tffCotBlockFromDoc(doc, marketId) {
  const block = tffInstrumentBlock(doc, marketId)
  const pos = block?.positioning
  if (!pos) return null
  return {
    long: pos.long,
    short: pos.short,
    net: pos.net,
    weekly_net_change: pos.one_week_net_change,
    four_week_net_change: pos.four_week_net_change,
    open_interest: pos.open_interest,
    net_percentile_3y: pos.net_percentile_13w,
    pct_long: pos.pct_long,
    pct_short: pos.pct_short,
    report_date: pos.date,
    source: 'CFTC TFF · Leveraged Money',
    trader_group: 'leveraged_money',
    error: block.available ? null : block?.error || 'no_tff_rows',
  }
}

export function isTffPositioningResolved(row) {
  if (!row || typeof row !== 'object') return false
  if (row.positioning_source === 'tff_leveraged_money' && finiteNum(row.net_value) !== null) return true
  if (row.tff_positioning && finiteNum(row.net_value) !== null) return true
  return false
}
