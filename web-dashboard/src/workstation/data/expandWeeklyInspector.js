/**
 * Expand compact cot_weekly_inspector_latest.json rows into the shape
 * expected by buildWeeklyViewModel (mirrors Python expand_compact_market).
 */

const MEASURE = 'net_positioning_expanding_percentile'
const MEASURE_LABEL = 'Net positioning percentile (expanding, point-in-time)'

const DIR_FROM = {
  0: 'strongly_increasing',
  1: 'increasing',
  2: 'stable',
  3: 'decreasing',
  4: 'strongly_decreasing',
  5: 'unknown',
}

const TEMP_FROM = {
  0: 'heating_rapidly',
  1: 'heating',
  2: 'cooling_from_extreme',
  3: 'deepening_extreme',
  4: 'recovering',
  5: 'building',
  6: 'weakening',
  7: 'elevated_stable',
  8: 'depressed_stable',
  9: 'neutral',
  10: 'unknown',
  11: 'recovering_strong',
}

const REL_FROM = {
  0: 'aligned',
  1: 'opposed',
  2: 'strong_opposition',
  3: 'mixed',
  4: 'unavailable',
}

const FLOW_FROM = {
  0: 'opposition_widening_rapidly',
  1: 'opposition_narrowing_rapidly',
  2: 'opposition_widening',
  3: 'opposition_narrowing',
  4: 'spread_widening',
  5: 'spread_narrowing',
  6: 'stable',
  7: 'unavailable',
}

const ARROW = {
  strongly_increasing: '▲▲',
  increasing: '▲',
  stable: '→',
  decreasing: '▼',
  strongly_decreasing: '▼▼',
  unknown: '·',
}

export const STATE_FROM_TEMP = {
  heating_rapidly: 'Deeper into extreme',
  heating: 'Deeper into extreme',
  cooling_from_extreme: 'Cooling from extreme',
  deepening_extreme: 'Deeper into low extreme',
  recovering: 'Moving out of extreme',
  recovering_strong: 'Strong rotation away from extreme',
  building: 'Rotation strengthening',
  weakening: 'Rotation weakening',
  elevated_stable: 'Elevated / stable',
  depressed_stable: 'Depressed / stable',
  neutral: 'Neutral',
  unknown: 'Unavailable',
}

/** Prefer temperature→label map so compact rows keep the restored wording. */
export function stateLabelFromTemperature(temperature, fallback = null) {
  if (temperature && STATE_FROM_TEMP[temperature]) return STATE_FROM_TEMP[temperature]
  return fallback || 'Unavailable'
}

function expandGroup(arr) {
  const a = Array.isArray(arr) ? [...arr] : []
  while (a.length < 12) a.push(null)
  const direction = DIR_FROM[a[9]] || 'unknown'
  const temperature = TEMP_FROM[a[10]] || 'unknown'
  return {
    net: a[0],
    weekly_change: a[1],
    four_week_change: a[2],
    twelve_week_change: a[3],
    percentile: a[4],
    percentile_change_1w: a[5],
    percentile_change_4w: a[6],
    percentile_change_12w: a[7],
    percentile_observation_count: a[8],
    direction,
    direction_arrow: ARROW[direction] || '·',
    temperature,
    state_label: STATE_FROM_TEMP[temperature] || 'Unavailable',
    is_extreme: Boolean(a[11]),
    measure: MEASURE,
  }
}

function expandCross(arr) {
  const a = Array.isArray(arr) ? [...arr] : []
  while (a.length < 11) a.push(null)
  return {
    commercial_percentile: a[0],
    noncommercial_percentile: a[1],
    nonreportable_percentile: a[2],
    comm_nc_spread: a[3],
    comm_nc_spread_percentile: a[4],
    comm_nc_spread_change_1w: a[5],
    comm_nc_spread_change_4w: a[6],
    comm_nr_spread: a[7],
    comm_nr_spread_percentile: a[8],
    relationship: REL_FROM[a[9]] || 'unavailable',
    flow: FLOW_FROM[a[10]] || 'unavailable',
    measure: MEASURE,
  }
}

function summaryLine(label, g) {
  const pct = g?.percentile
  if (pct == null || !Number.isFinite(Number(pct))) {
    return `${label} net positioning is unavailable for this week.`
  }
  const d4 = g.percentile_change_4w
  const d1 = g.percentile_change_1w
  const move = d4 != null && Number.isFinite(Number(d4)) ? Number(d4) : d1
  const state = g.state_label || 'Neutral'
  if (move == null || !Number.isFinite(Number(move))) {
    return `${label} positioning is at the ${Math.round(pct)}th net percentile. State: ${state}.`
  }
  const m = Number(move)
  const verb = m > 0 ? 'risen' : m < 0 ? 'fallen' : 'been unchanged'
  const horizon = d4 != null && Number.isFinite(Number(d4)) ? 'four weeks' : 'one week'
  return `${label} positioning is at the ${Math.round(pct)}th net percentile and has ${verb} ${Math.abs(m).toFixed(0)} percentile points over ${horizon}. ${state}.`
}

/** Expand one market block from compact export → weekly_inspector shape. */
export function expandWeeklyInspectorMarket(block) {
  if (!block?.available) {
    return {
      available: false,
      measure: MEASURE,
      measure_label: MEASURE_LABEL,
      weeks: [],
      week_count: 0,
    }
  }
  const weeks = []
  for (const row of block.rows || []) {
    if (!row?.length) continue
    const c = expandGroup(row[1])
    const nc = expandGroup(row[2])
    const nr = expandGroup(row[3])
    weeks.push({
      date: row[0],
      commercial: c,
      noncommercial: nc,
      nonreportable: nr,
      cross: expandCross(row[4]),
      summaries: {
        commercial: summaryLine('Commercial', c),
        noncommercial: summaryLine('Non-Commercial', nc),
        nonreportable: summaryLine('Non-Reportable', nr),
      },
    })
  }
  return {
    available: true,
    measure: MEASURE,
    measure_label: MEASURE_LABEL,
    weeks,
    week_count: weeks.length,
  }
}

export function resolveWeeklyInspectorBlock(doc, marketId, matchedKey = null) {
  const markets = doc?.markets || {}
  const block =
    markets[marketId] ||
    (matchedKey ? markets[matchedKey] : null) ||
    Object.entries(markets).find(
      ([k]) => String(k).toLowerCase() === String(marketId || '').toLowerCase(),
    )?.[1] ||
    null
  if (!block) return null
  return expandWeeklyInspectorMarket(block)
}
