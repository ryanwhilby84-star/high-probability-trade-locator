const PCT_CHG_STRONG = 7
const PCT_CHG_MILD = 2
const EXTREME_HIGH = 90
const HIGH = 70
const LOW = 30
const EXTREME_LOW = 10

function finite(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

function percentileRank(values, value) {
  if (value == null || !values.length) return null
  const clean = values.filter((v) => Number.isFinite(v)).slice().sort((a, b) => a - b)
  if (!clean.length) return null
  let below = 0
  let equal = 0
  for (const v of clean) {
    if (v < value) below += 1
    else if (v === value) equal += 1
  }
  return ((below + 0.5 * equal) / clean.length) * 100
}

function expandingPercentiles(values) {
  const hist = []
  return values.map((raw) => {
    const v = finite(raw)
    if (v == null) return null
    hist.push(v)
    return percentileRank(hist, v)
  })
}

function classifyDirection(delta) {
  const d = finite(delta)
  if (d == null) return 'unknown'
  if (d >= PCT_CHG_STRONG) return 'strongly_increasing'
  if (d >= PCT_CHG_MILD) return 'increasing'
  if (d > -PCT_CHG_MILD) return 'stable'
  if (d > -PCT_CHG_STRONG) return 'decreasing'
  return 'strongly_decreasing'
}

function arrow(direction) {
  return {
    strongly_increasing: '▲▲',
    increasing: '▲',
    stable: '→',
    decreasing: '▼',
    strongly_decreasing: '▼▼',
    unknown: '·',
  }[direction] || '·'
}

function classifyTemperature(percentile, d1, d4) {
  const p = finite(percentile)
  const a = finite(d1)
  const b = finite(d4)
  if (p == null) return ['unknown', 'Unavailable']

  let rising = a != null && a >= PCT_CHG_MILD
  let falling = a != null && a <= -PCT_CHG_MILD
  let strongUp = a != null && a >= PCT_CHG_STRONG
  let strongDown = a != null && a <= -PCT_CHG_STRONG

  if (a != null && Math.abs(a) < PCT_CHG_MILD && b != null) {
    if (b >= PCT_CHG_STRONG) { rising = true; strongUp = true }
    else if (b >= PCT_CHG_MILD) rising = true
    else if (b <= -PCT_CHG_STRONG) { falling = true; strongDown = true }
    else if (b <= -PCT_CHG_MILD) falling = true
  }

  if (p >= HIGH) {
    if (strongUp) return ['heating_rapidly', 'Deeper into extreme']
    if (rising) return ['heating', 'Deeper into extreme']
    if (strongDown || falling) return ['cooling_from_extreme', 'Cooling from extreme']
    return ['elevated_stable', 'Elevated / stable']
  }
  if (p <= LOW) {
    if (strongDown || falling) return ['deepening_extreme', 'Deeper into low extreme']
    if (strongUp) return ['recovering_strong', 'Strong rotation away from extreme']
    if (rising) return ['recovering', 'Moving out of extreme']
    return ['depressed_stable', 'Depressed / stable']
  }
  if (strongUp || (b != null && b >= PCT_CHG_STRONG)) return ['building', 'Rotation strengthening']
  if (rising) return ['building', 'Rotation strengthening']
  if (strongDown || (b != null && b <= -PCT_CHG_STRONG)) return ['weakening', 'Rotation weakening']
  if (falling) return ['weakening', 'Rotation weakening']
  return ['neutral', 'Neutral']
}

function diff(values, i, lag) {
  if (i < lag) return null
  const a = finite(values[i])
  const b = finite(values[i - lag])
  return a == null || b == null ? null : a - b
}

function buildGroup(nets, pcts, i) {
  const d1 = diff(pcts, i, 1)
  const d4 = diff(pcts, i, 4)
  const d12 = diff(pcts, i, 12)
  const direction = classifyDirection(d1)
  const [temperature, stateLabel] = classifyTemperature(pcts[i], d1, d4)
  return {
    net: finite(nets[i]),
    weekly_change: diff(nets, i, 1),
    four_week_change: diff(nets, i, 4),
    twelve_week_change: diff(nets, i, 12),
    percentile: finite(pcts[i]),
    percentile_change_1w: d1,
    percentile_change_4w: d4,
    percentile_change_12w: d12,
    percentile_observation_count: nets.slice(0, i + 1).filter((v) => finite(v) != null).length,
    direction,
    direction_arrow: arrow(direction),
    temperature,
    state_label: stateLabel,
    is_extreme: finite(pcts[i]) != null && (pcts[i] >= EXTREME_HIGH || pcts[i] <= EXTREME_LOW),
    measure: 'net_positioning_expanding_percentile',
  }
}

function relationship(c, nc) {
  if (c == null || nc == null) return 'unavailable'
  const cs = c >= 55 ? 1 : c <= 45 ? -1 : 0
  const ns = nc >= 55 ? 1 : nc <= 45 ? -1 : 0
  if (!cs || !ns) return 'mixed'
  if (cs === ns) return 'aligned'
  return Math.abs(c - nc) >= 60 ? 'strong_opposition' : 'opposed'
}

function spreadFlow(spread, d1) {
  if (spread == null || d1 == null) return 'unavailable'
  if (Math.abs(d1) < PCT_CHG_MILD) return 'stable'
  return d1 > 0 ? 'spread_widening' : 'spread_narrowing'
}

export function buildFreshWeeklyInspector(timelineRows) {
  const rows = Array.isArray(timelineRows) ? timelineRows : []
  if (!rows.length) return null

  const cNets = rows.map((r) => finite(r?.commercial_net))
  const ncNets = rows.map((r) => finite(r?.institutional_net))
  const nrNets = rows.map((r) => finite(r?.retail_net))
  const cPct = expandingPercentiles(cNets)
  const ncPct = expandingPercentiles(ncNets)
  const nrPct = expandingPercentiles(nrNets)

  const spreads = rows.map((_, i) =>
    cPct[i] == null || ncPct[i] == null ? null : cPct[i] - ncPct[i],
  )
  const spreadPcts = expandingPercentiles(spreads)
  const nrSpreads = rows.map((_, i) =>
    cPct[i] == null || nrPct[i] == null ? null : cPct[i] - nrPct[i],
  )
  const nrSpreadPcts = expandingPercentiles(nrSpreads)

  const weeks = rows.map((row, i) => {
    const commercial = buildGroup(cNets, cPct, i)
    const noncommercial = buildGroup(ncNets, ncPct, i)
    const nonreportable = buildGroup(nrNets, nrPct, i)
    const cn1 = diff(spreads, i, 1)
    const cn4 = diff(spreads, i, 4)
    return {
      date: String(row?.date || row?.label || '').slice(0, 10),
      commercial,
      noncommercial,
      nonreportable,
      cross: {
        commercial_percentile: commercial.percentile,
        noncommercial_percentile: noncommercial.percentile,
        nonreportable_percentile: nonreportable.percentile,
        comm_nc_spread: finite(spreads[i]),
        comm_nc_spread_percentile: finite(spreadPcts[i]),
        comm_nc_spread_change_1w: cn1,
        comm_nc_spread_change_4w: cn4,
        comm_nr_spread: finite(nrSpreads[i]),
        comm_nr_spread_percentile: finite(nrSpreadPcts[i]),
        relationship: relationship(commercial.percentile, noncommercial.percentile),
        flow: spreadFlow(spreads[i], cn1),
        measure: 'net_positioning_expanding_percentile',
      },
    }
  })

  return {
    available: true,
    measure: 'net_positioning_expanding_percentile',
    measure_label: 'Net positioning percentile (expanding, point-in-time)',
    weeks,
    week_count: weeks.length,
    source: 'live_workstation_timeline',
  }
}
