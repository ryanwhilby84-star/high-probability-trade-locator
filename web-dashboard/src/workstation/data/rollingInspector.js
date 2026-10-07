// Canonical inspector horizon. Rebuild from dated raw net positions so older
// expanding-percentile exports cannot silently change the trading view.
export const INSPECTOR_MEASURE = 'net_positioning_rolling_3y_percentile'
export const INSPECTOR_LABEL = 'Net positioning percentile (rolling 3Y / 156 reports, point-in-time)'
const groups = ['commercial', 'noncommercial', 'nonreportable']
const finite = (v) => typeof v === 'number' && Number.isFinite(v)
const rank = (values, value) => {
  const valid = values.filter(finite)
  if (!valid.length || !finite(value)) return null
  return Math.round(10000 * (valid.filter((v) => v < value).length + 0.5 * valid.filter((v) => v === value).length) / valid.length) / 100
}
const delta = (values, i, lag) => i >= lag && finite(values[i]) && finite(values[i - lag])
  ? Math.round((values[i] - values[i - lag]) * 100) / 100 : null

function flow(p, d1, d4) {
  if (!finite(p)) return ['unknown', 'Unavailable']
  let rising = finite(d1) && d1 >= 2, falling = finite(d1) && d1 <= -2
  let strongUp = finite(d1) && d1 >= 7, strongDown = finite(d1) && d1 <= -7
  if (finite(d1) && Math.abs(d1) < 2 && finite(d4)) {
    if (d4 >= 7) { rising = true; strongUp = true }
    else if (d4 >= 2) rising = true
    else if (d4 <= -7) { falling = true; strongDown = true }
    else if (d4 <= -2) falling = true
  }
  if (p >= 70) {
    if (strongUp) return ['heating_rapidly', 'Deeper into extreme']
    if (rising) return ['heating', 'Deeper into extreme']
    if (strongDown || falling) return ['cooling_from_extreme', 'Cooling from extreme']
    return ['elevated_stable', 'Elevated / stable']
  }
  if (p <= 30) {
    if (strongDown || falling) return ['deepening_extreme', 'Deeper into low extreme']
    if (strongUp) return ['recovering_strong', 'Strong rotation away from extreme']
    if (rising) return ['recovering', 'Moving out of extreme']
    return ['depressed_stable', 'Depressed / stable']
  }
  if (strongUp || (finite(d4) && d4 >= 7) || rising) return ['building', 'Rotation strengthening']
  if (strongDown || (finite(d4) && d4 <= -7) || falling) return ['weakening', 'Rotation weakening']
  return ['neutral', 'Neutral']
}

export function rollingInspector(source) {
  if (!source?.weeks?.length) return null
  const weeks = [...source.weeks].sort((a, b) => a.date.localeCompare(b.date))
  if (new Set(weeks.map((w) => w.date)).size !== weeks.length) throw new Error('Duplicate COT report dates in inspector archive')
  const series = Object.fromEntries(groups.map((g) => [g, weeks.map((w) => w[g]?.net)]))
  const pcts = Object.fromEntries(groups.map((g) => [g, series[g].map((net, i) => rank(series[g].slice(Math.max(0, i - 155), i + 1), net))]))
  const cn = weeks.map((_, i) => finite(pcts.commercial[i]) && finite(pcts.noncommercial[i]) ? Math.round((pcts.commercial[i] - pcts.noncommercial[i]) * 100) / 100 : null)
  const cr = weeks.map((_, i) => finite(pcts.commercial[i]) && finite(pcts.nonreportable[i]) ? Math.round((pcts.commercial[i] - pcts.nonreportable[i]) * 100) / 100 : null)
  return { ...source, measure: INSPECTOR_MEASURE, measure_label: INSPECTOR_LABEL, weeks: weeks.map((w, i) => {
    const next = { ...w, summaries: {} }
    for (const g of groups) {
      const pct = pcts[g][i], d1 = delta(pcts[g], i, 1), d4 = delta(pcts[g], i, 4)
      const direction = !finite(d1) ? 'unknown' : d1 >= 7 ? 'strongly_increasing' : d1 >= 2 ? 'increasing' : d1 <= -7 ? 'strongly_decreasing' : d1 <= -2 ? 'decreasing' : 'stable'
      const [temperature, state_label] = flow(pct, d1, d4)
      next[g] = { ...w[g], weekly_change: delta(series[g], i, 1), four_week_change: delta(series[g], i, 4), twelve_week_change: delta(series[g], i, 12), percentile: pct, percentile_change_1w: d1, percentile_change_4w: d4, percentile_change_12w: delta(pcts[g], i, 12), percentile_observation_count: series[g].slice(Math.max(0, i - 155), i + 1).filter(finite).length, direction, direction_arrow: ({strongly_increasing:'▲▲',increasing:'▲',stable:'→',decreasing:'▼',strongly_decreasing:'▼▼',unknown:'·'})[direction], temperature, state_label, is_extreme: finite(pct) && (pct >= 90 || pct <= 10), measure: INSPECTOR_MEASURE }
      const label = ({commercial:'Commercial',noncommercial:'Non-Commercial',nonreportable:'Non-Reportable'})[g]
      next.summaries[g] = finite(pct) ? `${label} is at the ${Math.round(pct)}th three-year net percentile. ${state_label}.` : `${label} three-year positioning is unavailable.`
    }
    const c = pcts.commercial[i], n = pcts.noncommercial[i]
    const side = (p) => p >= 55 ? 1 : p <= 45 ? -1 : 0
    const relationship = !finite(c) || !finite(n) ? 'unavailable' : !side(c) || !side(n) ? 'mixed' : side(c) === side(n) ? 'aligned' : Math.abs(c - n) >= 60 ? 'strong_opposition' : 'opposed'
    const change = delta(cn, i, 1) ?? delta(cn, i, 4)
    const widening = (cn[i] >= 0 && change > 0) || (cn[i] < 0 && change < 0)
    const crossFlow = !finite(change) ? 'unavailable' : Math.abs(change) < 2 ? 'stable' : relationship.includes('opposition') || relationship === 'opposed' ? `opposition_${widening ? 'widening' : 'narrowing'}${Math.abs(change) >= 7 ? '_rapidly' : ''}` : change > 0 ? 'spread_widening' : 'spread_narrowing'
    next.cross = { commercial_percentile:c, noncommercial_percentile:n, nonreportable_percentile:pcts.nonreportable[i], comm_nc_spread:cn[i], comm_nc_spread_percentile:rank(cn.slice(0,i+1),cn[i]), comm_nc_spread_change_1w:delta(cn,i,1), comm_nc_spread_change_4w:delta(cn,i,4), comm_nr_spread:cr[i], comm_nr_spread_percentile:rank(cr.slice(0,i+1),cr[i]), relationship, flow:crossFlow, measure:INSPECTOR_MEASURE }
    return next
  }) }
}
