/** Rolling 3Y positioning context from legacy_cot group weeks (same percentile convention as backend). */

const WINDOW_WEEKS_3Y = 156

const BANDS = [
  [0, 10, 'Extreme Low'],
  [10, 30, 'Low'],
  [30, 70, 'Neutral'],
  [70, 90, 'High'],
  [90, 100.0001, 'Extreme High'],
]

function finite(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

function classifyPercentile(pct) {
  if (!Number.isFinite(pct)) return 'N/A'
  for (const [lo, hi, label] of BANDS) {
    if (pct >= lo && pct < hi) return label
  }
  return 'Extreme High'
}

function empiricalPercentileRank(window, value) {
  const v = finite(value)
  if (v == null) return null
  const finiteVals = window.map((x) => Number(x)).filter(Number.isFinite)
  const n = finiteVals.length
  if (!n) return null
  if (n === 1) return 50
  const below = finiteVals.filter((x) => x < v).length
  const equal = finiteVals.filter((x) => x === v).length
  return (100 * (below + 0.5 * equal)) / n
}

function extrema(arr) {
  const fin = arr.map((x) => Number(x)).filter(Number.isFinite)
  if (!fin.length) return { min: null, max: null, avg: null }
  const sum = fin.reduce((a, b) => a + b, 0)
  return { min: Math.min(...fin), max: Math.max(...fin), avg: sum / fin.length }
}

function pctOfMax(current, max) {
  const c = finite(current)
  const m = finite(max)
  if (c == null || m == null || m === 0) return null
  return Math.max(0, Math.min(100, (100 * c) / m))
}

function pctOfRange(current, min, max) {
  const c = finite(current)
  const lo = finite(min)
  const hi = finite(max)
  if (c == null || lo == null || hi == null || hi === lo) return null
  return Math.max(0, Math.min(100, (100 * (c - lo)) / (hi - lo)))
}

function classificationLine(metric, pct) {
  if (!Number.isFinite(pct)) return null
  const cls = classifyPercentile(pct)
  if (cls === 'N/A') return null
  return `${metric} ${cls}`
}

export function buildRolling3yContextFromWeeks(weeks) {
  if (!Array.isArray(weeks) || !weeks.length) return null

  const longs = weeks.map((w) => finite(w.long))
  const shorts = weeks.map((w) => finite(w.short))
  const nets = weeks.map((w) => finite(w.net))
  const ois = weeks.map((w) => finite(w.open_interest))

  const i = weeks.length - 1
  const lo = Math.max(0, i + 1 - WINDOW_WEEKS_3Y)
  const wL = longs.slice(lo, i + 1)
  const wS = shorts.slice(lo, i + 1)
  const wN = nets.slice(lo, i + 1)
  const wO = ois.slice(lo, i + 1)

  const longStats = extrema(wL)
  const shortStats = extrema(wS)
  const netStats = extrema(wN)
  const oiStats = extrema(wO)

  const pl = empiricalPercentileRank(wL, longs[i])
  const ps = empiricalPercentileRank(wS, shorts[i])
  const pn = empiricalPercentileRank(wN, nets[i])
  const po = empiricalPercentileRank(wO, ois[i])

  const round1 = (v) => (v == null ? null : Math.round(v * 10) / 10)
  const rowsUsed = wN.filter((x) => x != null).length || wL.filter((x) => x != null).length

  const classificationLines = [
    classificationLine('Net', pn),
    classificationLine('Long', pl),
    classificationLine('Short', ps),
    classificationLine('OI', po),
  ].filter(Boolean)

  const earliest = String(weeks[lo]?.report_date || '').slice(0, 10)
  const latest = String(weeks[i]?.report_date || '').slice(0, 10)

  return {
    window_weeks: WINDOW_WEEKS_3Y,
    rows_used: rowsUsed,
    earliest_report_date: earliest || null,
    latest_report_date: latest || null,
    long_min: longStats.min,
    long_max: longStats.max,
    long_avg: longStats.avg,
    short_min: shortStats.min,
    short_max: shortStats.max,
    short_avg: shortStats.avg,
    net_min: netStats.min,
    net_max: netStats.max,
    net_avg: netStats.avg,
    oi_min: oiStats.min,
    oi_max: oiStats.max,
    oi_avg: oiStats.avg,
    long_percentile: round1(pl),
    short_percentile: round1(ps),
    net_percentile: round1(pn),
    oi_percentile: round1(po),
    long_class: classifyPercentile(pl),
    short_class: classifyPercentile(ps),
    net_class: classifyPercentile(pn),
    oi_class: classifyPercentile(po),
    net_interpretation: pn == null ? 'N/A' : classifyPercentile(pn),
    long_interpretation: pl == null ? 'N/A' : classifyPercentile(pl),
    short_interpretation: ps == null ? 'N/A' : classifyPercentile(ps),
    oi_interpretation: po == null ? 'N/A' : classifyPercentile(po),
    classification_lines: classificationLines,
    current_long: longs[i],
    current_short: shorts[i],
    current_net: nets[i],
    current_oi: ois[i],
    long_vs_3y_max_pct: round1(pctOfMax(longs[i], longStats.max)),
    short_vs_3y_max_pct: round1(pctOfMax(shorts[i], shortStats.max)),
    net_range_pct: round1(pctOfRange(nets[i], netStats.min, netStats.max)),
    oi_vs_3y_max_pct: round1(pctOfMax(ois[i], oiStats.max)),
    long_crowding: classifyPercentile(pl),
    short_crowding: classifyPercentile(ps),
    oi_participation: classifyPercentile(po),
    crowding_classification_lines: classificationLines.filter((l) => l && !l.startsWith('Net')),
    summary: rowsUsed
      ? `Rolling ${WINDOW_WEEKS_3Y}-week positioning context using ${rowsUsed} legacy COT reports (${earliest} → ${latest}).`
      : 'N/A: insufficient multi-year history for rolling 3Y positioning context.',
  }
}
