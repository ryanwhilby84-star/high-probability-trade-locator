/** COT positioning spreadsheet heat bands (matches legacy positioning trail). */

const LEG_FLOW_EPS = 1

function rank01(value, min, max) {
  const v = Number(value)
  const lo = Number(min)
  const hi = Number(max)
  if (!Number.isFinite(v) || !Number.isFinite(lo) || !Number.isFinite(hi)) return 0
  if (hi === lo) return v >= lo ? 0.5 : 0
  return Math.max(0, Math.min(1, (v - lo) / (hi - lo)))
}

function bandFromRank(t) {
  if (t < 0.12) return 0
  if (t < 0.32) return 1
  if (t < 0.52) return 2
  if (t < 0.74) return 3
  return 4
}

function heatCellProps(className = 'cot-heat-neutral-0') {
  return { className }
}

function heatClassSigned(value, values, invert = false) {
  const raw = Number(value)
  if (!Number.isFinite(raw)) return 'cot-heat-neutral-0'
  const v = invert ? -raw : raw
  if (Math.abs(v) <= LEG_FLOW_EPS) return 'cot-heat-neutral-0'
  const palette = v > 0 ? 'bull' : 'bear'
  const absArr = values
    .map((x) => Math.abs(invert ? -Number(x) : Number(x)))
    .filter((x) => Number.isFinite(x))
  const mag = Math.abs(v)
  if (!absArr.length) return `cot-heat-${palette}-2`
  const band = bandFromRank(rank01(mag, Math.min(...absArr), Math.max(...absArr)))
  return `cot-heat-${palette}-${Math.max(1, band)}`
}

export function signedDeltaHeat(delta, deltas, invert = false) {
  return heatCellProps(heatClassSigned(delta, deltas, invert))
}

export function longLevelHeat(value, min, max) {
  const v = Number(value)
  const lo = Number(min)
  const hi = Number(max)
  if (!Number.isFinite(v) || !Number.isFinite(lo) || !Number.isFinite(hi)) return heatCellProps()
  if (hi === lo) return heatCellProps('cot-heat-neutral-1')
  const t = rank01(v, lo, hi)
  if (t >= 0.52) {
    const band = bandFromRank((t - 0.52) / 0.48)
    return heatCellProps(`cot-heat-bull-${Math.max(1, band)}`)
  }
  if (t <= 0.48) {
    const band = bandFromRank((0.48 - t) / 0.48)
    return heatCellProps(`cot-heat-bear-${Math.max(1, band)}`)
  }
  const band = bandFromRank(1 - Math.abs(t - 0.5) / 0.04)
  return heatCellProps(`cot-heat-neutral-${Math.min(2, band)}`)
}

export function shortLevelHeat(value, min, max) {
  const v = Number(value)
  const lo = Number(min)
  const hi = Number(max)
  if (!Number.isFinite(v) || !Number.isFinite(lo) || !Number.isFinite(hi)) return heatCellProps()
  if (hi === lo) return heatCellProps('cot-heat-neutral-1')
  const t = rank01(v, lo, hi)
  if (t >= 0.52) {
    const band = bandFromRank((t - 0.52) / 0.48)
    return heatCellProps(`cot-heat-bear-${Math.max(1, band)}`)
  }
  if (t <= 0.48) {
    const band = bandFromRank((0.48 - t) / 0.48)
    return heatCellProps(`cot-heat-bull-${Math.max(1, band)}`)
  }
  const band = bandFromRank(1 - Math.abs(t - 0.5) / 0.04)
  return heatCellProps(`cot-heat-neutral-${Math.min(2, band)}`)
}

export function netLevelHeat(value, min, max) {
  const v = Number(value)
  const lo = Number(min)
  const hi = Number(max)
  if (!Number.isFinite(v) || !Number.isFinite(lo) || !Number.isFinite(hi)) return heatCellProps()
  if (hi === lo) {
    return heatCellProps(v >= 0 ? 'cot-heat-bull-2' : 'cot-heat-bear-2')
  }
  const mid = (hi + lo) / 2
  const span = Math.max((hi - lo) / 2, 1)
  if (Math.abs(v - mid) <= span * 0.1) {
    return heatCellProps(`cot-heat-neutral-${bandFromRank(0.35)}`)
  }
  if (v > mid) {
    const band = bandFromRank(rank01(v, mid, hi))
    return heatCellProps(`cot-heat-bull-${Math.max(1, band)}`)
  }
  const band = bandFromRank(rank01(v, lo, mid))
  return heatCellProps(`cot-heat-bear-${Math.max(1, band)}`)
}

export function totalOiLevelHeat(value, min, max) {
  const v = Number(value)
  const lo = Number(min)
  const hi = Number(max)
  if (!Number.isFinite(v) || !Number.isFinite(lo) || !Number.isFinite(hi)) return heatCellProps()
  if (hi === lo) return heatCellProps('cot-heat-oi-2')
  const band = bandFromRank(rank01(v, lo, hi))
  return heatCellProps(`cot-heat-oi-${band}`)
}

export function buildRawCotHeatRanges(rows) {
  const nums = (key) => rows.map((r) => Number(r[key])).filter(Number.isFinite)
  const longs = nums('long')
  const shorts = nums('short')
  const nets = nums('net')
  const ois = nums('open_interest')
  const longDeltas = nums('weekly_change_long')
  const shortDeltas = nums('weekly_change_short')
  const netDeltas = nums('weekly_change_net')

  const range = (arr) =>
    arr.length ? { min: Math.min(...arr), max: Math.max(...arr) } : { min: 0, max: 0 }

  return {
    long: range(longs),
    short: range(shorts),
    net: range(nets),
    oi: range(ois),
    longDeltas,
    shortDeltas,
    netDeltas,
  }
}
