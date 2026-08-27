/**
 * Human copy + light regime shading for Nasdaq vs US 10Y overlay (UI only).
 */

export const NQ_YIELD_DISPLAY = {
  index: 'Nasdaq Composite',
  yield: 'US 10Y Yield',
}

/**
 * One-line headline above the chart from rebased window + latest rolling correlation.
 * @param {Record<string, unknown>} rm — relationship map payload
 */
export function buildNqYieldInterpretationHeadline(rm) {
  const nq = rm?.nasdaq_rebased_pct || []
  const y10 = rm?.dgs10_rebased_pct || []
  const n = nq.length
  if (n < 25) {
    return 'Both lines reset to 0% at the start of the window — follow separation vs overlap to read who is leading.'
  }
  const win = 20
  const i1 = n - 1
  const i0 = Math.max(0, i1 - win)
  const dn = Number(nq[i1]) - Number(nq[i0])
  const dy = Number(y10[i1]) - Number(y10[i0])
  const rc = rm?.latest_rolling_corr_20
  const rcN = rc != null && Number.isFinite(Number(rc)) ? Number(rc) : null

  if (dy > 0.25 && dn > 0.15) {
    return 'Nasdaq has held in even as yields rose here — equities are absorbing higher rates on this stretch (still size and catalyst aware).'
  }
  if (dy > 0.15 && dn < -0.25) {
    return 'Yields moved up while Nasdaq softened — the classic “rates friction” picture shows clearly in the overlay.'
  }
  if (dy < -0.2 && dn > 0.25) {
    return 'Yields eased while Nasdaq rebuilt — a more comfortable tone for growth/duration over this window.'
  }
  if (rcN != null && rcN < -0.22) {
    return 'Recent day-to-day moves still skew opposite: when yields jump, Nasdaq often dips — watch for stretches where that link breaks.'
  }
  if (rcN != null && rcN > 0.22) {
    return 'Recent day-to-day moves have been unusually in sync — worth attention when yields and equities rise or fall together.'
  }
  return 'Read the spread between lines: widening usually means one side of the macro vs equity story is dominating week to week.'
}

/**
 * @typedef {{ i: number, dt: string, nq: number, y10: number, rc20: number|null, zone: 'supportive'|'pressure'|'tension'|'neutral' }} NqYieldRow
 * @param {Record<string, unknown>} rm
 * @returns {NqYieldRow[]}
 */
export function buildNqYieldChartRows(rm) {
  const dates = rm?.dates || []
  const nq = rm?.nasdaq_rebased_pct || []
  const y10 = rm?.dgs10_rebased_pct || []
  const rc = rm?.rolling_corr_20 || []
  const win = 15
  const out = []
  for (let idx = 0; idx < dates.length; idx++) {
    const nv = nq[idx]
    const yv = y10[idx]
    if (nv == null || yv == null || !Number.isFinite(Number(nv)) || !Number.isFinite(Number(yv))) continue
    let zone = 'neutral'
    if (idx >= win) {
      const dn = Number(nq[idx]) - Number(nq[idx - win])
      const dy = Number(y10[idx]) - Number(y10[idx - win])
      if (dy > 0.35 && dn < -0.25) zone = 'pressure'
      else if (dy > 0.25 && dn > 0.2) zone = 'tension'
      else if (dy < -0.2 && dn > 0.25) zone = 'supportive'
      else if (dy > 0.15 && dn < -0.1) zone = 'pressure'
    }
    const r = rc[idx]
    const rc20 = r != null && Number.isFinite(Number(r)) ? Number(r) : null
    out.push({ i: out.length, dt: dates[idx], nq: Number(nv), y10: Number(yv), rc20, zone })
  }
  return out
}

/**
 * Merge contiguous zones for ReferenceArea.
 * @param {NqYieldRow[]} rows
 * @returns {{ x1: number, x2: number, zone: string }[]}
 */
export function mergeShadeSegments(rows) {
  if (!rows.length) return []
  const segs = []
  let start = 0
  let z = rows[0].zone
  for (let k = 1; k <= rows.length; k++) {
    const cur = rows[k]?.zone
    if (k === rows.length || cur !== z) {
      if (z && z !== 'neutral') {
        segs.push({ x1: rows[start].i, x2: rows[k - 1].i, zone: z })
      }
      start = k
      z = cur
    }
  }
  return segs
}

const SHADE = {
  supportive: 'rgba(16, 185, 129, 0.09)',
  pressure: 'rgba(248, 113, 113, 0.08)',
  tension: 'rgba(251, 191, 36, 0.07)',
}

export function shadeFill(zone) {
  return SHADE[zone] || 'transparent'
}
