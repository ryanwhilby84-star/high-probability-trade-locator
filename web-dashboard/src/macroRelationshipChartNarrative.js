/**
 * Chart copy + shading for Macro Relationship Map (generic price vs driver payload).
 */

/** @param {Record<string, unknown>} rm */
export function getSeriesPair(rm) {
  const price =
    (Array.isArray(rm?.price_rebased_pct) && rm.price_rebased_pct) ||
    rm?.nasdaq_rebased_pct ||
    rm?.sp500_rebased_pct ||
    []
  const driver =
    (Array.isArray(rm?.driver_rebased_pct) && rm.driver_rebased_pct) ||
    rm?.dgs10_rebased_pct ||
    []
  const priceLabel = String(rm?.price_series_display || 'Price')
  const driverLabel = String(rm?.driver_series_display || 'Macro driver')
  const cadence = String(rm?.cadence || 'daily')
  return { price, driver, priceLabel, driverLabel, cadence }
}

/** @param {Record<string, unknown>} rm */
export function rollingStripLabels(rm) {
  const cadence = String(rm?.cadence || 'daily')
  const n1 = rm?.rolling_primary_n ?? 20
  const n2 = rm?.rolling_secondary_n ?? 30
  const n3 = rm?.rolling_tertiary_n
  if (cadence === 'monthly') {
    return { a: `${n1} months`, b: `${n2} months`, c: n3 != null ? `${n3} months` : null }
  }
  if (cadence === 'quarterly') {
    return { a: `${n1} quarters`, b: `${n2} quarters`, c: n3 != null ? `${n3} quarters` : null }
  }
  return { a: `${n1} sessions`, b: `${n2} sessions`, c: n3 != null ? `${n3} sessions` : null }
}

/** @param {Record<string, unknown>} rm */
export function correlationRegimeLabel(rm) {
  const r = String(rm?.correlation_regime || '').toLowerCase()
  if (r === 'unstable') return { text: 'Unstable link', tone: 'amber' }
  if (r === 'weak') return { text: 'Weak / quiet link', tone: 'slate' }
  if (r === 'diverging') return { text: 'Regime shifting', tone: 'amber' }
  if (r === 'active') return { text: 'Active link', tone: 'emerald' }
  return { text: '—', tone: 'slate' }
}

function zoneThresholds(cadence) {
  if (cadence === 'monthly') return { win: 4, dyP: 0.06, dnN: -0.04, dyT: 0.04, dnT: 0.03, dyS: -0.04, dnP: 0.03, dyPr: 0.05, dnW: -0.02 }
  if (cadence === 'quarterly') return { win: 3, dyP: 0.08, dnN: -0.05, dyT: 0.05, dnT: 0.04, dyS: -0.05, dnP: 0.04, dyPr: 0.06, dnW: -0.03 }
  return { win: 15, dyP: 0.35, dnN: -0.25, dyT: 0.25, dnT: 0.2, dyS: -0.2, dnP: 0.25, dyPr: 0.15, dnW: -0.1 }
}

/**
 * @param {Record<string, unknown>} rm
 */
export function buildOverlayInterpretationHeadline(rm) {
  const { price: nq, driver: y10, priceLabel, driverLabel, cadence } = getSeriesPair(rm)
  const n = nq.length
  if (n < 8) {
    return 'Both lines reset to 0% at the start of the window — follow separation vs overlap to read who is leading.'
  }
  const win = cadence === 'daily' ? 20 : cadence === 'monthly' ? 6 : 4
  const i1 = n - 1
  const i0 = Math.max(0, i1 - win)
  const dn = Number(nq[i1]) - Number(nq[i0])
  const dy = Number(y10[i1]) - Number(y10[i0])
  const rc = rm?.latest_rolling_corr_20
  const rcN = rc != null && Number.isFinite(Number(rc)) ? Number(rc) : null

  const pl = priceLabel
  const dl = driverLabel

  if (dy > 0.12 && dn > 0.08) {
    return `${pl} has held in while ${dl} rose on this stretch — price is absorbing macro pressure here (still not a signal).`
  }
  if (dy > 0.08 && dn < -0.1) {
    return `${dl} moved higher while ${pl} softened — macro friction shows up clearly in the overlay.`
  }
  if (dy < -0.08 && dn > 0.1) {
    return `${dl} eased while ${pl} rebuilt — a friendlier macro tone for this market over this window.`
  }
  if (rcN != null && rcN < -0.22) {
    return `Short-term moves skew opposite: when ${dl} pushes one way, ${pl} often leans the other — watch for stretches where that breaks.`
  }
  if (rcN != null && rcN > 0.22) {
    return `Short-term moves have been more aligned than usual — worth a sanity check vs your normal read for ${pl}.`
  }
  return `Read the spread between ${pl} and ${dl}: widening usually means one side of the macro vs price story is winning.`
}

/**
 * @typedef {{ i: number, dt: string, nq: number, y10: number, rc20: number|null, zone: string }} OverlayRow
 * @param {Record<string, unknown>} rm
 * @returns {OverlayRow[]}
 */
export function buildOverlayChartRows(rm) {
  const { price: nq, driver: y10, cadence } = getSeriesPair(rm)
  const dates = rm?.dates || []
  const rc = rm?.rolling_corr_20 || []
  const zt = zoneThresholds(cadence)
  const win = zt.win
  const out = []
  for (let idx = 0; idx < dates.length; idx++) {
    const nv = nq[idx]
    const yv = y10[idx]
    if (nv == null || yv == null || !Number.isFinite(Number(nv)) || !Number.isFinite(Number(yv))) continue
    let zone = 'neutral'
    if (idx >= win) {
      const dn = Number(nq[idx]) - Number(nq[idx - win])
      const dy = Number(y10[idx]) - Number(y10[idx - win])
      if (dy > zt.dyP && dn < zt.dnN) zone = 'pressure'
      else if (dy > zt.dyT && dn > zt.dnT) zone = 'tension'
      else if (dy < zt.dyS && dn > zt.dnP) zone = 'supportive'
      else if (dy > zt.dyPr && dn < zt.dnW) zone = 'pressure'
    }
    const r = rc[idx]
    const rc20 = r != null && Number.isFinite(Number(r)) ? Number(r) : null
    out.push({ i: idx, dt: dates[idx], nq: Number(nv), y10: Number(yv), rc20, zone })
  }
  return out
}

/**
 * @param {OverlayRow[]} rows
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

/** @param {Record<string, unknown>} rm */
export function plainRollingRead(rm) {
  const { priceLabel, driverLabel, cadence } = getSeriesPair(rm)
  const c20 = rm?.latest_rolling_corr_20
  const cad = cadence === 'daily' ? 'day-to-day' : cadence === 'monthly' ? 'month-to-month' : 'quarter-to-quarter'
  if (c20 == null || !Number.isFinite(Number(c20))) {
    return 'Short-term statistical link is still filling in — give the window more time.'
  }
  const a = Math.abs(Number(c20))
  if (a < 0.12) {
    return `${cad}, ${priceLabel} and ${driverLabel} are only loosely linked — other drivers are probably leading.`
  }
  if (Number(c20) < -0.25) {
    return `${cad} changes often run opposite: when ${driverLabel} pushes one way, ${priceLabel} tends to lean the other — context only.`
  }
  if (Number(c20) > 0.25) {
    return `${cad} changes have been unusually in sync — notice if that matches your usual mental model for ${priceLabel}.`
  }
  return `Co-movement is in a middling range for this sample — use it as tone, not as an entry trigger.`
}
