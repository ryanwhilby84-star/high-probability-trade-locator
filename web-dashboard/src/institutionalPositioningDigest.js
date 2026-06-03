/**
 * Compact institutional / flow read — derived only from COT row numerics + bias labels.
 * No narrative paragraphs; returns short strings for UI.
 */

function num(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : NaN
}

function fmtContracts(n) {
  if (!Number.isFinite(n)) return '—'
  const abs = Math.abs(Math.round(n))
  return abs.toLocaleString()
}

function netStance(net, cotBias) {
  if (Number.isFinite(net) && Math.abs(net) >= 300) {
    const a = Math.abs(net)
    if (net > 0) {
      if (a >= 25000) return 'Heavily net long'
      if (a >= 8000) return 'Net long'
      return 'Modestly net long'
    }
    if (a >= 25000) return 'Heavily net short'
    if (a >= 8000) return 'Net short'
    return 'Modestly net short'
  }
  const b = String(cotBias || '').toLowerCase()
  if (b.includes('bull')) return 'Long-biased (COT bias label)'
  if (b.includes('bear')) return 'Short-biased (COT bias label)'
  if (Number.isFinite(net)) return 'Roughly flat net'
  return 'No clean read'
}

function weeklyNetLine(w1, net) {
  if (!Number.isFinite(w1)) return 'Weekly net change: not in this row.'
  const mag = fmtContracts(w1)
  if (!Number.isFinite(net) || Math.abs(net) < 300) {
    if (w1 > 0) return `Net increased by ${mag} contracts vs prior week`
    if (w1 < 0) return `Net decreased by ${mag} contracts vs prior week`
    return 'Weekly net change: flat vs prior week.'
  }
  if (net > 0) {
    if (w1 > 0) return `Net long increased by ${mag} contracts`
    return `Net long reduced by ${mag} contracts`
  }
  if (w1 > 0) return `Net short reduced by ${mag} contracts`
  if (w1 < 0) return `Net short increased by ${mag} contracts`
  return 'Weekly net change: flat vs prior week.'
}

function fourWeekLine(w4, net, w1, storyFallback) {
  if (Number.isFinite(w4)) {
    const mag = fmtContracts(w4)
    if (!Number.isFinite(net) || Math.abs(net) < 300) {
      if (w4 > 0) return `Four-week net up ${mag} contracts vs four reports ago`
      if (w4 < 0) return `Four-week net down ${mag} contracts vs four reports ago`
      return '4-week net: flat vs four reports ago.'
    }
    if (net > 0) {
      if (w4 > 0) return `Still net long; net added ${mag} over four reports`
      if (w4 < 0) return `Still net long, but net trimmed by ${mag} over four reports`
      return '4-week net: flat vs four reports ago.'
    }
    if (w4 > 0) {
      if (Number.isFinite(w1) && w1 > 0) return `Still strongly bearish on net, but improving (+${mag} four-week net).`
      return `Net short edge eroded by ${mag} contracts over four reports.`
    }
    if (w4 < 0) return `Bearish book deepened by ${mag} contracts net over four reports`
    return '4-week net: flat vs four reports ago.'
  }
  const s = String(storyFallback || '').trim()
  if (s && s.toUpperCase() !== 'N/A') return s.length > 140 ? `${s.slice(0, 139)}…` : s
  return '4-week net change: not in this row (need consecutive prints).'
}

function meaningLine(net, w1, cotBias) {
  if (!Number.isFinite(net) || Math.abs(net) < 300) {
    if (Number.isFinite(w1) && w1 !== 0) return 'Book is near flat; weekly flow is the main signal on this print.'
    return 'No clean read — net is too flat for a directional book read.'
  }
  if (net > 0) {
    if (Number.isFinite(w1)) {
      if (w1 > 0) return 'Long-biased book; this week added to the net long.'
      if (w1 < 0) return 'Long-biased book; this week trimmed the net long.'
    }
    return 'Long-biased book; weekly net change not on this print.'
  }
  if (Number.isFinite(w1)) {
    if (w1 > 0) return 'Bears still control the book, but pressure is fading.'
    if (w1 < 0) return 'Bears still control the book; weekly flow is adding to the short lean.'
  }
  return 'Short-biased book; weekly flow not available for a momentum qualifier.'
}

function traderReadFromDeltas(net, w1, longW, shortW) {
  const L = num(longW)
  const S = num(shortW)
  const hasLegs = Number.isFinite(L) && Number.isFinite(S)

  if (!Number.isFinite(w1) && !hasLegs) return 'No clean read'

  if (Number.isFinite(net) && Math.abs(net) < 500 && hasLegs) {
    if (Math.abs(L - S) < Math.max(Math.abs(L), Math.abs(S)) * 0.25) return 'Two-way / mixed'
  }

  if (!Number.isFinite(net) || Math.abs(net) < 300) {
    if (hasLegs) {
      if (L > 0 && S > 0 && Math.abs(L - S) < 500) return 'Two-way / mixed'
      if (L > S + 500 && S <= 0) return 'Bullish pressure increasing'
      if (S > L + 500 && L <= 0) return 'Bearish pressure increasing'
      return 'Two-way / mixed'
    }
    return 'No clean read'
  }

  if (net > 0) {
    if (Number.isFinite(w1)) {
      if (w1 > 0) return 'Bullish pressure increasing'
      if (w1 < 0) return 'Bullish pressure fading'
    }
    if (hasLegs && L < S) return 'Bullish pressure fading'
    if (hasLegs && L > S) return 'Bullish pressure increasing'
    return 'Two-way / mixed'
  }

  if (Number.isFinite(w1)) {
    if (w1 > 0) return 'Bearish pressure fading'
    if (w1 < 0) return 'Bearish pressure increasing'
  }
  if (hasLegs && S < L) return 'Bearish pressure fading'
  if (hasLegs && S > L) return 'Bearish pressure increasing'
  return 'Two-way / mixed'
}

function actionBiasLine(net, w1, traderRead, zoneFocus) {
  const z = String(zoneFocus || '').trim()
  const zclip = z.length > 90 ? `${z.slice(0, 89)}…` : z

  if (traderRead === 'No clean read' || traderRead === 'Two-way / mixed') {
    return 'No clean edge'
  }
  if (traderRead === 'Bearish pressure fading') {
    const tail = zclip ? ` ${zclip}` : ''
    return `Do not chase shorts into demand.${tail} Watch for short-covering continuation or a demand-zone reaction.`
  }
  if (traderRead === 'Bearish pressure increasing') {
    return zclip ? `Respect supply / weakness until flow stabilizes. Watch: ${zclip}` : 'Avoid chasing longs; respect supply until flow stabilizes.'
  }
  if (traderRead === 'Bullish pressure fading') {
    return zclip ? `Avoid chasing longs into extension. Watch: ${zclip}` : 'Avoid chasing longs; look for supply rejection or participation fade.'
  }
  if (traderRead === 'Bullish pressure increasing') {
    return zclip ? `Look for demand continuation; do not fade strength without a level. Watch: ${zclip}` : 'Look for demand continuation; do not fade strength without a level.'
  }
  return 'No clean edge'
}

/**
 * @param {object} row COT / confluence row
 */
export function buildInstitutionalDecisionDigest(row) {
  const net = num(row?.net_value)
  const w1 = num(row?.one_week_net_change)
  const w4 = num(row?.four_week_net_change)
  const longW = row?.one_week_long_change
  const shortW = row?.one_week_short_change
  const cotBias = row?.cot_bias

  const positioning = {
    stance: netStance(net, cotBias),
    weekly: weeklyNetLine(w1, net),
    fourWeek: fourWeekLine(w4, net, w1, row?.four_week_positioning_story),
    meaning: meaningLine(net, w1, cotBias),
  }

  const traderRead = traderReadFromDeltas(net, w1, longW, shortW)
  const actionBias = actionBiasLine(net, w1, traderRead, row?.zone_focus)

  return { positioning, traderRead, actionBias }
}
