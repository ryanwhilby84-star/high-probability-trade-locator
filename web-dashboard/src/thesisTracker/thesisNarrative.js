// JS mirror of src/hptl/thesis_tracker/decision.py.
//
// Used only for locally-added theses (instrument-page "Track thesis"), which
// never pass through the Python store. Seeded/server theses already carry a
// `decision` block in the export and use that directly.

import { computeTrend, convictionSeries, normStatus } from './thesisModel.js'

export const MISSING_CONFIRMATIONS = [
  { label: 'Valuation', wired: false },
  { label: 'Seasonality', wired: false },
  { label: 'Retail positioning', wired: false },
  { label: 'Price / demand zone', wired: false },
]

const CONV_HIGH = 66
const CONV_MOD = 45

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function k(v) {
  if (!isNum(v)) return 'n/a'
  return Math.abs(v) >= 1000 ? `${Math.round(v / 1000)}k` : `${Math.round(v)}`
}
function signedK(v) {
  if (!isNum(v)) return 'n/a'
  const s = k(v)
  return v < 0 || s.startsWith('-') ? s : `+${s}`
}
function firstLast(snaps, field) {
  const vals = snaps.filter((s) => isNum(s[field])).map((s) => s[field])
  return vals.length ? [vals[0], vals[vals.length - 1]] : [null, null]
}
function effDir(direction, nf, nl) {
  if (direction === 'long') return 1
  if (direction === 'short') return -1
  if (isNum(nf) && isNum(nl)) return nl > nf ? 1 : nl < nf ? -1 : 0
  return 0
}
function significant(delta, base) {
  if (!isNum(delta)) return false
  const floor = base ? Math.max(1000, 0.04 * Math.abs(base)) : 1000
  return Math.abs(delta) >= floor
}

function evolution(snaps, direction) {
  const improved = []
  const deteriorated = []
  const unchanged = []
  if (snaps.length < 2) {
    const last = snaps[snaps.length - 1] || {}
    if (isNum(last.net_value)) unchanged.push(`Market net ${last.net_value > 0 ? 'long' : 'short'} (${signedK(last.net_value)})`)
    return { improved, deteriorated, unchanged }
  }
  const [lf, ll] = firstLast(snaps, 'long_value')
  const [sf, sl] = firstLast(snaps, 'short_value')
  const [nf, nl] = firstLast(snaps, 'net_value')
  const eff = effDir(direction, nf, nl) || 1

  if (isNum(lf) && isNum(ll)) {
    const d = ll - lf
    if (significant(d, lf)) (d * eff > 0 ? improved : deteriorated).push(`Long exposure ${d > 0 ? 'increased' : 'decreased'} (${signedK(d)})`)
    else unchanged.push('Long exposure broadly flat')
  }
  if (isNum(sf) && isNum(sl)) {
    const d = sl - sf
    if (significant(d, sf)) (d * eff < 0 ? improved : deteriorated).push(`Short exposure ${d > 0 ? 'increased' : 'decreased'} (${signedK(d)})`)
    else unchanged.push('Short exposure broadly flat')
  }
  if (isNum(nf) && isNum(nl)) {
    const d = nl - nf
    if (significant(d, nf)) (d * eff > 0 ? improved : deteriorated).push(`Net positioning moved ${signedK(d)} (${signedK(nf)} → ${signedK(nl)})`)
    unchanged.push(`Market still net ${nl > 0 ? 'long' : 'short'}`)
  }
  const mom = snaps.filter((s) => isNum(s.one_week_net_change)).map((s) => s.one_week_net_change)
  if (mom.length >= 2) {
    const lastM = mom[mom.length - 1]
    const prevM = mom[mom.length - 2]
    if (lastM * eff < 0) deteriorated.push('Weekly momentum turned against the thesis')
    else if (Math.abs(lastM) < Math.abs(prevM) * 0.6) deteriorated.push('Momentum slowed week-on-week')
    else if (lastM * eff > 0 && Math.abs(lastM) >= Math.abs(prevM)) improved.push('Weekly momentum building in favour')
  }
  const conv = convictionSeries(snaps)
  if (conv.length >= 2) {
    const d = conv[conv.length - 1] - conv[0]
    if (d >= 3) improved.push(`Composite conviction rose (${conv[0]} → ${conv[conv.length - 1]})`)
    else if (d <= -3) deteriorated.push(`Composite conviction fell (${conv[0]} → ${conv[conv.length - 1]})`)
  }
  return { improved, deteriorated, unchanged }
}

function interpretation(dl, ds, dn) {
  const lu = dl > 0
  const ld = dl < 0
  const su = ds > 0
  const sd = ds < 0
  const nu = dn > 0
  const nd = dn < 0
  if (nu && lu && sd) return 'Short covering and fresh accumulation — bearish pressure is unwinding.'
  if (nu && lu) return 'Fresh long accumulation is driving net positioning higher.'
  if (nu && sd) return 'Short covering is lifting net positioning — bears are stepping back.'
  if (nd && su && ld) return 'Fresh short selling and long liquidation — distribution underway.'
  if (nd && su) return 'Shorts are pressing; net positioning is deteriorating.'
  if (nd && ld) return 'Longs are being liquidated — the bid is fading.'
  if (lu && su) return 'Both legs are building — two-way conviction, not a one-sided trend yet.'
  return 'Positioning is broadly stable with no decisive one-sided flow.'
}

function story(snaps, direction) {
  if (!snaps.length) return [['No weekly snapshots captured yet.'], 'Awaiting first snapshot.']
  const weeks = new Set(snaps.map((s) => s.week).filter(Boolean)).size
  const [lf, ll] = firstLast(snaps, 'long_value')
  const [sf, sl] = firstLast(snaps, 'short_value')
  const [nf, nl] = firstLast(snaps, 'net_value')
  const lines = []
  if (isNum(nf) && isNum(nl) && weeks >= 2) {
    const dn = nl - nf
    const verb = dn > 0 ? 'improved' : dn < 0 ? 'deteriorated' : 'held flat'
    lines.push(`Over the last ${weeks} weeks net positioning ${verb} from ${signedK(nf)} to ${signedK(nl)} (${signedK(dn)}).`)
  } else if (isNum(nl)) {
    lines.push(`Net positioning is ${signedK(nl)} (${nl > 0 ? 'net long' : 'net short'}).`)
  }
  if (isNum(lf) && isNum(ll) && isNum(sf) && isNum(sl)) {
    lines.push(`Speculative longs went from ${k(lf)} to ${k(ll)} while shorts went from ${k(sf)} to ${k(sl)}.`)
  }
  if (isNum(nl)) {
    const side = nl > 0 ? 'long' : 'short'
    const dn = isNum(nf) ? nl - nf : 0
    if (side === 'short' && dn > 0) lines.push('The market remains net short, but bearish pressure continues to weaken.')
    else if (side === 'long' && dn > 0) lines.push('The market is net long and net-long conviction is building.')
    else if (side === 'short' && dn < 0) lines.push('The market is net short and shorts are still extending.')
    else if (side === 'long' && dn < 0) lines.push('The market remains net long, but long conviction is fading.')
    else lines.push(`The market remains net ${side} with little net change.`)
  }
  const gated = (last, first) => (isNum(last) && isNum(first) && significant(last - first, first) ? last - first : 0)
  return [lines, interpretation(gated(ll, lf), gated(sl, sf), gated(nl, nf))]
}

function readiness(snaps, direction, status, conv, trend) {
  const [nf, nl] = firstLast(snaps, 'net_value')
  const eff = effDir(direction, nf, nl)
  const netConfirms = isNum(nf) && isNum(nl) && eff && (nl - nf) * eff > 0
  const stl = firstLast(snaps, 'structural_score')[1]
  const structAligned = isNum(stl) && ((stl >= 50 && eff >= 0) || (stl <= 50 && eff < 0))
  const checks = [
    { label: 'Conviction trend improving', met: trend === 'improving' },
    { label: `Conviction ≥ ${CONV_MOD + 10}`, met: isNum(conv) && conv >= CONV_MOD + 10 },
    { label: 'Positioning confirming the bias', met: Boolean(netConfirms) },
    { label: 'Structural regime aligned', met: Boolean(structAligned) },
  ]
  const met = checks.filter((c) => c.met).length
  let label
  if (status === 'ACTIVE') label = 'In trade — manage the position'
  else if (status === 'READY') label = 'Limit-order preparation justified'
  else if (status === 'INVALIDATED') label = 'Invalidated — stand aside'
  else if (status === 'COMPLETED') label = 'Completed'
  else if (met >= 3) label = 'Approaching readiness — prepare watch levels'
  else if (met === 2) label = 'Developing — track weekly'
  else label = 'Early — observation only'
  return { label, met, total: checks.length, checks }
}

function priority(status, met) {
  if (status === 'INVALIDATED' || status === 'COMPLETED') return { tier: 3, label: 'Closed / archived', reason: 'Thesis is no longer actionable.' }
  if (status === 'READY' || status === 'ACTIVE') return { tier: 1, label: 'Ready soon — monitor closely', reason: `Status ${status}; near or in execution.` }
  if (status === 'DEVELOPING') return met >= 3 ? { tier: 1, label: 'Ready soon — monitor closely', reason: 'Developing with 3+ readiness checks met.' } : { tier: 2, label: 'Developing — track weekly', reason: 'Conditions forming; not yet aligned.' }
  return met >= 3 ? { tier: 2, label: 'Developing — track weekly', reason: 'Early signal already showing alignment.' } : { tier: 3, label: 'Observation only', reason: 'Initial signal; monitor for development.' }
}

function confidence(conv, trend, present) {
  if (!isNum(conv)) return ['Insufficient', 'No scored components available yet.']
  const n = (present || []).length
  let label = conv >= CONV_HIGH && trend !== 'deteriorating' ? 'High' : conv >= CONV_MOD ? 'Moderate' : 'Low'
  if (trend === 'deteriorating' && label === 'High') label = 'Moderate'
  return [label, `Composite ${Math.round(conv)}/100 from ${n} wired component(s) (${(present || []).join(', ') || 'none'}); valuation, seasonality and retail still unconfirmed.`]
}

function upgradeTriggers(direction, conv, present) {
  const longBias = direction !== 'short'
  const out = [longBias ? 'Another week of long accumulation' : 'Another week of short accumulation', longBias ? 'Further short reduction' : 'Further long liquidation']
  if (isNum(conv)) out.push(`Composite conviction holds above ${Math.max(CONV_MOD, Math.round(conv))}`)
  if (!(present || []).includes('structural')) out.push('Structural regime confirms the bias')
  out.push('Price reaches and defends a demand/supply zone (price feed not wired)')
  return out.slice(0, 5)
}

function invalidationTriggers(conv) {
  const out = ['Net positioning reverses against the thesis', 'Weekly momentum flips and sustains the other way']
  if (isNum(conv)) out.push(`Composite conviction falls below ${Math.max(20, Math.round(conv) - 15)}`)
  out.push('Demand zone fails / key level breaks (price feed not wired)')
  return out
}

function headline(direction, evo, prio) {
  const mover = (evo.improved[0] || evo.deteriorated[0] || '').toLowerCase()
  const side = { long: 'Long', short: 'Short', neutral: 'Neutral' }[direction] || 'Neutral'
  return mover ? `${side} thesis — ${mover}.` : `${side} thesis — ${prio.label.toLowerCase()}.`
}

export function buildDecision(thesis) {
  const snaps = [...(thesis.snapshots || [])].sort((a, b) => String(a.week || '').localeCompare(String(b.week || '')))
  const direction = String(thesis.direction_bias || 'neutral').toLowerCase()
  const status = normStatus(thesis.status)
  const series = convictionSeries(snaps)
  const conv = series.length ? series[series.length - 1] : null
  const trend = computeTrend(snaps)
  const present = (snaps.length ? snaps[snaps.length - 1].conviction_components_present : []) || []

  const evo = evolution(snaps, direction)
  const [storyLines, interp] = story(snaps, direction)
  const ready = readiness(snaps, direction, status, conv, trend)
  const prio = priority(status, ready.met)
  const [conf, confReason] = confidence(conv, trend, present)
  const upgrades = upgradeTriggers(direction, conv, present)

  return {
    priority_tier: prio.tier,
    priority_label: prio.label,
    priority_reason: prio.reason,
    confidence: conf,
    confidence_reason: confReason,
    headline: headline(direction, evo, prio),
    story: storyLines,
    interpretation: interp,
    missing_confirmations: MISSING_CONFIRMATIONS.map((m) => ({ ...m })),
    evolution: evo,
    upgrade_triggers: upgrades,
    invalidation_triggers: invalidationTriggers(conv),
    next_trigger: upgrades[0] || 'Monitor for development.',
    readiness: ready,
    weeks_observed: new Set(snaps.map((s) => s.week).filter(Boolean)).size,
  }
}

export function getDecision(thesis) {
  if (thesis?.decision && typeof thesis.decision === 'object') return thesis.decision
  return buildDecision(thesis || {})
}
