// Mirrors src/hptl/thesis_tracker/alignment.py + opportunity.py (client-side for local theses).

import { confluenceRowToScoringSnap } from './confluenceOverlay.js'
import { computeTrend, directionFromSnapshot } from './thesisModel.js'
import { normStatus, STATUS_COMPLETED, STATUS_INVALIDATED } from './thesisModel.js'

export const ACTION_HIGH = 'HIGH ATTENTION'
export const ACTION_PAY = 'PAY ATTENTION'
export const ACTION_WATCH = 'WATCH'
export const ACTION_NONE = 'NO EDGE'
export const ACTION_CLOSED = 'CLOSED'

const ACTION_WEIGHT = {
  [ACTION_HIGH]: 50,
  [ACTION_PAY]: 35,
  [ACTION_WATCH]: 15,
  [ACTION_NONE]: 0,
  [ACTION_CLOSED]: -100,
}

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

export function displayInstrumentName(market) {
  const base = String(market || '').split('/')[0].trim()
  return (base || market || '').toUpperCase()
}

function biasDirection(cotBias) {
  const b = String(cotBias || '').toLowerCase()
  if (b.includes('bull')) return 'long'
  if (b.includes('bear')) return 'short'
  return 'neutral'
}

function effectiveDirection(thesisDirection, snap) {
  const d = String(thesisDirection || 'neutral').toLowerCase()
  if (d === 'long' || d === 'short') return d
  return biasDirection(snap.cot_bias)
}

function institutionsState(cotBias, cotScore) {
  const b = String(cotBias || '').trim()
  if (!b || b.toUpperCase() === 'N/A') return 'UNAVAILABLE'
  const strong = isNum(cotScore) && cotScore >= 6
  if (b.toLowerCase().includes('bull')) return strong ? 'STRONGLY BULLISH' : 'BULLISH'
  if (b.toLowerCase().includes('bear')) return strong ? 'STRONGLY BEARISH' : 'BEARISH'
  if (b.toLowerCase().includes('neutral')) return 'NEUTRAL'
  return b.toUpperCase()
}

function retailState(snap) {
  const net = snap.retail_net
  if (!isNum(net)) return 'UNAVAILABLE'
  if (Math.abs(net) < 500) return 'NEUTRAL'
  return net > 0 ? 'BULLISH' : 'BEARISH'
}

function locationState(zoneFocus) {
  const z = String(zoneFocus || '').trim()
  if (!z || z.toUpperCase() === 'N/A') return 'UNAVAILABLE'
  const low = z.toLowerCase()
  if (low.includes('demand first') || low.startsWith('demand')) return 'AT DEMAND'
  if (low.includes('look for demand') || low.includes('demand watch')) return 'AWAITING DEMAND ZONE'
  if (low.includes('demand')) return 'AT DEMAND'
  if (low.includes('supply first') || low.startsWith('supply')) return 'AT SUPPLY'
  if (low.includes('look for supply') || low.includes('supply watch')) return 'AWAITING SUPPLY ZONE'
  if (low.includes('supply')) return 'AT SUPPLY'
  if (low.includes('wait') || low.includes('mixed')) return 'AWAITING DEMAND ZONE'
  return z.toUpperCase()
}

function locationPass(state, direction) {
  if (state === 'UNAVAILABLE') return false
  if (direction === 'long') return state === 'AT DEMAND' || state === 'AWAITING DEMAND ZONE'
  if (direction === 'short') return state === 'AT SUPPLY' || state === 'AWAITING SUPPLY ZONE'
  return state !== 'UNAVAILABLE'
}

function institutionsPass(state, direction) {
  if (state === 'UNAVAILABLE') return false
  if (direction === 'long') return state.includes('BULL')
  if (direction === 'short') return state.includes('BEAR')
  return state === 'NEUTRAL'
}

function retailPass(state, direction) {
  if (state === 'UNAVAILABLE') return false
  if (direction === 'long') return state === 'BEARISH'
  if (direction === 'short') return state === 'BULLISH'
  return state === 'NEUTRAL'
}

function valuationPass(bias, direction) {
  if (!bias || bias === 'UNAVAILABLE') return false
  if (direction === 'long') return bias === 'Bullish'
  if (direction === 'short') return bias === 'Bearish'
  return bias === 'Neutral'
}

function seasonalityPass(bias, direction) {
  return valuationPass(bias, direction)
}

function valuationPillar(snap, direction) {
  const bias = String(snap.valuation_bias || 'UNAVAILABLE')
  const wired =
    !!snap.valuation_wired ||
    (bias && !['UNAVAILABLE', 'PENDING', ''].includes(bias))
  if (!wired) {
    return {
      pillar: 'valuation',
      label: 'Valuation',
      state: 'UNAVAILABLE',
      score_display: '—',
      pass: false,
      wired: false,
      one_line: snap.valuation_reason || 'Valuation engine has no data for this week.',
    }
  }
  const score = snap.valuation_score
  return {
    pillar: 'valuation',
    label: 'Valuation',
    state: bias.toUpperCase(),
    score_display: isNum(score) ? `${score.toFixed(1)} / 10` : '—',
    pass: valuationPass(bias, direction),
    wired: true,
    one_line: snap.valuation_reason || '',
  }
}

function seasonalityPillar(snap, direction) {
  const bias = String(snap.seasonality_bias || 'UNAVAILABLE')
  const wired =
    !!snap.seasonality_wired ||
    (bias && !['UNAVAILABLE', 'PENDING', ''].includes(bias))
  if (!wired) {
    return {
      pillar: 'seasonality',
      label: 'Seasonality',
      state: 'UNAVAILABLE',
      score_display: '—',
      pass: false,
      wired: false,
      one_line: snap.seasonality_reason || 'Seasonality engine has no data for this week.',
    }
  }
  const score = snap.seasonality_score
  return {
    pillar: 'seasonality',
    label: 'Seasonality',
    state: bias.toUpperCase(),
    score_display: isNum(score) ? `${score.toFixed(1)} / 10` : '—',
    pass: seasonalityPass(bias, direction),
    wired: true,
    one_line: snap.seasonality_reason || '',
  }
}

function institutionsOneLine(state, snap) {
  const ps = String(snap.positioning_state || '').trim()
  if (state === 'UNAVAILABLE') return 'Institutional positioning not available for this week.'
  if (ps && ps.toUpperCase() !== 'N/A') return `Institutions read ${state.toLowerCase()} (${ps}).`
  return `Institutions read ${state.toLowerCase()} on the COT score scale.`
}

function retailOneLine(state, snap) {
  if (state === 'UNAVAILABLE' || !isNum(snap.retail_net)) return 'Retail proxy (non-reportable) not on this snapshot.'
  const side = snap.retail_net > 0 ? 'long' : 'short'
  return `Retail proxy is net ${side} (${Math.round(snap.retail_net).toLocaleString()} contracts).`
}

function locationOneLine(state, zoneRaw) {
  if (state === 'UNAVAILABLE') return 'Location / zone tag not on this snapshot.'
  if (zoneRaw) return `HTPL location tag: ${zoneRaw}.`
  return `Location state: ${state.replace(/_/g, ' ')}.`
}

export function evaluatePillars(snap, direction) {
  const pillars = [valuationPillar(snap, direction)]

  const instState = institutionsState(snap.cot_bias, snap.cot_score)
  pillars.push({
    pillar: 'institutions',
    label: 'Institutions',
    state: instState,
    score_display: isNum(snap.cot_score) ? `${snap.cot_score.toFixed(1)} / 10` : '—',
    pass: institutionsPass(instState, direction),
    wired: instState !== 'UNAVAILABLE',
    one_line: institutionsOneLine(instState, snap),
  })

  const retState = retailState(snap)
  pillars.push({
    pillar: 'retail',
    label: 'Retail',
    state: retState,
    score_display: '—',
    pass: retailPass(retState, direction),
    wired: retState !== 'UNAVAILABLE',
    one_line: retailOneLine(retState, snap),
  })

  const locState = locationState(snap.zone_focus)
  pillars.push({
    pillar: 'location',
    label: 'Location',
    state: locState,
    score_display: '—',
    pass: locationPass(locState, direction),
    wired: locState !== 'UNAVAILABLE',
    one_line: locationOneLine(locState, snap.zone_focus),
  })

  pillars.push(seasonalityPillar(snap, direction))

  return pillars
}

export function alignmentSummary(pillars) {
  const total = pillars.length
  const pass = pillars.filter((p) => p.pass === true).length
  return { pass, total, label: `${pass} / ${total}` }
}

function deriveAction({ alignmentPass, trend, status, archived }) {
  const st = normStatus(status)
  if (archived || st === STATUS_INVALIDATED || st === STATUS_COMPLETED) return ACTION_CLOSED
  if (alignmentPass >= 5 && trend !== 'deteriorating') return ACTION_HIGH
  if (alignmentPass >= 4) return ACTION_PAY
  if (alignmentPass >= 3 && trend === 'improving') return ACTION_PAY
  if (alignmentPass >= 3) return ACTION_WATCH
  return ACTION_NONE
}

function rankScore(alignmentPass, action, cotScore) {
  const w = ACTION_WEIGHT[action] ?? 0
  const cot = isNum(cotScore) ? Math.min(cotScore, 10) * 2 : 0
  return Math.round(alignmentPass * 20 + w + cot)
}

function summaryCard(pillars, market) {
  const byId = Object.fromEntries(pillars.map((p) => [p.pillar, p]))
  const row = (pid) => ({
    state: byId[pid]?.state,
    score_display: byId[pid]?.score_display,
  })
  return {
    instrument_display: displayInstrumentName(market),
    valuation: row('valuation'),
    institutions: row('institutions'),
    retail: row('retail'),
    seasonality: row('seasonality'),
    location: row('location'),
  }
}

function scoringSnapFromThesis(thesis) {
  if (thesis?.confluenceRow) {
    return confluenceRowToScoringSnap(thesis.confluenceRow)
  }
  const snaps = thesis?.snapshots || []
  return snaps.length ? snaps[snaps.length - 1] : {}
}

export function buildOpportunity(thesis) {
  const snaps = thesis.snapshots || []
  const snap = scoringSnapFromThesis(thesis)
  const market = String(thesis.market || '').trim()
  const direction = effectiveDirection(thesis.direction_bias, snap)
  const trend = thesis.conviction_trend || computeTrend(snaps)
  const pillars = evaluatePillars(snap, direction)
  const align = alignmentSummary(pillars)
  const status = normStatus(thesis.status)
  const action = deriveAction({
    alignmentPass: align.pass,
    trend,
    status,
    archived: !!thesis.archived,
  })
  const why = pillars.map((p) => ({
    pillar: p.pillar,
    label: p.label,
    pass: p.pass,
    wired: p.wired,
    state: p.state,
    detail: p.one_line,
  }))
  const rank = rankScore(align.pass, action, snap.cot_score)
  const headline =
    action === ACTION_CLOSED
      ? `${displayInstrumentName(market)} — closed / not actionable.`
      : action === ACTION_NONE
        ? `${displayInstrumentName(market)} — ${align.label} alignment — no institutional edge.`
        : `${displayInstrumentName(market)} — ${align.label} alignment — ${action.toLowerCase()}.`

  return {
    alignment: { ...align, pillars },
    action,
    action_key: action.toLowerCase().replace(/ /g, '_'),
    rank_score: rank,
    direction,
    summary: summaryCard(pillars, market),
    why,
    headline,
  }
}

export function getOpportunity(thesis) {
  // Scoring from thesis.confluenceRow (confluence_history_latest.json); never thesis.opportunity.
  return buildOpportunity(thesis || {})
}
