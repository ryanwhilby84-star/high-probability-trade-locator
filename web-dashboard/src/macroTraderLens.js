/**
 * Presentation-only: maps existing row / ui_pack / intelligence into a fast trader read.
 * Does not change COT, scores, or backend fields.
 */

import { computeInstrumentIntelligence } from './marketIntelligence.js'

const num = (v) => {
  const n = Number(v)
  return Number.isFinite(n) ? n : NaN
}

function intermarketWord(row) {
  const conf = String(row?.intermarket_impulse_context?.intermarket_confirmation || '').toUpperCase()
  if (conf === 'CONFIRMING') return { value: 'Confirming', tone: 'good' }
  if (conf === 'MIXED') return { value: 'Mixed', tone: 'mid' }
  if (conf === 'DIVERGING' || conf === 'WARNING') return { value: 'Diverging', tone: 'warn' }
  if (conf && conf !== '—') return { value: conf.charAt(0) + conf.slice(1).toLowerCase(), tone: 'mid' }
  return { value: 'Unknown', tone: 'neutral' }
}

function environmentFromPack(pack) {
  const m = String(pack?.executive?.macro || '').trim()
  if (!m || m === 'Macro unavailable') return { value: 'Unknown', tone: 'neutral' }
  if (m === 'Supportive') return { value: 'Supportive', tone: 'good' }
  if (m === 'Restrictive') return { value: 'Restrictive', tone: 'warn' }
  return { value: 'Mixed', tone: 'mid' }
}

function positioningCard(row) {
  const ps = String(row?.positioning_state || '').trim()
  const v = ps && ps.toUpperCase() !== 'N/A' ? ps : String(row?.cot_bias || '—').trim() || '—'
  const lower = v.toLowerCase()
  let tone = 'mid'
  if (/strengthening|accumulation/i.test(lower) && !/bear|short/i.test(lower)) tone = 'good'
  else if (/strengthening|distribution/i.test(lower) && /bear|short/i.test(lower)) tone = 'warn'
  else if (/weakening|covering|improving|softening|transition/i.test(lower)) tone = 'mid'
  return { value: v, tone }
}

function macroAlignmentCard(row) {
  const mr = String(row?.macro_regime || '').toLowerCase()
  const bias = String(row?.cot_bias || '').toLowerCase()
  const ms = num(row?.macro_score)
  const cot = num(row?.cot_score)
  if (mr.includes('risk_off') && bias.includes('bull')) return { value: 'Fighting', tone: 'warn' }
  if (mr.includes('risk_on') && bias.includes('bear')) return { value: 'Fighting', tone: 'warn' }
  if (Number.isFinite(ms) && ms <= 3 && Number.isFinite(cot) && cot >= 7) return { value: 'Fighting', tone: 'warn' }
  if (mr.includes('risk_on') && bias.includes('bull')) return { value: 'Helping', tone: 'good' }
  if (mr.includes('risk_off') && bias.includes('bear')) return { value: 'Helping', tone: 'good' }
  if (Number.isFinite(ms) && ms >= 5 && ((bias.includes('bull') && !mr.includes('risk_off')) || (bias.includes('bear') && mr.includes('risk_off')))) {
    return { value: 'Aligned', tone: 'good' }
  }
  if (!mr || mr === 'n/a') return { value: 'Unclear', tone: 'neutral' }
  return { value: 'Mixed', tone: 'mid' }
}

function stabilityFromTrade(trade, eventRisk) {
  const q = String(trade?.quality || '')
  if (eventRisk?.level === 'high' && (q === 'Event-Driven Noise' || q === 'Low Clarity')) return { value: 'Fragile', tone: 'warn' }
  if (q === 'High Quality') return { value: 'Stable', tone: 'good' }
  if (q === 'Mixed Conditions') return { value: 'Mixed', tone: 'mid' }
  if (q === 'Low Clarity') return { value: 'Fragile', tone: 'warn' }
  if (q === 'Event-Driven Noise') return { value: 'Fragile', tone: 'warn' }
  if (q === 'Avoid This Week') return { value: 'Fragile', tone: 'bad' }
  return { value: 'Mixed', tone: 'mid' }
}

function eventRiskCard(eventRisk) {
  const lv = String(eventRisk?.level || 'low')
  if (lv === 'high') return { value: 'Elevated', tone: 'warn' }
  if (lv === 'medium') return { value: 'Moderate', tone: 'mid' }
  return { value: 'Subdued', tone: 'good' }
}

function tradeEnvironmentLine(trade, eventRisk) {
  const q = String(trade?.quality || '')
  if (q === 'Avoid This Week') return { value: 'Poor — wait for cleaner conditions', tone: 'bad' }
  if (q === 'Event-Driven Noise') return { value: 'Tradable but unstable', tone: 'warn' }
  if (q === 'Low Clarity') return { value: 'Choppy — size down', tone: 'warn' }
  if (q === 'Mixed Conditions') return { value: 'Tradable with noise', tone: 'mid' }
  if (q === 'High Quality') return { value: 'Favorable backdrop', tone: 'good' }
  if (eventRisk?.level === 'high') return { value: 'Tradable but event-heavy', tone: 'warn' }
  return { value: 'Mixed — use discretion', tone: 'mid' }
}

function buildContextBrief(intel, pack, row, relationshipMapData) {
  const env = environmentFromPack(pack)
  const pos = positioningCard(row)
  const align = macroAlignmentCard(row)
  const { trade, eventRisk, env: mEnv } = intel
  const parts = []

  let s1 = ''
  if (env.value === 'Unknown') {
    s1 = `Macro backdrop is thin on this row; positioning reads ${pos.value}.`
  } else {
    s1 = `The week reads ${env.value.toLowerCase()} for broad risk assets. Positioning: ${pos.value}.`
  }
  parts.push(s1)

  if (align.value === 'Fighting') {
    parts.push('Macro and positioning are not fully on the same page — be selective on entries and respect event windows.')
  } else if (align.value === 'Helping' || align.value === 'Aligned') {
    parts.push('Macro broadly supports the positioning lean — still not a signal, but friction is lower.')
  } else {
    parts.push('Macro vs positioning is mixed — lean on location and your own risk rules.')
  }

  if (eventRisk.level === 'high') {
    parts.push('Headline / data risk looks elevated in the embedded text scan — expect lumpier tape around releases.')
  } else if (eventRisk.level === 'medium') {
    parts.push('Some second-tier data or headlines may add noise this week.')
  }

  if (row?.market === 'NASDAQ / NQ' && relationshipMapData?.available && relationshipMapData.driver_id === 'dgs10') {
    const r20 = relationshipMapData.latest_rolling_corr_20
    if (r20 != null && Number.isFinite(Number(r20))) {
      parts.push(`Live NQ vs 10Y window: short-term rolling correlation ≈ ${Number(r20).toFixed(2)} (see chart).`)
    }
  }

  return parts.slice(0, 3).join(' ')
}

/**
 * @param {object} row
 * @param {object} pack — ui_pack or fallback
 * @param {object} peersByMarket
 * @param {object|null} globalMarketRegime
 * @param {object|null} latestParticipation
 * @param {object|null} relationshipMapData — optional NASDAQ live map
 */
export function buildMacroTraderLens(row, pack, peersByMarket, globalMarketRegime, latestParticipation, relationshipMapData = null) {
  const p = pack || row?.ui_pack || {}
  const intel = computeInstrumentIntelligence(row, p, peersByMarket || {}, globalMarketRegime, latestParticipation || null)
  const envCard = environmentFromPack(p)
  const posCard = positioningCard(row)
  const alignCard = macroAlignmentCard(row)
  const stabCard = stabilityFromTrade(intel.trade, intel.eventRisk)
  const evtCard = eventRiskCard(intel.eventRisk)
  const imCard = intermarketWord(row)
  const tradeCard = tradeEnvironmentLine(intel.trade, intel.eventRisk)

  const cards = [
    { id: 'environment', label: 'Environment', ...envCard },
    { id: 'positioning', label: 'Positioning', ...posCard },
    { id: 'macro_alignment', label: 'Macro alignment', ...alignCard },
    { id: 'stability', label: 'Stability', ...stabCard },
    { id: 'event_risk', label: 'Event risk', ...evtCard },
    { id: 'intermarket', label: 'Intermarket', ...imCard },
    { id: 'trade_env', label: 'Trade environment', ...tradeCard },
  ]

  const contextBrief = buildContextBrief(intel, p, row, relationshipMapData)

  return { cards, contextBrief, _intel: intel }
}
