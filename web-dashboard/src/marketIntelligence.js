/**
 * UI-only market context / intelligence helpers.
 * Uses existing confluence row fields — does not alter COT parsing, scoring, or history math.
 */

export const RELATED_SECTOR_PEERS = {
  Wheat: ['Corn', 'Soybeans'],
  Corn: ['Wheat', 'Soybeans'],
  Soybeans: ['Wheat', 'Corn'],
  'NASDAQ / NQ': ['S&P 500 / ES', 'Dow / YM'],
  'S&P 500 / ES': ['NASDAQ / NQ', 'Dow / YM'],
  'Dow / YM': ['NASDAQ / NQ', 'S&P 500 / ES'],
  Gold: ['Silver', 'Copper / HG'],
  Silver: ['Gold', 'Copper / HG'],
  'Copper / HG': ['Gold', 'Silver'],
  'Crude Oil / CL': ['Natural Gas / NG'],
  'Natural Gas / NG': ['Crude Oil / CL'],
  Coffee: ['Cocoa'],
  Cocoa: ['Coffee'],
}

const num = (v) => {
  const n = Number(v)
  return Number.isFinite(n) ? n : NaN
}

function netLean(net) {
  if (!Number.isFinite(net)) return 'unknown'
  if (net > 500) return 'long'
  if (net < -500) return 'short'
  return 'neutral'
}

function biasLean(bias) {
  const b = String(bias || '').toLowerCase()
  if (b.includes('bull')) return 'long'
  if (b.includes('bear')) return 'short'
  return 'neutral'
}

/** @param {string} focalMarket */
export function analyzeSectorPeers(focalMarket, focalRow, peersByMarket) {
  const peerNames = RELATED_SECTOR_PEERS[focalMarket] || []
  const warnings = []
  const notes = []
  let agree = 0
  let disagree = 0
  let unknown = 0
  const focalNet = num(focalRow?.net_value)
  const focalLean = netLean(focalNet)

  const inter = focalRow?.intermarket_impulse_context && typeof focalRow.intermarket_impulse_context === 'object'
    ? focalRow.intermarket_impulse_context
    : {}
  const conf = String(inter.intermarket_confirmation || '').toUpperCase()

  for (const name of peerNames) {
    const pr = peersByMarket?.[name]
    if (!pr) {
      unknown += 1
      continue
    }
    const pNet = num(pr.net_value)
    const peerLean = netLean(pNet)
    if (peerLean === 'unknown' || focalLean === 'unknown') {
      unknown += 1
      continue
    }
    if (peerLean === focalLean || (focalLean === 'neutral' || peerLean === 'neutral')) {
      agree += 1
    } else {
      disagree += 1
      warnings.push(
        `${name} managed-money net leans ${peerLean === 'long' ? 'long' : peerLean === 'short' ? 'short' : 'flat'} this week vs ${focalMarket} (${focalLean === 'long' ? 'long' : focalLean === 'short' ? 'short' : 'flat'}) — sector participation is not one clean story.`,
      )
    }
  }

  const sup = Array.isArray(inter.supporting_drivers) ? inter.supporting_drivers : []
  const con = Array.isArray(inter.conflicting_drivers) ? inter.conflicting_drivers : []
  for (const c of con) {
    const cs = String(c)
    if (peerNames.some((p) => cs.includes(p))) {
      notes.push(`Intermarket layer flags «${cs}» as conflicting — related-market confirmation is weak.`)
    }
  }
  if (conf === 'DIVERGING' || conf === 'WARNING') {
    notes.push(`Impulse read is ${conf} — treat related futures as sending different messages this week.`)
  }

  let alignmentScore = 5
  const denom = agree + disagree
  if (denom > 0) {
    alignmentScore += Math.round((agree / denom) * 4)
  }
  if (conf === 'CONFIRMING') alignmentScore += 2
  if (conf === 'MIXED') alignmentScore -= 1
  if (conf === 'DIVERGING' || conf === 'WARNING') alignmentScore -= 2
  if (disagree >= 2) alignmentScore -= 2
  alignmentScore = Math.max(0, Math.min(10, alignmentScore))

  let summary = ''
  if (!peerNames.length) {
    summary =
      'No fixed peer cluster is defined for this market in the dashboard map — use the intermarket drivers list below for cross-asset context.'
  } else if (!denom && unknown === peerNames.length) {
    summary = 'Related markets are not all present in this week’s loaded row set — sector confirmation cannot be scored here.'
  } else if (disagree === 0 && denom > 0) {
    summary = `${focalMarket} and tracked peers in this cluster show aligned net leaning this week — complex behaviour looks more one-sided on the positioning tape.`
  } else if (disagree > 0) {
    summary = `${focalMarket} positioning is not fully echoed by tracked peers (${peerNames.join(', ')}) — sector divergence can mean rotational flows, idiosyncratic supply/demand, or index vs single-name splits.`
  } else {
    summary = 'Peer alignment is inconclusive (mixed nets or missing peer rows) — lean on the impulse summary and catalyst list for the week.'
  }

  return {
    peerNames,
    alignmentScore,
    alignmentLabel: alignmentScore >= 7 ? 'Strong' : alignmentScore >= 5 ? 'Moderate' : 'Weak',
    divergenceWarnings: [...new Set(warnings)].slice(0, 4),
    sectorNotes: [...new Set(notes)].slice(0, 4),
    summary,
    intermarketConfirmation: conf || '—',
    supportingDrivers: sup,
    conflictingDrivers: con,
  }
}

const EVENT_HIGH_TERMS = [
  'CPI',
  'CORE PCE',
  'FOMC',
  'NFP',
  'NON-FARM',
  'PAYROLL',
  'USDA',
  'WASDE',
  'OPEC',
  'FED DECISION',
  'RATE DECISION',
  'GEOPOL',
  'BLACK SEA',
]
const EVENT_MED_TERMS = [
  'EIA',
  'INVENTOR',
  'GDP',
  'RETAIL SALES',
  'PMI',
  'ISM',
  'JACKSON',
  'POWELL',
  'TESTIMON',
  'DEBT CEILING',
  'EARNINGS',
  'BIG-TECH',
  'BIG TECH',
  'CROP REPORT',
]

export function assessEventRisk(row, pack) {
  const chunks = [
    row?.instrument_intel_context?.news_catalysts,
    row?.instrument_intel_context?.macro_impact,
    row?.next_data_watch,
    row?.flow_change_summary,
    Array.isArray(pack?.news_catalyst_bullets) ? pack.news_catalyst_bullets.join(' · ') : '',
    row?.macro_audit?.macro_rationale,
    row?.global_market_regime?.news_intensity,
  ]
    .filter((x) => x != null && String(x).trim() !== '')
    .join(' \n ')
  const upper = chunks.toUpperCase()
  const hits = []
  let level = 'low'
  for (const t of EVENT_HIGH_TERMS) {
    const u = t.toUpperCase()
    if (upper.includes(u)) {
      hits.push(t.trim())
      level = 'high'
    }
  }
  if (level === 'low') {
    for (const t of EVENT_MED_TERMS) {
      const u = t.toUpperCase()
      if (upper.includes(u)) {
        hits.push(t.trim())
        level = 'medium'
      }
    }
  }
  const uniq = [...new Set(hits)].slice(0, 8)
  const explain =
    level === 'high'
      ? 'Calendar / headline channels in this row mention high-impact release types (policy, employment, major supply reports, or geopolitical corridor risk). Expect prints to move implied vol and liquidity.'
      : level === 'medium'
        ? 'Some medium-impact macro or data channels appear in the text — not necessarily “event week,” but tape quality can be lumpier around releases.'
        : 'No strong scheduled-risk keywords were detected in embedded text — still use your own calendar; this scan is keyword-only, not exhaustive.'

  return { level, keywords: uniq, explain }
}

export function buildMacroMechanicsNarrative(row, globalRegime) {
  const g = globalRegime && typeof globalRegime === 'object' ? globalRegime : row?.global_market_regime
  const rm = row?.rates_macro && typeof row.rates_macro === 'object' ? row.rates_macro : null
  const mr = String(row?.macro_regime || '').replace(/_/g, ' ')
  const ms = num(row?.macro_score)
  const bullets = []

  if (rm) {
    const bias = displaySafe(rm.rates_bias)
    const curve = displaySafe(rm.curve_state)
    const sig = displaySafe(rm.macro_signal)
    bullets.push(
      `Rates snapshot: signal «${sig}», bias «${bias}», curve «${curve}». For risk assets and duration, falling yields often ease pressure; rising yields alongside restrictive language tends to tighten financial conditions.`,
    )
  } else if (g && hasText(g.rates_pressure)) {
    bullets.push(`Global rates pressure (week): ${clip(g.rates_pressure, 220)}`)
  }

  if (g && hasText(g.liquidity_regime)) {
    bullets.push(`Liquidity read: ${clip(g.liquidity_regime, 200)} Tighter liquidity often raises correlation and gap risk; easier liquidity can support carry and multiples — context for how violent mean-reversion might be.`)
  }

  if (hasText(mr) && mr.toUpperCase() !== 'N/A') {
    const align = Number.isFinite(ms) && ms >= 5
      ? 'macro filter reads supportive this week'
      : Number.isFinite(ms) && ms <= 3
        ? 'macro filter reads soft or conflicting this week'
        : 'macro filter is mid-range'
    bullets.push(`Macro label «${mr}» (${align}). This does not dictate entries — it tells you whether macro is broadly rowing with or against the positioning story this week.`)
  } else if (!bullets.length) {
    bullets.push('Macro regime fields are sparse on this row — interpret price vs positioning without a strong macro filter from this JSON snapshot.')
  }

  const conflict =
    Number.isFinite(ms) && ms <= 3 && num(row?.cot_score) >= 7
      ? 'Macro score is weak while COT conviction reads elevated — that mix often produces headline-driven chops: positioning can be “right” over a horizon but painful intraweek.'
      : String(row?.cot_bias || '').toLowerCase().includes('bull') && String(mr).toLowerCase().includes('risk_off')
        ? 'Positioning bias leans constructive while the macro label skews risk-off — a classic conflict regime: respect invalidations and event windows.'
        : null

  return { bullets: bullets.slice(0, 4), conflictNote: conflict }
}

function hasText(x) {
  return x != null && String(x).trim() !== '' && String(x).trim().toUpperCase() !== 'N/A'
}

function clip(s, n) {
  const t = String(s || '').trim()
  if (t.length <= n) return t
  return `${t.slice(0, n - 1)}…`
}

function displaySafe(v) {
  if (v == null || v === '') return '—'
  return String(v)
}

/**
 * @param {object} row
 * @param {{ participationCategory?: string, sectorDivergence?: boolean, pack?: object }} aux
 */
export function classifyMarketEnvironment(row, aux = {}) {
  const drivers = []
  const conflicts = []
  const ps = String(row?.positioning_state || '')
  const w1 = num(row?.one_week_net_change)
  const w4 = num(row?.four_week_net_change)
  const inter = row?.intermarket_impulse_context && typeof row.intermarket_impulse_context === 'object' ? row.intermarket_impulse_context : {}
  const conf = String(inter.intermarket_confirmation || '').toUpperCase()
  const impulse = num(inter.impulse_score)
  const sent = String(row?.instrument_intel_context?.sentiment_interference || '').toUpperCase()
  const netPct = num(row?.full_loaded_history_context?.current_net_percentile ?? row?.current_net_percentile)
  const cat = String(aux.participationCategory || '')
  const mr = String(row?.macro_regime || '').toLowerCase()
  const ms = num(row?.macro_score)
  const cot = num(row?.cot_score)
  const bias = String(row?.cot_bias || '').toLowerCase()

  const event = assessEventRisk(row, aux.pack || row?.ui_pack || {})

  let label = 'Low Conviction Environment'
  let blurb =
    'Positioning, macro impulse, and cross-market reads do not line up into a single clean story — that is normal; it argues for patience and location discipline rather than forcing a narrative.'

  if (event.level === 'high') {
    label = 'Event-Driven Volatility'
    blurb =
      'Keyword scan of embedded catalyst text suggests elevated scheduled / headline risk. Liquidity can thin around releases; weekly positioning can still whip even when the slower backdrop has not changed much.'
    drivers.push('Event / calendar keywords flagged in row text')
  } else if (cat === 'Participation Collapse') {
    label = 'Participation Collapse'
    blurb =
      'Both managed-money longs and shorts stepped down week-on-week — open interest is shrinking. That often means conviction cooling or risk being reduced; trend follow-through can weaken until participation returns.'
    drivers.push('Leg flow: two-sided contraction in the participation window')
  } else if (cat === 'Two-Way Expansion' && (conf === 'MIXED' || (Number.isFinite(impulse) && impulse <= 4))) {
    label = 'Rotational / Unstable'
    blurb =
      'Longs and shorts both grew — two-way open interest expansion — while impulse reads are soft or mixed. Tape quality tends to be choppier; breakouts are more often faded until a leader emerges.'
    drivers.push('Two-way OI expansion + mixed / weak impulse')
  } else if (Number.isFinite(netPct) && (netPct >= 92 || netPct <= 8) && (sent.includes('HIGH') || sent.includes('EXTREME'))) {
    label = 'Squeeze Risk'
    blurb =
      'Positioning is historically stretched and sentiment interference reads elevated. That does not time turns — it warns that positioning itself can become the volatility engine if a catalyst hits.'
    drivers.push('Historically stretched net positioning plus distortion language in text')
  } else if (
    (Number.isFinite(w1) && Number.isFinite(w4) && w1 * w4 < 0 && Math.abs(w1) > 2000)
    || (ps.toLowerCase().includes('weakening') && Number.isFinite(netPct) && (netPct >= 85 || netPct <= 15))
  ) {
    label = 'Exhaustion Risk'
    blurb =
      'Short-term net flow is fighting the four-week drift, or state language suggests a late-cycle lean. Trends can persist, but continuation quality is often lower — pay attention to failed breaks and violent mean-reversion.'
    drivers.push('1w vs 4w net tension or late-cycle state language')
  } else if ((mr.includes('risk_off') && bias.includes('bull')) || (Number.isFinite(ms) && ms <= 3 && Number.isFinite(cot) && cot >= 7)) {
    label = 'Macro Conflict'
    blurb =
      'Macro label or macro score is not supporting a strong COT read, or risk-off macro sits against a constructive positioning bias. This is a filter regime: good ideas need cleaner location and smaller size.'
    conflicts.push('Macro backdrop and positioning lean are pulling in different directions')
    drivers.push('Macro regime / score vs COT bias tension')
  } else if (aux.sectorDivergence) {
    label = 'Sector Divergence'
    blurb =
      'Tracked peers in the same sector bucket are not confirming the same net lean this week. One market can absolutely lead — but when the complex splits, false follow-through risk rises.'
    drivers.push('Peer net lean disagreement in this market group')
  } else if (Number.isFinite(w1) && Number.isFinite(w4) && w1 * w4 > 0 && Math.abs(w1) > Math.abs(w4) * 0.15 && Math.abs(w1) > 1500) {
    label = 'Momentum Expansion'
    blurb =
      'Recent weeks and the month-long drift point the same way with a meaningful weekly push — participation is reinforcing the existing lean (context only, not an entry trigger).'
    drivers.push('Aligned 1w / 4w net with sizeable weekly delta')
  } else if ((ps.includes('Strengthening') || ps.includes('Accumulation') || ps.includes('Distribution')) && conf === 'CONFIRMING') {
    label = 'Trend Continuation'
    blurb =
      'Positioning state reads directional and related markets scored as confirming this week. Continuation can still mean-revert on headlines — but alignment is cleaner than a full conflict tape.'
    drivers.push('Directional state + CONFIRMING intermarket')
  } else if (conf === 'CONFIRMING' && !sent.includes('HIGH') && !sent.includes('EXTREME') && mr.includes('risk_on')) {
    label = 'Clean Alignment'
    blurb =
      'Intermarket confirmation is constructive, macro label skews risk-on, and sentiment interference is not screaming distortion. This is what “cleaner backdrop” looks like on this dashboard — still not an entry trigger.'
    drivers.push('Confirming impulse + risk-on macro + calmer sentiment read')
  }

  return { label, blurb, drivers, conflicts }
}

export function computeTradeability(row, sector, eventRisk, env) {
  let score = 52
  const factors = []

  const cot = num(row?.cot_score)
  if (Number.isFinite(cot)) {
    if (cot >= 7) {
      score += 8
      factors.push('COT score reads high conviction on the engine scale')
    } else if (cot <= 3) {
      score -= 6
      factors.push('COT score is soft — positioning edge is not sharp this week')
    }
  }

  const conf = String(row?.intermarket_impulse_context?.intermarket_confirmation || '').toUpperCase()
  if (conf === 'CONFIRMING') {
    score += 7
    factors.push('Intermarket confirmation: CONFIRMING')
  } else if (conf === 'MIXED') {
    score -= 2
    factors.push('Intermarket: MIXED — expect more two-way behaviour')
  } else if (conf === 'DIVERGING' || conf === 'WARNING') {
    score -= 8
    factors.push('Intermarket: diverging / warning — cross-market disagreement')
  }

  if (sector.alignmentScore >= 7) {
    score += 5
    factors.push('Sector peer nets broadly agree this week')
  } else if (sector.divergenceWarnings.length >= 2) {
    score -= 7
    factors.push('Several peer divergences — grain / index complex not unified')
  } else if (sector.divergenceWarnings.length === 1) {
    score -= 3
    factors.push('At least one peer divergence flagged')
  }

  const ms = num(row?.macro_score)
  const bias = String(row?.cot_bias || '').toLowerCase()
  const mr = String(row?.macro_regime || '').toLowerCase()
  if (Number.isFinite(ms)) {
    if (ms >= 5 && ((mr.includes('risk_on') && bias.includes('bull')) || (mr.includes('risk_off') && bias.includes('bear')))) {
      score += 6
      factors.push('Macro score and bias direction broadly agree')
    } else if (ms <= 3) {
      score -= 5
      factors.push('Macro score is weak — macro filter is not supportive')
    }
  }

  if (eventRisk.level === 'high') {
    score -= 12
    factors.push('Elevated event-risk keywords in embedded text')
  } else if (eventRisk.level === 'medium') {
    score -= 4
    factors.push('Some medium-impact calendar keywords detected')
  }

  const sent = String(row?.instrument_intel_context?.sentiment_interference || '').toUpperCase()
  if (sent.includes('EXTREME')) {
    score -= 10
    factors.push('Sentiment interference reads extreme — narrative risk')
  } else if (sent.includes('HIGH')) {
    score -= 5
    factors.push('Sentiment interference reads high')
  }

  if (env.label === 'Clean Alignment' || env.label === 'Trend Continuation') score += 4
  if (env.label === 'Macro Conflict' || env.label === 'Rotational / Unstable') score -= 5
  if (env.label === 'Event-Driven Volatility') score -= 4

  score = Math.max(8, Math.min(96, Math.round(score)))

  let quality = 'Mixed Conditions'
  if (eventRisk.level === 'high' && score < 60) quality = 'Event-Driven Noise'
  else if (score >= 74) quality = 'High Quality'
  else if (score >= 58) quality = 'Mixed Conditions'
  else if (score >= 44) quality = 'Low Clarity'
  else if (score < 36) quality = 'Avoid This Week'

  return { score, quality, factors: factors.slice(0, 7) }
}

export function buildIntelligenceBriefing(row, pack, sector, eventRisk, env, trade, macroN) {
  const parts = []
  parts.push(`Environment: ${env.label}. ${env.blurb}`)
  if (sector.summary) parts.push(`Related markets: ${sector.summary}`)
  if (sector.divergenceWarnings.length) {
    parts.push(`Divergence: ${sector.divergenceWarnings[0]}`)
  }
  parts.push(`Readiness (${trade.score}/96): ${trade.quality}. Heuristic only — not a signal. It summarises clarity, cross-market confirmation, macro filter strength, event noise, and sentiment interference.`)
  if (macroN.conflictNote) parts.push(macroN.conflictNote)
  if (eventRisk.level !== 'low') parts.push(`Event risk scan: ${eventRisk.level}. ${eventRisk.explain}`)
  const intel = row?.instrument_intel_context?.final_context_summary
  if (hasText(intel)) {
    parts.push(`Row narrative (embedded): ${clip(intel, 320)}`)
  } else if (hasText(row?.final_context_reason)) {
    parts.push(`Confluence note: ${clip(row.final_context_reason, 280)}`)
  }
  return parts.join('\n\n')
}

/** Single call-site bundle for the dashboard context engine (interpretation only). */
export function computeInstrumentIntelligence(row, pack, peersByMarket, globalMarketRegime, latestParticipation) {
  const p = pack || row?.ui_pack || {}
  const sector = analyzeSectorPeers(row?.market, row, peersByMarket || {})
  const eventRisk = assessEventRisk(row, p)
  const env = classifyMarketEnvironment(row, {
    participationCategory: latestParticipation?.category,
    sectorDivergence: sector.divergenceWarnings.length > 0,
    pack: p,
  })
  const macroN = buildMacroMechanicsNarrative(row, globalMarketRegime)
  const trade = computeTradeability(row, sector, eventRisk, env)
  const briefing = buildIntelligenceBriefing(row, p, sector, eventRisk, env, trade, macroN)
  return { sector, eventRisk, env, macroN, trade, briefing }
}
