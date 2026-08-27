/**
 * Macro / backdrop interpretation for the dashboard only.
 * Does not modify COT or backend scoring; does not emit trade signals.
 */

import { assessEventRisk } from './marketIntelligence.js'

const num = (v) => {
  const n = Number(v)
  return Number.isFinite(n) ? n : NaN
}

const has = (x) => x != null && String(x).trim() !== '' && String(x).trim().toUpperCase() !== 'N/A'
const low = (s) => String(s || '').toLowerCase()

function clip(s, n) {
  const t = String(s || '').trim()
  if (t.length <= n) return t
  return `${t.slice(0, n - 1)}…`
}

/** Institutional-style “what matters” per tracked market — guides copy, not data generation. */
export const MARKET_MACRO_PROFILES = {
  'NASDAQ / NQ': {
    headline: 'Duration & liquidity complex',
    channels: [
      'Front-end yields (2Y) and the path of Fed expectations often dominate multiples and high-duration growth.',
      '10Y–30Y shape matters for discount-rate narratives; curve inflections can shift “growth vs safety” tone faster than single prints.',
      'Broad USD (DXY when wired) affects overseas earnings translation and global liquidity recycling — cohort positioning rarely ignores it.',
      'Liquidity / financial conditions: when conditions tighten, correlation rises and gap risk tends to increase even without a COT regime change.',
    ],
  },
  'S&P 500 / ES': {
    headline: 'Policy rate, curve, and broad risk appetite',
    channels: [
      'Index futures aggregate macro: rates, earnings expectations, and cross-asset flows show up as impulse more than single-name idiosyncrasy.',
      'Flattening / inversion episodes historically coincide with higher drawdown volatility for broad beta.',
      'When macro_score is soft but COT conviction is high, tape quality is often headline-driven — not “wrong,” but harder to execute cleanly.',
    ],
  },
  'Dow / YM': {
    headline: 'Cyclical / value tilt vs pure duration',
    channels: [
      'Cyclical industrials and banks in the Dow can respond differently to curve steepening than pure growth-duration names.',
      'Use the same rates snapshot as NQ/ES, but interpret through “reflation vs slowdown” language in flows rather than a single beta call.',
    ],
  },
  Gold: {
    headline: 'Real yields, USD, and stress hedging',
    channels: [
      'Gold often keys off real-rate expectations and USD competitiveness — when real yields rise, non-yielding bullion can face headwinds absent a separate stress bid.',
      'Safe-haven demand can temporarily dominate macro: geopolitical shocks can decouple week-to-week positioning from the rates layer.',
      'Inflation persistence narratives matter for whether gold is treated as a hedge or as a liquidity sink.',
    ],
  },
  Silver: {
    headline: 'Hybrid: precious metal + industrial beta',
    channels: [
      'Shares gold’s sensitivity to real yields and USD, but often carries more industrial / growth cyclicality than gold alone.',
      'Cross-read copper and risk appetite when silver diverges from gold — the cohort story may be “dual use,” not a single theme.',
    ],
  },
  'Copper / HG': {
    headline: 'Growth impulse & China / construction channel',
    channels: [
      'Copper is often read as a check on global industrial demand — macro slowdown language in rates/liquidity can weigh even when micro supply is tight.',
      'Watch USD and freight / inventory narratives in headlines; positioning can move on growth revisions faster than visible inventories.',
    ],
  },
  'Crude Oil / CL': {
    headline: 'Balances, policy, and geopolitical risk',
    channels: [
      'OPEC+ guidance and inventory surprises can dominate a week even when broad macro is quiet — event sensitivity is structurally high.',
      'Global growth expectations and the USD frame export demand and affordability for importers.',
      'Geopolitical supply risk can reprice risk premia quickly; positioning squeeze risk rises when shorts cluster into known catalyst windows.',
    ],
  },
  'Natural Gas / NG': {
    headline: 'Weather, storage, LNG, and seasonal demand',
    channels: [
      'Henry Hub often trades storage trajectory vs heating/cooling degree expectations — EIA prints can move implied vol sharply.',
      'LNG / export demand and supply disruptions can override a quiet macro week for broader risk assets.',
      'Elevated short interest in the cohort plus a volatile inventory path is a classic squeeze-prone setup on data — not a forecast, a fragility note.',
    ],
  },
  Wheat: {
    headline: 'Balance sheet, weather, and export corridors',
    channels: [
      'USDA / WASDE and global balance-sheet surprises can reprice the entire grain complex even when rates are stable.',
      'Black Sea / export corridor headlines are first-order for wheat; treat macro rates as background unless financial conditions seize up.',
      'Weather and crop-condition narratives interact with seasonal cycles — positioning often leads headlines into report weeks.',
    ],
  },
  Corn: {
    headline: 'Ethanol, feed demand, and South Hemisphere crop risk',
    channels: [
      'Energy policy (ethanol) and livestock/feed demand tie corn to macro in a different way than wheat’s export corridor emphasis.',
      'Brazil/Argentina weather can move corn independently of SRW wheat — sector divergence is common, not anomalous.',
    ],
  },
  Soybeans: {
    headline: 'China import pace, oil share, and South America supply',
    channels: [
      'Soy often carries China-demand and oilseed share narratives; check whether macro risk-off is coming with commodity-specific demand hits or broad de-risking only.',
      'USDA and crop tours matter; USD level frames export competitiveness.',
    ],
  },
  Coffee: {
    headline: 'Weather, crop risk, and FX in producers',
    channels: [
      'Softs can decouple from US rates for stretches — frost/drought and producer-currency moves often dominate a single week.',
      'Liquidity still matters for margin and carry when volatility spikes.',
    ],
  },
  Cocoa: {
    headline: 'Supply disruptions and origin politics',
    channels: [
      'Cocoa supply is geographically concentrated — political and port logistics headlines can swamp generic risk-on/risk-off labels short term.',
      'Watch whether global liquidity stress shows up as financing / warehouse constraints rather than only yields.',
    ],
  },
}

export function getMarketMacroProfile(market) {
  return MARKET_MACRO_PROFILES[market] || {
    headline: 'Multi-factor macro intersection',
    channels: [
      'Use the rates snapshot and global regime lines as the generic financial-conditions filter.',
      'Read the instrument intel “macro impact” block for this contract’s documented sensitivities.',
    ],
  }
}

function resolvedRates(row, globalRegime) {
  const rm = row?.rates_macro && typeof row.rates_macro === 'object' ? row.rates_macro : null
  const audit = row?.macro_audit && typeof row.macro_audit === 'object' && row.macro_audit.available !== false ? row.macro_audit : null
  const rr = audit?.resolved_regime && typeof audit.resolved_regime === 'object' ? audit.resolved_regime : null
  const g = globalRegime && typeof globalRegime === 'object' ? globalRegime : row?.global_market_regime
  const sig = String(rr?.macro_signal || rm?.macro_signal || g?.resolved_macro_signal || row?.macro_regime || '').toLowerCase()
  const liq = low(rr?.liquidity_regime || rm?.liquidity_regime || g?.liquidity_regime)
  const infl = low(g?.inflation_regime || '')
  const ratesBias = low(rr?.rates_bias || rm?.rates_bias || '')
  const policy = low(rr?.policy_pressure || rm?.policy_pressure || '')
  const curve = String(rr?.curve_state || rr?.curve_context || rm?.curve_state || g?.curve_state || '')
  return { rm, audit, rr, g, sig, liq, infl, ratesBias, policy, curve }
}

/**
 * Macro regime classification — interpretive labels on top of existing JSON signals.
 */
export function classifyMacroRegimeState(row, globalRegime, eventRisk) {
  const { sig, liq, infl, ratesBias, policy, g, curve } = resolvedRates(row, globalRegime)
  const ms = num(row?.macro_score)
  const cot = num(row?.cot_score)
  const rationale = []
  const tags = []

  let primary = 'Macro backdrop indeterminate'
  let secondary = null

  const restrictiveLiq = /tight|restrict|drain|pressure|harsher|fragile/i.test(liq)
  const easyLiq = /ease|support|ample|accommod|inject/i.test(liq)

  if (eventRisk?.level === 'high') {
    primary = 'Event-Driven Volatility'
    rationale.push('Embedded catalyst text flags high-impact release types; macro “regime” can be temporarily overshadowed by positioning into/ out of prints.')
    tags.push('event-heavy')
  }

  if (restrictiveLiq && !eventRisk?.level === 'high') {
    /* eslint-disable no-self-compare */ /* keep structure explicit */
  }
  if (restrictiveLiq && primary === 'Macro backdrop indeterminate') {
    primary = 'Restrictive Liquidity'
    rationale.push('Liquidity language skews restrictive — financial conditions may punish leverage and lengthen correlation spikes across risk assets.')
    tags.push('liquidity-tight')
  } else if (easyLiq && sig.includes('risk_on')) {
    if (primary === 'Macro backdrop indeterminate') primary = 'Liquidity Expansion'
    rationale.push('Liquidity reads supportive alongside a constructive rates snapshot — carry and multiples often get a friendlier default volatility backdrop (not a guarantee).')
    tags.push('liquidity-easy')
  }

  if (/persist|sticky|slow/i.test(infl)) {
    secondary = secondary || 'Inflation Persistence'
    rationale.push('Inflation narrative language mentions persistence — markets may reprice terminal policy and curve in steps rather than smooth trends.')
    tags.push('inflation-narrative')
  }

  if (sig.includes('risk_off') || ratesBias.includes('bear')) {
    if (primary === 'Macro backdrop indeterminate' || primary === 'Liquidity Expansion') {
      primary = 'Risk-Off Pressure'
    }
    rationale.push('Resolved / rates signal skews risk-off or bearish rates bias — duration and beta often face a higher hurdle rate for narrative follow-through.')
    tags.push('risk-off')
  } else if (sig.includes('risk_on') && ratesBias.includes('bull')) {
    if (primary === 'Macro backdrop indeterminate') primary = 'Risk-On Expansion'
    rationale.push('Risk-on alignment in the rates snapshot — macro filter is supportive for typical risk-beta framing this week.')
    tags.push('risk-on')
  }

  if (/slow|recession|downshift|weak growth/i.test(low(g?.risk_regime || ''))) {
    secondary = secondary || 'Growth Slowdown'
    rationale.push('Global risk-regime copy references growth slowdown — cyclical commodities and earnings beta can decouple from a single headline index move.')
    tags.push('growth-slow')
  }

  if (policy.includes('restrict')) {
    rationale.push('Policy pressure reads restrictive — even with mixed curves, funding stress and guidance surprises can dominate quiet fundamental weeks.')
    tags.push('policy-tight')
  }

  if (/steep|bear steep|reflat/i.test(low(curve))) {
    rationale.push(`Curve language (${clip(curve, 80)}) often interacts with bank/cyclical narratives vs pure duration — check whether your market is beta to that channel.`)
    tags.push('curve-story')
  }

  if (Number.isFinite(ms) && ms <= 3 && Number.isFinite(cot) && cot >= 7) {
    secondary = secondary || 'Macro Conflict'
    rationale.push('Dashboard macro_score is soft while COT conviction reads elevated — a classic “filter vs positioning” tension: trends can continue on positioning alone while macro disagrees.')
    tags.push('macro-cot-tension')
  }

  if (rationale.length === 0) {
    rationale.push('Macro fields are sparse or mixed — treat the rates audit block (when available) as the authoritative bridge; avoid forcing a single regime label.')
  }

  return { primary, secondary, rationale: rationale.slice(0, 6), tags: [...new Set(tags)] }
}

/** Heuristic “tape language” vs macro backdrop — uses confluence text only (no OHLC). */
export function interpretMacroVsTapeTension(row, participationCategory) {
  const mr = low(row?.macro_regime)
  const ms = num(row?.macro_score)
  const corpus = [
    row?.institutional_flow_summary,
    row?.flow_change_summary,
    row?.zone_focus,
    row?.technical_action_note,
    row?.setup_type,
    row?.final_context,
  ]
    .filter(has)
    .join(' ')
  const c = low(corpus)

  const tapeConstructive =
    /(rally|rip|demand|covering|lift|ease|improv|accumul|constructive|less bearish|supportive|bid|reversal|tightening shorts)/i.test(corpus)
  const tapeDefensive =
    /(supply|breakdown|deterior|rollover|distribution|vulnerable|fade|pressure|heavy short|bearish lean|sell)/i.test(corpus)

  const macroRestrictive = mr.includes('risk_off') || (Number.isFinite(ms) && ms <= 3) || /restrict|tighten|bearish rate|elevated yield/i.test(corpus)
  const macroSupportive = mr.includes('risk_on') || (Number.isFinite(ms) && ms >= 6)

  const lines = []
  let label = 'Macro–tape relationship: unclear'

  if (macroRestrictive && tapeConstructive) {
    label = 'Momentum despite restriction (plausible)'
    lines.push(
      `${clip(row?.market || 'This market', 32)}: flow and decision-layer language sounds constructive or demand-leaning while the macro filter reads restrictive or soft on the dashboard bridge.`,
    )
    lines.push(
      'That combination often appears when positioning rotation, short-covering, or forward repricing of policy runs ahead of the slow macro score — it can also reflect squeeze-style mechanics into catalysts. It is not evidence that macro “stopped mattering.”',
    )
  } else if (macroSupportive && tapeDefensive) {
    label = 'Soft tape response despite supportive macro (plausible)'
    lines.push(
      'Macro labels read supportive, but flow / zone language skews cautious or supply-leaning — participants may be prioritizing location, event risk, or micro balance sheets over the generic risk-on tag.',
    )
    lines.push(
      'This is a useful conflict flag: execution quality often depends on whether the disconnect resolves via data or via price cleaning out one side of positioning.',
    )
  } else if (macroRestrictive && tapeDefensive) {
    label = 'Aligned stress (macro and tape both cautious)'
    lines.push(
      'Macro filter and positioning-flow language both skew cautious — trend continuation, if any, may rely more on idiosyncratic catalysts than broad beta tailwinds.',
    )
  } else if (macroSupportive && tapeConstructive) {
    label = 'Broad alignment (macro and tape language agree)'
    lines.push(
      'Macro filter and positioning-flow language broadly agree — this is what “cleaner backdrop” tends to look like in text, not proof of direction.',
    )
  }

  if (participationCategory === 'Two-Way Expansion' || participationCategory === 'Participation Collapse') {
    lines.push(
      `Participation flow reads «${participationCategory}» — open-interest dynamics can add chop even when macro and positioning bias agree on direction.`,
    )
  }

  if (!lines.length) {
    lines.push(
      'Not enough consistent language in flow / zone fields to classify a macro–tape tension — rely on rates audit, intermarket, and your price chart for discretion.',
    )
  }

  return { label, lines: lines.slice(0, 5) }
}

const EVENT_TYPES = [
  { id: 'cpi', match: ['CPI', 'CORE PCE', 'PCE', 'INFLATION PRINT'], vol: 'high', sensitivity: 'Rates, USD, and index futures often reprice sharply; commodities can move on implied real-rate shocks.' },
  { id: 'fomc', match: ['FOMC', 'FED DECISION', 'RATE DECISION', 'DOT PLOT'], vol: 'high', sensitivity: 'Front-end yields and volatility dominate cross-asset beta for the session window.' },
  { id: 'nfp', match: ['NFP', 'NON-FARM', 'PAYROLL', 'JOBS REPORT'], vol: 'high', sensitivity: 'Labor strength feeds curve and Fed-path repricing — growth and duration trade together intraday.' },
  { id: 'eia', match: ['EIA', 'INVENTOR', 'STORAGE'], vol: 'high', sensitivity: 'Energy complexes and sometimes refined products — positioning into the print can squeeze both ways.' },
  { id: 'usda', match: ['USDA', 'WASDE', 'CROP REPORT'], vol: 'high', sensitivity: 'Grains and oilseeds: balance-sheet surprises can move the entire complex; check peer divergence after the headline.' },
  { id: 'opec', match: ['OPEC'], vol: 'high', sensitivity: 'Crude: quota and compliance language can reprice risk premia independent of US rates.' },
  { id: 'earnings', match: ['EARNINGS', 'BIG-TECH', 'GUIDANCE'], vol: 'medium', sensitivity: 'Index futures: idiosyncratic megacap prints can dominate a macro-quiet tape.' },
  { id: 'geo', match: ['GEOPOL', 'BLACK SEA', 'WAR', 'SANCTION'], vol: 'high', sensitivity: 'Wheat/energy safe-haven channels can activate; correlation regime may spike.' },
]

function marketRelevanceLine(market, eventId) {
  const m = market || ''
  if (eventId === 'eia' && (m.includes('Natural Gas') || m.includes('Crude'))) {
    return 'Direct inventory path — positioning into the release often raises squeeze risk if consensus is one-sided.'
  }
  if (eventId === 'usda' && /Wheat|Corn|Soybeans/.test(m)) {
    return 'Direct balance-sheet relevance for this contract — watch cross-grain confirmation after the headline.'
  }
  if (eventId === 'cpi' || eventId === 'fomc' || eventId === 'nfp') {
    if (m.includes('NASDAQ') || m.includes('S&P') || m.includes('Dow')) return 'High beta to policy path repricing and duration.'
    if (m === 'Gold' || m.includes('Silver')) return 'Real-yield and USD repricing channel; can move even when equity beta is quiet.'
  }
  if (eventId === 'opec' && m.includes('Crude')) return 'First-order for crude; NG may only echo via broad energy sentiment unless LNG headlines overlap.'
  if (eventId === 'geo' && m.includes('Wheat')) return 'Export corridor narratives are first-order for wheat — can dominate a single week.'
  return 'Relevance depends on whether your book is beta to the channel named in the headline — treat as elevated noise risk, not a directional call.'
}

export function buildExpandedEventIntel(market, row, pack) {
  const base = assessEventRisk(row, pack || row?.ui_pack || {})
  const upper = [
    row?.instrument_intel_context?.news_catalysts,
    row?.instrument_intel_context?.macro_impact,
    row?.next_data_watch,
    Array.isArray(pack?.news_catalyst_bullets) ? pack.news_catalyst_bullets.join(' | ') : '',
  ]
    .filter(has)
    .join(' | ')
    .toUpperCase()

  const matchedEvents = []
  for (const ev of EVENT_TYPES) {
    if (ev.match.some((k) => upper.includes(k))) {
      matchedEvents.push({
        ...ev,
        relevance: marketRelevanceLine(market, ev.id),
        instability: ev.vol === 'high' ? 'Print windows often raise gap risk and thinner liquidity — conviction can evaporate quickly after the headline.' : 'Secondary volatility — still respect stops and calendar.',
      })
    }
  }

  const uniq = []
  const seen = new Set()
  for (const e of matchedEvents) {
    if (seen.has(e.id)) continue
    seen.add(e.id)
    uniq.push(e)
  }

  return {
    ...base,
    structuredEvents: uniq.slice(0, 8),
    marketEventSummary:
      uniq.length === 0
        ? 'No structured event tags beyond the keyword scan — still merge with your own economic calendar.'
        : `This row’s text intersects: ${uniq.map((e) => e.match[0]).join(', ')}. Volatility risk is skewed ${uniq.some((e) => e.vol === 'high') ? 'higher' : 'moderate'} into those channels for ${market || 'this contract'}.`,
  }
}

/**
 * Interpretive macro conviction index (0–100) — separate from backend macro_score; describes narrative quality / stability.
 */
export function computeMacroInterpretationConviction(row, globalRegime, participationCategory) {
  const { rm, rr, g } = resolvedRates(row, globalRegime)
  const ms = num(row?.macro_score)
  const inter = row?.intermarket_impulse_context && typeof row.intermarket_impulse_context === 'object' ? row.intermarket_impulse_context : {}
  const conf = String(inter.intermarket_confirmation || '').toUpperCase()
  const event = assessEventRisk(row, row?.ui_pack || {})

  const dAlignment = (() => {
    let s = 6
    if (Number.isFinite(ms)) {
      if (ms >= 6) s += 2
      if (ms <= 3) s -= 2
    }
    if (conf === 'CONFIRMING') s += 2
    if (conf === 'DIVERGING' || conf === 'WARNING') s -= 3
    return Math.max(0, Math.min(10, s))
  })()

  const dStability = (() => {
    let s = 6
    if (conf === 'MIXED') s -= 1
    if (event.level === 'high') s -= 3
    if (event.level === 'medium') s -= 1
    if (has(rr?.macro_rationale || rm?.macro_rationale || g?.macro_rationale)) s += 1
    return Math.max(0, Math.min(10, s))
  })()

  const dConflict = (() => {
    let s = 3
    const t = interpretMacroVsTapeTension(row, participationCategory)
    if (t.label.includes('despite')) s += 3
    if (Number.isFinite(ms) && ms <= 3 && num(row?.cot_score) >= 7) s += 3
    return Math.max(0, Math.min(10, s))
  })()

  const dEventDistortion = (() => {
    let s = event.level === 'high' ? 8 : event.level === 'medium' ? 5 : 2
    return Math.max(0, Math.min(10, s))
  })()

  const dSensitivity = (() => {
    const prof = getMarketMacroProfile(row?.market)
    const base = Math.min(10, 6 + Math.floor((prof.channels?.length || 0) / 2))
    return Math.max(5, Math.min(10, base))
  })()

  const dPersistence = (() => {
    const w1 = num(row?.one_week_net_change)
    const w4 = num(row?.four_week_net_change)
    let s = 5
    if (Number.isFinite(w1) && Number.isFinite(w4) && w1 * w4 > 0) s += 3
    if (Number.isFinite(w1) && Number.isFinite(w4) && w1 * w4 < 0) s -= 2
    return Math.max(0, Math.min(10, s))
  })()

  const dims = {
    alignment: dAlignment,
    stability: dStability,
    conflictPressure: dConflict,
    eventDistortion: dEventDistortion,
    channelSensitivity: dSensitivity,
    flowPersistence: dPersistence,
  }
  const overall = Math.round(
    (dims.alignment * 0.22 + dims.stability * 0.2 + (10 - dims.conflictPressure) * 0.18 + (10 - dims.eventDistortion) * 0.12 + dims.channelSensitivity * 0.14 + dims.flowPersistence * 0.14) * 10,
  )

  let band = 'Balanced / nuanced'
  if (overall >= 72) band = 'High structural clarity'
  else if (overall >= 55) band = 'Moderate clarity'
  else if (overall >= 42) band = 'Fragile / conflicted'
  else band = 'Low clarity — elevated noise'

  return {
    overall: Math.max(8, Math.min(96, overall)),
    band,
    dimensions: dims,
    explain:
      'Interpretation index only: higher means clearer cross-layer agreement and calmer event distortion in text — not “macro is bullish.”',
  }
}

export function buildMacroInstitutionalBriefing(market, row, pack, globalRegime, participationCategory) {
  const profile = getMarketMacroProfile(market)
  const regime = classifyMacroRegimeState(row, globalRegime, assessEventRisk(row, pack || row?.ui_pack || {}))
  const tension = interpretMacroVsTapeTension(row, participationCategory)
  const events = buildExpandedEventIntel(market, row, pack)
  const conv = computeMacroInterpretationConviction(row, globalRegime, participationCategory)
  const intel = row?.instrument_intel_context?.macro_impact

  const parts = []
  parts.push(`【${market} — macro framework】 ${profile.headline}.`)
  profile.channels.forEach((ch) => parts.push(`• ${ch}`))
  parts.push(`【Regime read (interpretive)】 Primary: ${regime.primary}.${regime.secondary ? ` Overlay: ${regime.secondary}.` : ''}`)
  regime.rationale.forEach((r) => parts.push(`• ${r}`))
  parts.push(`【Macro vs positioning / tape language】 ${tension.label}`)
  tension.lines.forEach((l) => parts.push(`• ${l}`))
  parts.push(`【Event intelligence】 ${events.marketEventSummary}`)
  if (events.structuredEvents.length) {
    events.structuredEvents.slice(0, 4).forEach((e) => {
      parts.push(`• ${e.match[0]}: ${e.sensitivity} ${clip(e.relevance, 220)}`)
    })
  }
  parts.push(
    `【Interpretation conviction】 ${conv.overall}/100 (${conv.band}). ${conv.explain} Subscores (0–10): alignment ${conv.dimensions.alignment}, stability ${conv.dimensions.stability}, conflict-pressure ${conv.dimensions.conflictPressure}, event-distortion ${conv.dimensions.eventDistortion}, sensitivity ${conv.dimensions.channelSensitivity}, flow-persistence ${conv.dimensions.flowPersistence}.`,
  )
  if (has(intel)) {
    parts.push(`【Embedded desk copy】 ${clip(intel, 420)}`)
  }
  return parts.join('\n\n')
}

export function computeMacroIntelligenceBundle(market, row, pack, globalRegime, participationCategory) {
  const p = pack || row?.ui_pack || {}
  const eventRisk = assessEventRisk(row, p)
  const regime = classifyMacroRegimeState(row, globalRegime, eventRisk)
  const tension = interpretMacroVsTapeTension(row, participationCategory)
  const events = buildExpandedEventIntel(market, row, p)
  const conviction = computeMacroInterpretationConviction(row, globalRegime, participationCategory)
  const briefing = buildMacroInstitutionalBriefing(market, row, p, globalRegime, participationCategory)
  const profile = getMarketMacroProfile(market)
  return { profile, regime, tension, events, conviction, briefing }
}
