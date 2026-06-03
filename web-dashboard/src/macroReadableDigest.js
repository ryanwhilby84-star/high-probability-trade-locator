/**
 * Trader-readable macro digest — interpretation only, no probability theatre.
 * Uses validated row export + optional week-level global_market_regime.
 */

import { validateEventItem } from './marketEnvironment.js'

function clip(s, n = 140) {
  const t = String(s || '').trim()
  if (!t) return ''
  if (t.length <= n) return t
  return `${t.slice(0, n - 1)}…`
}

function low(s) {
  return String(s || '').trim().toLowerCase()
}

/** Reject placeholder / empty macro strings from exports. */
export function validMacroText(s) {
  const t = String(s ?? '').trim()
  if (!t || t.toUpperCase() === 'N/A' || t === '—') return ''
  const l = t.toLowerCase()
  if (l.includes('source unavailable')) return ''
  if (/^no macro\/rates snapshot/i.test(t)) return ''
  return t
}

function formatSignal(sig) {
  const s = String(sig || '').trim().replace(/_/g, ' ')
  if (!validMacroText(s)) return ''
  return s.charAt(0).toUpperCase() + s.slice(1)
}

function num(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : NaN
}

function composeRegimeLabel(g, row) {
  if (g && typeof g === 'object') {
    const sig = formatSignal(g.resolved_macro_signal)
    if (sig) return clip(sig, 88)

    const rp = low(g.rates_pressure)
    const inf = low(g.inflation_regime)
    const liq = low(g.liquidity_regime)
    const rr = low(g.risk_regime)

    if (inf.includes('disinfl') && (inf.includes('slow') || inf.includes('growth') || rp.includes('slow')))
      return 'Disinflation + slowing growth'
    if ((liq.includes('expan') || liq.includes('easy') || liq.includes('accommod')) && (rr.includes('risk_on') || rr.includes('risk on')))
      return 'Risk-on liquidity expansion'
    if (rp.includes('restrict') || rp.includes('elevated yield') || rp.includes('tighten') || rp.includes('higher yield'))
      return 'Restrictive yields / tightening pressure'
    if (rr.includes('risk_off') || rr.includes('risk off')) return 'Defensive / risk-off tone'
    if (rr.includes('risk_on') || rr.includes('risk on')) return 'Risk-on tone'

    const first = [validMacroText(g.inflation_regime), validMacroText(g.liquidity_regime), validMacroText(g.rates_pressure)].find(Boolean)
    if (first) return clip(first, 88)
  }

  const mr = validMacroText(String(row?.macro_regime || '').replace(/_/g, ' '))
  if (mr) return clip(mr, 88)
  return 'Mixed macro backdrop'
}

function composeConviction(g, row) {
  const scores = []
  if (g && typeof g === 'object') {
    const a = num(g.resolved_macro_score)
    if (Number.isFinite(a)) scores.push(a)
  }
  const b = num(row?.macro_score)
  if (Number.isFinite(b)) scores.push(b)

  let level = null
  if (scores.length) {
    const blend = scores.reduce((x, y) => x + y, 0) / scores.length
    if (blend < 4) level = 'Low'
    else if (blend <= 6.5) level = 'Medium'
    else level = 'High'
  }

  let detail = ''
  if (g && typeof g === 'object') {
    detail = validMacroText(g.rates_pressure) || validMacroText(g.summary)
  }
  detail = clip(detail, 96)
  if (!detail && scores.length === 0) detail = 'Macro fields thin on this export — use rates table and your calendar.'

  return { level, detail }
}

function isPreciousMetal(market) {
  return /gold|silver/i.test(market || '')
}

function isEquityIndex(market) {
  const m = low(market)
  return m.includes('nasdaq') || m.includes('s&p') || m.includes('/ es') || m.includes('/ nq') || m.includes('dow') || m.includes('/ ym')
}

function buildRatesInterpretation(market, row, g) {
  const lines = []
  const rm = row?.rates_macro && typeof row.rates_macro === 'object' ? row.rates_macro : null
  const bias = low(rm?.rates_bias)
  const curve = validMacroText(rm?.curve_state)
  const macroSig = validMacroText(rm?.macro_signal)

  const z = rm != null ? num(rm.real_yield_z_score ?? rm.real_yield_z ?? rm.tips_z_score) : NaN
  if (Number.isFinite(z)) {
    if (z >= 0.75) lines.push('Real yields elevated vs recent range.')
    else if (z <= -0.75) lines.push('Real yields soft vs recent range.')
    else lines.push('Real yields near the middle of the recent range.')
  }

  if (bias.includes('rising') && (bias.includes('yield') || bias.includes('rate'))) {
    if (isPreciousMetal(market)) lines.push('Nominal yields leaning higher — often a headwind for precious metals when the story is rate-led.')
    else if (isEquityIndex(market)) lines.push('Yields drifting up — watch duration-heavy equity beta unless liquidity clearly offsets.')
    else lines.push('Yields drifting up — tighter financial-conditions tone vs the prior snapshot.')
  } else if (bias.includes('fall') || bias.includes('easing') || bias.includes('lower') || bias.includes('declin')) {
    if (isEquityIndex(market)) lines.push('Yields softer or stable on this print — supportive for growth / risk assets when nothing else breaks.')
    else if (isPreciousMetal(market)) lines.push('Yields softer on this print — eases one common weight on bullion.')
    else lines.push('Yields softer on this print — looser rate tone for cyclicals and carry.')
  }

  const rp = g && typeof g === 'object' ? validMacroText(g.rates_pressure) : ''
  if (rp && lines.length < 3 && !lines.some((x) => low(x).includes(low(rp).slice(0, 24)))) lines.push(clip(rp, 110))

  if (curve && lines.length < 3) lines.push(`Curve: ${clip(curve, 72)}`)

  if (macroSig && lines.length < 3 && !macroSig.toLowerCase().includes('unavailable')) lines.push(clip(macroSig, 100))

  const usd = g && typeof g === 'object' ? validMacroText(g.usd_impulse) : ''
  if (usd && !low(usd).includes('not modeled') && lines.length < 3) lines.push(`Dollar: ${clip(usd, 80)}`)

  return [...new Set(lines.map((x) => x.trim()).filter(Boolean))].slice(0, 3)
}

const WEEKDAYS = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']

function importanceLabel(risk) {
  const r = low(risk)
  if (r === 'elevated') return 'Elevated'
  if (r === 'moderate') return 'Moderate'
  if (r === 'low') return 'Low'
  return 'Unknown'
}

function eventContext(risk) {
  const r = low(risk)
  if (r === 'elevated') return 'Elevated volatility likely around the print.'
  if (r === 'low') return 'Event risk low vs larger macro weeks.'
  return 'Worth a size plan; no implied move here.'
}

function calendarEventFromCatalysts(feed, market) {
  const cal = feed?.calendar_catalysts
  if (!cal?.wired || !Array.isArray(cal.upcoming_high_impact) || !cal.upcoming_high_impact.length) {
    return null
  }
  const upcoming = [...cal.upcoming_high_impact].sort((a, b) =>
    String(a.event_timestamp || a.date || '').localeCompare(String(b.event_timestamp || b.date || '')),
  )
  const ev = upcoming.find((e) => (e.affected_markets || []).includes(market)) || upcoming[0]
  const ts = String(ev.event_timestamp || ev.date || '').trim()
  const t = Date.parse(ts)
  const day = Number.isFinite(t) ? WEEKDAYS[new Date(t).getDay()] : ''
  const headline = clip(ev.event_name || ev.headline, 56)
  const rank = Number(ev.importance_rank) || 0
  const risk = rank >= 3 ? 'elevated' : rank === 2 ? 'moderate' : 'low'
  const imp = importanceLabel(risk)
  const summaryLine = day ? `${headline} · ${day}` : headline
  return {
    headline,
    timing: ts ? ts.slice(0, 16).replace('T', ' ') : day || 'Scheduled',
    importance: imp,
    contextLine: clip(validMacroText(ev.interpretation) || eventContext(risk), 120),
    summaryLine,
    published_at: ts || null,
    source: ev.source || cal.provider || 'finnhub',
  }
}

function buildNextCalendarEvent(row, market) {
  const feed = row?.market_environment_feed && typeof row.market_environment_feed === 'object' ? row.market_environment_feed : {}
  const raw = Array.isArray(feed.event_items) ? feed.event_items : []
  const validated = []
  for (let i = 0; i < raw.length; i++) {
    const v = validateEventItem(raw[i], market)
    if (v.ok && v.item) validated.push(v.item)
  }
  const inst = validated.filter(
    (e) => !e.related_instruments?.length || e.related_instruments.some((x) => String(x).trim() === market),
  )
  if (!inst.length) return calendarEventFromCatalysts(feed, market)

  inst.sort((a, b) => Date.parse(a.published_at) - Date.parse(b.published_at))
  const ev = inst[0]
  const t = Date.parse(ev.published_at)
  const day = Number.isFinite(t) ? WEEKDAYS[new Date(t).getDay()] : ''
  const headline = clip(ev.headline, 56)
  const imp = importanceLabel(ev.risk_level)
  const summaryLine = day ? `${headline} · ${day}` : headline
  return {
    headline,
    timing: day || 'Scheduled',
    importance: imp,
    contextLine: eventContext(ev.risk_level),
    summaryLine,
    published_at: ev.published_at,
    source: ev.source,
  }
}

function macroBiasTag(row) {
  const inter = row?.intermarket_impulse_context && typeof row.intermarket_impulse_context === 'object' ? row.intermarket_impulse_context : {}
  const c = String(inter.intermarket_confirmation || '').trim().toUpperCase()
  if (c === 'CONFIRMING') return 'Related markets confirming'
  if (c === 'DIVERGING' || c === 'WARNING') return 'Related markets split'
  if (c === 'MIXED') return 'Mixed'
  return 'Mixed'
}

/**
 * @param {object} row — confluence row (needs `market`, optional rates_macro, global_market_regime, market_environment_feed)
 * @param {object|null|undefined} globalMarketRegime — week-level regime (falls back to row.global_market_regime)
 */
export function buildMacroReadableDigest(row, globalMarketRegime) {
  const market = String(row?.market || '').trim() || '—'
  const g =
    globalMarketRegime && typeof globalMarketRegime === 'object'
      ? globalMarketRegime
      : row?.global_market_regime && typeof row.global_market_regime === 'object'
        ? row.global_market_regime
        : null

  const regimeLabel = composeRegimeLabel(g, row)
  const { level: convictionLevel, detail: convictionDetail } = composeConviction(g, row)
  const ratesLines = buildRatesInterpretation(market, row, g)
  const calendar = buildNextCalendarEvent(row, market)
  const biasTag = macroBiasTag(row)

  const parts = [`Regime: ${regimeLabel}`]
  if (convictionLevel) {
    parts.push(`Conviction: ${convictionLevel}${convictionDetail ? ` (${convictionDetail})` : ''}`)
  }
  for (const line of ratesLines) parts.push(line)
  const calWired = row?.market_environment_feed?.calendar_catalysts?.wired === true
  if (calendar) parts.push(`Next: ${calendar.summaryLine} · Importance ${calendar.importance}`)
  else if (calWired) parts.push('Calendar: wired — no upcoming high-impact print mapped to this market in the window.')
  else parts.push('Calendar: NOT WIRED on this row.')
  parts.push(`Macro bias: ${biasTag}`)

  const macroBiasLine = parts.join('\n')
  const briefMacroLine = clip(
    [regimeLabel, ratesLines[0] || '', calendar ? `Next: ${calendar.summaryLine}` : 'No calendar in feed', `Bias ${biasTag}`]
      .filter(Boolean)
      .join(' · '),
    200,
  )

  return {
    regimeLabel,
    convictionLevel,
    convictionDetail,
    ratesLines,
    calendar,
    macroBiasLine,
    briefMacroLine,
    nextEventSummary: calendar
      ? `${calendar.summaryLine} (${calendar.importance})`
      : calWired
        ? 'LOW CONFIDENCE — calendar wired, no upcoming print for this market'
        : 'NOT WIRED',
    macroBiasTag: biasTag,
  }
}

/** Week-level strip (no instrument row) — same regime/conviction logic as rows. */
export function buildWeekBackdropDigest(globalRegime) {
  const stub = { market: '', rates_macro: null, macro_regime: null, macro_score: null, market_environment_feed: {} }
  return buildMacroReadableDigest(stub, globalRegime)
}
