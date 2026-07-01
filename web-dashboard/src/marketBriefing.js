/**
 * Scannable per-market briefing from real row + ui_pack + computeInstrumentIntelligence output.
 * Presentation only — no fabricated fields.
 */

function clip(s, n = 160) {
  const t = String(s || '').trim()
  if (!t || t.toUpperCase() === 'N/A') return ''
  if (t.length <= n) return t
  return `${t.slice(0, n - 1)}…`
}

function has(t) {
  const s = String(t || '').trim()
  return s.length > 0 && s.toUpperCase() !== 'N/A' && s !== '—'
}

function cotLeanLine(row) {
  const n = Number(row?.net_value)
  if (!Number.isFinite(n)) return ''
  if (n > 5000) return 'Net positioning is long-biased in contracts.'
  if (n < -5000) return 'Net positioning is short-biased in contracts.'
  if (n !== 0) return 'Net positioning is slightly directional versus flat.'
  return 'Net positioning is roughly balanced.'
}

/** @param {object} row @param {object} pack @param {ReturnType<import('./marketIntelligence.js').computeInstrumentIntelligence>} intel */
export function buildMarketBriefing(row, pack, intel) {
  const p = pack || {}
  const ex = p.executive || {}
  const inter = row?.intermarket_impulse_context && typeof row.intermarket_impulse_context === 'object' ? row.intermarket_impulse_context : {}
  const biasParts = [row?.cot_bias, row?.positioning_state].map((x) => String(x || '').trim()).filter(has)
  const bias = biasParts.length ? biasParts.join(' — ') : '—'

  let positioning = clip(row?.institutional_flow_summary, 220)
  if (!positioning) positioning = cotLeanLine(row) || '—'

  const pressure = clip(row?.pressure_summary, 200) || '—'

  let macro = clip(ex.macro, 120)
  if (!macro && has(row?.macro_regime)) macro = String(row.macro_regime).replace(/_/g, ' ')
  if (!macro) macro = '—'

  const conf = String(inter.intermarket_confirmation || '').trim()
  let intermarket = conf && conf !== '—' ? conf : '—'
  if (intermarket === '—' && has(p.final_context_line)) {
    const imp = String(ex.impulse || '').trim()
    if (has(imp)) intermarket = imp
  }

  const er = intel?.eventRisk
  let eventRisk = '—'
  if (er?.level === 'high') eventRisk = `Elevated — ${clip(er.explain, 100)}`
  else if (er?.level === 'medium') eventRisk = `Moderate — ${clip(er.explain, 100)}`
  else if (er?.level === 'low') eventRisk = `Subdued — ${clip(er.explain, 90)}`

  const tradeEnvironment = clip(intel?.trade?.quality, 120) || clip(ex.environment, 120) || '—'

  let watchNext = clip(row?.next_data_watch, 200)
  if (!watchNext) watchNext = clip(row?.zone_focus, 200)
  if (!watchNext) watchNext = clip(row?.zone_to_watch, 200)
  if (!watchNext) watchNext = '—'

  return {
    bias,
    positioning,
    pressure,
    macro,
    intermarket,
    eventRisk,
    tradeEnvironment,
    watchNext,
  }
}
