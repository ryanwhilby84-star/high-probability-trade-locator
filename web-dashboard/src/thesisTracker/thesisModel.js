// Pure display + derivation helpers for the Thesis Tracker.
// Mirrors src/hptl/thesis_tracker/{models,conviction}.py so client-side adds
// (from the instrument page) compute the same composite the backend does.

export const STATUS_FLOW = ['DISCOVERED', 'DEVELOPING', 'READY', 'ACTIVE', 'COMPLETED']
export const STATUS_COMPLETED = 'COMPLETED'
export const STATUS_INVALIDATED = 'INVALIDATED'
export const ALL_STATUSES = [...STATUS_FLOW, STATUS_INVALIDATED]
export const TERMINAL_STATUSES = new Set(['COMPLETED', STATUS_INVALIDATED])

const STATUS_ALIASES = {
  'LIMIT ORDER SET': 'READY',
  LIMIT_ORDER_SET: 'READY',
  'ACTIVE TRADE': 'ACTIVE',
  ACTIVE_TRADE: 'ACTIVE',
}

export const STATUS_META = {
  DISCOVERED: { label: 'Discovered', tone: 'slate' },
  DEVELOPING: { label: 'Developing', tone: 'sky' },
  READY: { label: 'Ready', tone: 'emerald' },
  ACTIVE: { label: 'Active', tone: 'amber' },
  COMPLETED: { label: 'Completed', tone: 'slate' },
  INVALIDATED: { label: 'Invalidated', tone: 'rose' },
}

export const STATUS_DEFINITIONS = {
  DISCOVERED: 'Initial signal detected — worth monitoring.',
  DEVELOPING: 'Conditions improving — thesis forming.',
  READY: 'Multiple factors aligned — limit-order preparation justified.',
  ACTIVE: 'Trade entered / order placed.',
  INVALIDATED: 'Thesis broken — conditions deteriorated.',
  COMPLETED: 'Trade closed — thesis finished.',
}

export function statusMeta(status) {
  return STATUS_META[normStatus(status)] || { label: String(status || '—'), tone: 'slate' }
}

export function normStatus(value) {
  const s = String(value || '').trim().toUpperCase()
  if (STATUS_ALIASES[s]) return STATUS_ALIASES[s]
  if (ALL_STATUSES.includes(s)) return s
  const s2 = s.replace(/_/g, ' ')
  if (STATUS_ALIASES[s2]) return STATUS_ALIASES[s2]
  return ALL_STATUSES.includes(s2) ? s2 : 'DISCOVERED'
}

export const TIER_META = {
  1: { label: 'Tier 1', tone: 'rose', blurb: 'Ready soon — monitor closely' },
  2: { label: 'Tier 2', tone: 'amber', blurb: 'Developing — track weekly' },
  3: { label: 'Tier 3', tone: 'slate', blurb: 'Observation only' },
}

export const COMPONENT_WEIGHTS = { cot: 0.45, macro: 0.25, structural: 0.3 }
export const PLACEHOLDER_COMPONENTS = []

const clamp = (v, lo = 0, hi = 100) => Math.max(lo, Math.min(hi, v))
const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function componentValues(snap = {}) {
  const out = {}
  if (isNum(snap.cot_score)) out.cot = clamp(snap.cot_score * 10)
  if (isNum(snap.macro_score)) out.macro = clamp(snap.macro_score * 10)
  if (isNum(snap.structural_score)) out.structural = clamp(snap.structural_score)
  return out
}

export function computeConviction(snap = {}) {
  if (isNum(snap.conviction_score)) {
    return { score: Math.round(snap.conviction_score), present: snap.conviction_components_present || [] }
  }
  const comps = componentValues(snap)
  const keys = Object.keys(comps)
  if (!keys.length) return { score: null, present: [] }
  const totalW = keys.reduce((a, k) => a + (COMPONENT_WEIGHTS[k] || 0), 0)
  if (totalW <= 0) return { score: null, present: keys.sort() }
  const score = keys.reduce((a, k) => a + comps[k] * (COMPONENT_WEIGHTS[k] || 0), 0) / totalW
  return { score: Math.round(clamp(score)), present: keys.sort() }
}

export function convictionSeries(snapshots = []) {
  return [...snapshots]
    .sort((a, b) => String(a.week || '').localeCompare(String(b.week || '')))
    .map((s) => computeConviction(s).score)
    .filter((v) => isNum(v))
}

export const TREND_META = {
  improving: { label: 'Improving', tone: 'emerald', arrow: '▲' },
  deteriorating: { label: 'Deteriorating', tone: 'rose', arrow: '▼' },
  stable: { label: 'Stable', tone: 'slate', arrow: '▬' },
}

export function computeTrend(snapshots = [], window = 4) {
  const series = convictionSeries(snapshots)
  if (series.length < 2) return 'stable'
  const tail = series.slice(-window)
  const net = tail[tail.length - 1] - tail[0]
  if (net >= 3) return 'improving'
  if (net <= -3) return 'deteriorating'
  return 'stable'
}

export function ageWeeks(snapshots = []) {
  const weeks = new Set(snapshots.map((s) => String(s.week || '').trim()).filter(Boolean))
  return weeks.size
}

export const DIRECTION_META = {
  long: { label: 'Long bias', tone: 'emerald' },
  short: { label: 'Short bias', tone: 'rose' },
  neutral: { label: 'Neutral', tone: 'slate' },
}

// Build a Week-1 snapshot from a confluence/instrument row (client-side add).
export function snapshotFromRow(row = {}, week) {
  const inst = row.institutional_context && typeof row.institutional_context === 'object' ? row.institutional_context : {}
  const attention = inst.attention && typeof inst.attention === 'object' ? inst.attention : {}
  const num = (v) => {
    if (v === null || v === undefined) return null
    if (typeof v === 'string') {
      const s = v.trim().toLowerCase()
      if (!s || ['n/a', 'nan', 'null', 'none', '—'].includes(s)) return null
    }
    const n = Number(v)
    return Number.isFinite(n) ? n : null
  }
  const str = (v) => {
    if (v === null || v === undefined) return null
    const s = String(v).trim()
    return !s || ['n/a', 'nan', 'null', 'none'].includes(s.toLowerCase()) ? null : s
  }
  const snap = {
    week: week || str(row.date) || str(row.cot_report_date) || str(row.latest_report_date) || '',
    cot_report_date: str(row.cot_report_date) || str(row.latest_report_date),
    captured_at: new Date().toISOString(),
    cot_bias: str(row.cot_bias) || str(row.final_calculated_cot_bias),
    cot_score: num(row.cot_score) ?? num(row.final_calculated_cot_score),
    long_value: num(row.long_value),
    short_value: num(row.short_value),
    net_value: num(row.net_value),
    one_week_net_change: num(row.one_week_net_change),
    four_week_net_change: num(row.four_week_net_change),
    positioning_state: str(row.positioning_state),
    macro_regime: str(row.macro_regime) || str(row.macro_signal),
    macro_score: num(row.macro_score),
    structural_score: num(inst.structural_score),
    structural_conviction: str(inst.structural_conviction),
    priority_score: num(attention.priority_score),
    zone_focus: str(row.zone_focus) || str(inst.tactical?.zone_focus),
    retail_long: (() => {
      const nr = row.cot_positioning_groups?.nonreportable
      return nr ? num(nr.long) : null
    })(),
    retail_short: (() => {
      const nr = row.cot_positioning_groups?.nonreportable
      return nr ? num(nr.short) : null
    })(),
    retail_net: (() => {
      const nr = row.cot_positioning_groups?.nonreportable
      return nr ? num(nr.net) : null
    })(),
    valuation_bias: str(row.valuation_bias),
    valuation_score: num(row.valuation_score),
    valuation_reason: str(row.valuation_reason),
    valuation_wired: !!row.valuation_wired,
    seasonality_bias: str(row.seasonality_bias),
    seasonality_score: num(row.seasonality_score),
    seasonality_reason: str(row.seasonality_reason),
    seasonality_wired: !!row.seasonality_wired,
    retail_positioning_score: null,
  }
  const { score, present } = computeConviction(snap)
  snap.conviction_score = score
  snap.conviction_components_present = present
  return snap
}

export function directionFromSnapshot(snap = {}) {
  const bias = String(snap.cot_bias || '').toLowerCase()
  if (bias.includes('bull')) return 'long'
  if (bias.includes('bear')) return 'short'
  return 'neutral'
}
