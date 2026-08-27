/** Seasonality confidence interpretation — explicit labels for traders (display only). */

import { fmtPct, horizonConfidence } from './seasonalityDecision.js'

export const CONFIDENCE_TOOLTIPS = {
  dataQuality:
    'How trustworthy the price history is: years of coverage, bar density, trust grade, and any data-quality warnings.',
  pathAgreement:
    'Whether 3Y, 5Y, and 10Y seasonal paths agree on forward direction. Weak means windows disagree — not that the data is bad.',
  forwardWindow:
    'Reliability of one forward return window (sample size, win rate, average move). Independent of path agreement.',
  tradeUsefulness:
    'Whether seasonality supports longs, shorts, is mixed, or neutral — and how strongly paths agree.',
  strongNotGuaranteed: 'Strong path agreement does not guarantee the move; it means historical windows point the same way.',
  weakNotUnusable:
    'Weak path agreement does not mean seasonality is unusable — one forward window may still have a medium-quality read.',
}

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function pathAgreementLevel(confidenceBlock) {
  const level = String(confidenceBlock?.level || '')
  if (level === 'Strong') return 'Strong'
  if (level === 'Medium') return 'Medium'
  if (level === 'Weak') return 'Weak'
  if (level === 'Low sample reliability' || level === 'Insufficient history') return 'Low'
  return 'Weak'
}

function pathAgreementDisplayLevel(level) {
  if (level === 'Strong') return 'Strong'
  if (level === 'Medium') return 'Medium'
  if (level === 'Low') return 'Low'
  return 'Weak'
}

export function buildDataQualityRead(block) {
  const grade = block?.trust_grade || 'C'
  const years = block?.years_used ?? block?.years_of_history ?? '—'
  const sample8 = block?.forward_read?.next_8w?.sample_years ?? block?.sample_size ?? '—'
  const warning = block?.data_quality_warning

  let level = 'Low'
  if (grade === 'A' && !warning) level = 'High'
  else if (grade === 'A' || grade === 'B') level = 'Medium'

  const parts = [`${years} years`, `trust grade ${grade}`]
  if (sample8 !== '—') parts.push(`n=${sample8}`)
  let summary = `${pathAgreementDisplayLevel(level === 'High' ? 'Strong' : level === 'Medium' ? 'Medium' : 'Weak')} — ${parts.join(', ')}.`
  if (level === 'High') summary = `High — ${parts.join(', ')}.`
  else if (level === 'Medium') summary = `Medium — ${parts.join(', ')}.`
  else summary = `Low — ${parts.join(', ')}.`

  if (warning) summary += ` Warning: ${warning}.`
  if (grade === 'C') {
    summary = `Low — ${block?.trust_notes || block?.reason || 'Insufficient history'}.`
    level = 'Low'
  }

  return {
    level,
    label: 'Data quality',
    summary,
    trustGrade: grade,
    warning: warning || null,
  }
}

export function buildPathAgreementRead(block) {
  const cb = block?.confidence || {}
  const raw = pathAgreementLevel(cb)
  const level = pathAgreementDisplayLevel(raw)
  const detail = cb.detail || 'Path agreement unavailable.'
  return {
    level,
    rawLevel: raw,
    label: 'Seasonal path agreement',
    summary: `${level} — ${detail}`,
    detail,
    windows: cb.windows ?? 0,
    agreement: cb.agreement ?? 0,
  }
}

function isReliabilityLabel(direction) {
  const d = String(direction || '')
  return d === 'Low sample reliability' || d === 'Insufficient history'
}

function forwardWindowConfidence(row, pathRawLevel) {
  if (!row?.available) return '—'
  const grade = 'A' // gated upstream in cards
  if (grade !== 'A' || (row.sample_years ?? 0) < 5 || isReliabilityLabel(row.direction)) return 'Low'
  const overall =
    pathRawLevel === 'Strong' ? 'High' : pathRawLevel === 'Medium' ? 'Medium' : 'Low'
  return horizonConfidence(row, overall) || overall
}

export function buildForwardWindowRead(block, horizonWeeks = 8) {
  const forwardRead = block?.forward_read || {}
  const row =
    horizonWeeks === 4
      ? forwardRead.next_4w
      : horizonWeeks === 12
        ? forwardRead.next_12w
        : forwardRead.next_8w
  const grade = block?.trust_grade || 'C'
  const pathRaw = pathAgreementLevel(block?.confidence)
  const key = `${horizonWeeks}W`

  if (grade === 'C' || !row?.available || (row.sample_years ?? 0) < 5 || isReliabilityLabel(row.direction)) {
    return {
      key,
      level: 'Low',
      label: 'Forward window read',
      summary: `${key}: Unreliable — insufficient sample (n=${row?.sample_years ?? 0}).`,
      direction: '—',
      avgReturn: '—',
      winRate: '—',
      sampleYears: row?.sample_years ?? '—',
    }
  }

  const conf = forwardWindowConfidence(row, pathRaw)
  const dir = String(row.direction || 'Neutral')
  const wr = isNum(row.win_rate_pct) ? `${row.win_rate_pct}%` : '—'
  const n = row.sample_years ?? '—'

  return {
    key,
    level: conf,
    label: 'Forward window read',
    summary: `${key}: ${dir}, ${conf} confidence — avg ${fmtPct(row.avg_return_pct)}, win rate ${wr}, n=${n}.`,
    direction: dir,
    avgReturn: fmtPct(row.avg_return_pct),
    winRate: wr,
    sampleYears: n,
  }
}

export function buildTradeUsefulnessRead(block) {
  const grade = block?.trust_grade || 'C'
  const f4 = block?.forward_read?.next_4w
  const f8 = block?.forward_read?.next_8w
  const f12 = block?.forward_read?.next_12w
  const path = buildPathAgreementRead(block)

  if (!block?.available || grade === 'C') {
    return {
      level: 'Low',
      label: 'Trade usefulness',
      summary: 'Seasonality not reliable enough to support a trade thesis.',
      tone: 'warn',
    }
  }

  if (!f8?.available || (f8.sample_years ?? 0) < 5 || isReliabilityLabel(f8.direction)) {
    return {
      level: 'Low',
      label: 'Trade usefulness',
      summary: 'Forward sample too thin — seasonality does not support a directional thesis.',
      tone: 'warn',
    }
  }

  if (grade === 'B') {
    return {
      level: 'Low',
      label: 'Trade usefulness',
      summary: 'Context only — sparse curve; use seasonality as background, not a primary thesis driver.',
      tone: 'neutral',
    }
  }

  const d8 = String(f8.direction || 'Neutral')
  const dirs = [
    f4?.available && !isReliabilityLabel(f4.direction) ? String(f4.direction) : null,
    d8,
    f12?.available && !isReliabilityLabel(f12.direction) ? String(f12.direction) : null,
  ].filter(Boolean)
  const unique = [...new Set(dirs)]

  let thesis = 'Seasonality neutral'
  let tone = 'neutral'
  if (unique.length > 1) {
    thesis = 'Seasonality mixed across forward windows'
    tone = 'neutral'
  } else if (d8 === 'Bullish') {
    thesis = 'Seasonality supports longs'
    tone = 'bull'
  } else if (d8 === 'Bearish') {
    thesis = 'Seasonality supports shorts'
    tone = 'bear'
  }

  const pathQualifier =
    path.rawLevel === 'Strong'
      ? 'Path agreement is strong.'
      : path.rawLevel === 'Medium'
        ? 'Path agreement is moderate — not all windows align.'
        : 'Path agreement is weak — windows disagree even if one read looks usable.'

  const level =
    path.rawLevel === 'Strong' && unique.length === 1 && d8 !== 'Neutral'
      ? 'High'
      : path.rawLevel === 'Weak' || unique.length > 1
        ? 'Low'
        : 'Medium'

  return {
    level,
    label: 'Trade usefulness',
    summary: `${thesis}; ${pathQualifier}`,
    tone,
    thesis,
    pathQualifier,
  }
}

/** Full clarity block for panel + audits. */
export function buildSeasonalityConfidenceClarity(block) {
  const path = buildPathAgreementRead(block)
  return {
    dataQuality: buildDataQualityRead(block),
    pathAgreement: path,
    forward4w: buildForwardWindowRead(block, 4),
    forward8w: buildForwardWindowRead(block, 8),
    forward12w: buildForwardWindowRead(block, 12),
    tradeUsefulness: buildTradeUsefulnessRead(block),
    tooltips: CONFIDENCE_TOOLTIPS,
  }
}
