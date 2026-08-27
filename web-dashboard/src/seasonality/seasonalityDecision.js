/** Production seasonality decision panel helpers (no timeline overlay). */

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

export const fmtPct = (v) => (isNum(v) ? `${v >= 0 ? '+' : ''}${v.toFixed(2)}%` : '—')

export function resolveSeasonBlock(seasonalityDoc, marketId) {
  if (!seasonalityDoc?.markets || !marketId) return null
  if (seasonalityDoc.markets[marketId]) return seasonalityDoc.markets[marketId]
  const target = String(marketId).toLowerCase()
  for (const [key, block] of Object.entries(seasonalityDoc.markets)) {
    if (String(key).toLowerCase() === target) return block
  }
  return null
}

export function confidenceLabel(confidenceBlock, forward8w) {
  /** @deprecated Prefer buildPathAgreementRead — this maps path agreement (Strong/Medium/Weak) only. */
  const level = confidenceBlock?.level
  if (level === 'Strong') return 'High'
  if (level === 'Medium') return 'Medium'
  if (level === 'Weak') return 'Low'
  const n = forward8w?.sample_years ?? 0
  const avg = forward8w?.avg_return_pct
  if (n >= 10 && isNum(avg) && Math.abs(avg) >= 0.5) return 'Medium'
  return 'Low'
}

export function horizonConfidence(row, overall) {
  if (!row?.available) return '—'
  const n = row.sample_years ?? 0
  const wr = row.win_rate_pct
  const avg = row.avg_return_pct
  if (overall === 'High' && n >= 8 && isNum(wr) && (wr >= 60 || wr <= 40)) return 'High'
  if (n >= 7 && isNum(avg) && Math.abs(avg) >= 0.3) return 'Medium'
  return 'Low'
}

function biasStrength(row) {
  if (!row?.available) return ''
  const avg = row.avg_return_pct
  const wr = row.win_rate_pct
  if (!isNum(avg)) return ''
  const mag = Math.abs(avg)
  if (mag >= 1.5 && isNum(wr) && (wr >= 65 || wr <= 35)) return 'strongly '
  if (mag >= 0.5 || (isNum(wr) && (wr >= 55 || wr <= 45))) return 'weakly '
  return ''
}

export function buildPlainEnglishRead(block, marketName, horizonWeeks = 8) {
  const name = marketName || block?.market || 'This market'
  const forwardRead = block?.forward_read || {}
  const row =
    horizonWeeks === 4
      ? forwardRead.next_4w
      : horizonWeeks === 12
        ? forwardRead.next_12w
        : forwardRead.next_8w
  const grade = block?.trust_grade || 'C'

  if (grade === 'C') {
    return `${name} seasonality is unreliable. ${block?.trust_notes || block?.reason || 'Insufficient history.'} Hidden by default.`
  }

  if (!row?.available) {
    return `${name} seasonality: forward outlook unavailable for ISO week ${block?.current_week ?? '—'}.`
  }

  const pathAgreement = block?.confidence?.level || 'Weak'
  const windowConf = horizonConfidence(
    row,
    pathAgreement === 'Strong' ? 'High' : pathAgreement === 'Medium' ? 'Medium' : 'Low',
  )
  const dir = String(row.direction || 'Neutral').toLowerCase()
  const tone = dir === 'bullish' ? 'bullish' : dir === 'bearish' ? 'bearish' : 'neutral'
  const wr = isNum(row.win_rate_pct) ? `${row.win_rate_pct}%` : '—'
  const sample = row.sample_years ?? '—'
  const strength = biasStrength(row)
  const prefix =
    grade === 'B'
      ? 'Limited sample / sparse curve — use as context only. '
      : ''

  return (
    `${prefix}${name} seasonality is ${strength}${tone} over the next ${horizonWeeks} weeks. ` +
    `Average return: ${fmtPct(row.avg_return_pct)}. ` +
    `Win rate: ${wr}. ` +
    `Sample: ${sample} years. ` +
    `Forward window read confidence: ${windowConf}. ` +
    `Seasonal path agreement: ${pathAgreement}. ` +
    `Use as supporting context only.`
  )
}

export function gradePillClass(grade) {
  if (grade === 'A') return 'high'
  if (grade === 'B') return 'medium'
  return 'neutral'
}
