/** Client-side attention / priority helpers (mirrors server attention_engine). */

import { getInstitutionalContext } from './institutionalContext.js'

const TIER_ORDER = { high_attention: 0, developing: 1, watchlist: 2, low_priority: 3 }

const TIER_CLASS = {
  high_attention: 'priority-high',
  developing: 'priority-developing',
  watchlist: 'priority-watchlist',
  low_priority: 'priority-low',
}

export function attentionFromRow(row) {
  return getInstitutionalContext(row)?.attention || null
}

export function dominantNarrative(row) {
  const att = attentionFromRow(row)
  if (att?.dominant_narrative) return att.dominant_narrative
  return null
}

export function priorityTier(row) {
  return attentionFromRow(row)?.priority_tier || 'low_priority'
}

export function priorityLabel(row) {
  return attentionFromRow(row)?.priority_label || 'LOW PRIORITY'
}

export function priorityClass(row) {
  return TIER_CLASS[priorityTier(row)] || 'priority-low'
}

export function attentionAlerts(row) {
  return attentionFromRow(row)?.alerts || []
}

export function tacticalReadable(row) {
  const att = attentionFromRow(row)
  return att?.tactical_readable || null
}

/** Prefer server-built priority debug (full universe). */
export function buildPriorityBoardFromDebug(debugDoc, { topN = 6 } = {}) {
  if (!debugDoc || !Array.isArray(debugDoc.priority_markets) || !debugDoc.priority_markets.length) {
    return null
  }
  return {
    priority_markets: debugDoc.priority_markets.slice(0, topN),
    calendar_week: debugDoc.calendar_week,
    total_actionable: debugDoc.candidates_above_floor ?? debugDoc.priority_markets.length,
    high_attention: debugDoc.priority_markets.filter((m) => m.priority_tier === 'high_attention'),
    developing: debugDoc.priority_markets.filter((m) => m.priority_tier === 'developing'),
    source: 'priority_debug_latest',
  }
}

/** Build priority board from current week rows (fallback if payload lacks server debug). */
export function buildPriorityBoardFromRows(marketRows, { topN = 6 } = {}) {
  const ranked = marketRows
    .map((r) => {
      const att = attentionFromRow(r)
      if (!att || att.priority_tier === 'low_priority') return null
      const alerts = att.alerts || []
      return {
        market: r.market,
        priority_tier: att.priority_tier,
        priority_label: att.priority_label,
        priority_score: Number(att.priority_score) || 0,
        dominant_narrative: att.dominant_narrative,
        priority_headline: att.priority_headline,
        icon: alerts[0]?.icon || '👀',
        tactical_readable: att.tactical_readable,
      }
    })
    .filter(Boolean)
    .sort((a, b) => b.priority_score - a.priority_score)

  const high = ranked.filter((r) => r.priority_tier === 'high_attention').slice(0, topN)
  const developing = ranked.filter((r) => r.priority_tier === 'developing').slice(0, Math.max(0, topN - high.length))

  return {
    priority_markets: [...high, ...developing].slice(0, topN),
    high_attention: high,
    developing,
    total_actionable: ranked.length,
  }
}

export function sortByPriority(rows) {
  return [...rows].sort((a, b) => {
    const ta = TIER_ORDER[priorityTier(a)] ?? 9
    const tb = TIER_ORDER[priorityTier(b)] ?? 9
    if (ta !== tb) return ta - tb
    const sa = Number(attentionFromRow(a)?.priority_score) || 0
    const sb = Number(attentionFromRow(b)?.priority_score) || 0
    if (sa !== sb) return sb - sa
    return String(a.market).localeCompare(String(b.market))
  })
}
