import { buildRolling3yContextFromWeeks } from './rolling3yFromLegacyWeeks.js'
import { normalizeReportDate } from '../marketResolution.js'

export const MARKET_STATE_LABELS = {
  COMM_ACCUM: 'Commercial Accumulation Candidate',
  COMM_DIST: 'Commercial Distribution Candidate',
  SPEC_LIQ: 'Speculative Liquidation',
  SPEC_EXP: 'Speculative Expansion',
  RETAIL_BULL_EXT: 'Retail Bullish Extreme',
  RETAIL_BEAR_EXT: 'Retail Bearish Extreme',
  BULL_ALIGN: 'Bullish Alignment',
  BEAR_ALIGN: 'Bearish Alignment',
  CROWDED_LONG: 'Crowded Long Risk',
  CROWDED_SHORT: 'Crowded Short Risk',
  MIXED: 'Mixed / Neutral',
}

function finite(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

function fmtDelta(v) {
  const n = finite(v)
  if (n == null) return '—'
  const sign = n > 0 ? '+' : ''
  return `${sign}${n.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
}

function fmtPct(v) {
  const n = finite(v)
  if (n == null) return '—'
  return `${n.toFixed(0)}%`
}

function sortedWeeks(weeks) {
  if (!Array.isArray(weeks)) return []
  return [...weeks]
    .filter((w) => w?.report_date)
    .sort((a, b) => String(a.report_date).localeCompare(String(b.report_date)))
}

function weeksAsOf(weeks, asOfDate) {
  const asOf = normalizeReportDate(asOfDate)
  const sorted = sortedWeeks(weeks)
  if (!asOf) return sorted
  return sorted.filter((w) => normalizeReportDate(w.report_date) <= asOf)
}

function netChangeOver(weeks, lag) {
  const s = sortedWeeks(weeks)
  if (s.length < 2) return null
  const latest = finite(s[s.length - 1]?.net)
  const idx = Math.max(0, s.length - 1 - lag)
  const prior = finite(s[idx]?.net)
  if (latest == null || prior == null) return null
  return latest - prior
}

function netPctOi(week) {
  const net = finite(week?.net)
  const oi = finite(week?.open_interest)
  if (net == null || oi == null || oi === 0) return null
  return (100 * net) / oi
}

function groupMetrics(weeks) {
  const sorted = sortedWeeks(weeks)
  if (!sorted.length) return null
  const latest = sorted[sorted.length - 1]
  const ctx = buildRolling3yContextFromWeeks(sorted)
  const net = finite(latest.net)
  return {
    net,
    netPctOi: netPctOi(latest),
    netPercentile: ctx?.net_percentile,
    netClass: ctx?.net_class,
    change4w: netChangeOver(sorted, 4),
    change13w: netChangeOver(sorted, 13),
    crowding: ctx?.net_class,
    direction: net > 0 ? 'long' : net < 0 ? 'short' : 'flat',
  }
}

function isExtremeHigh(pct) {
  return Number.isFinite(pct) && pct >= 85
}

function isExtremeLow(pct) {
  return Number.isFinite(pct) && pct <= 15
}

function significantDecline(change, refNet) {
  if (!Number.isFinite(change) || change >= 0) return false
  const ref = Math.max(Math.abs(finite(refNet) || 0), 1)
  return Math.abs(change) >= Math.max(1000, ref * 0.12)
}

function significantRise(change, refNet) {
  if (!Number.isFinite(change) || change <= 0) return false
  const ref = Math.max(Math.abs(finite(refNet) || 0), 1)
  return change >= Math.max(1000, ref * 0.12)
}

function legacyInstrumentAsOf(legacyInstrument, asOfDate) {
  if (!legacyInstrument?.groups) return null
  const groups = {}
  for (const key of ['noncommercials', 'commercials', 'nonreportables']) {
    const block = legacyInstrument.groups[key]
    if (!block) continue
    groups[key] = {
      ...block,
      weeks: weeksAsOf(block.weeks, asOfDate),
    }
  }
  return { ...legacyInstrument, groups }
}

/** Classify one instrument from legacy_cot_latest.json cohort weeks. */
export function classifyMarketState(legacyInstrument, asOfDate = null) {
  const inst = asOfDate ? legacyInstrumentAsOf(legacyInstrument, asOfDate) : legacyInstrument
  const groups = inst?.groups
  if (!groups) {
    return {
      state: MARKET_STATE_LABELS.MIXED,
      stateId: 'MIXED',
      reason: 'Legacy COT data unavailable.',
      metrics: null,
      attentionScore: 0,
    }
  }

  const comm = groupMetrics(groups.commercials?.weeks)
  const spec = groupMetrics(groups.noncommercials?.weeks)
  const retail = groupMetrics(groups.nonreportables?.weeks)

  if (!spec && !comm && !retail) {
    return {
      state: MARKET_STATE_LABELS.MIXED,
      stateId: 'MIXED',
      reason: 'Insufficient positioning history.',
      metrics: { comm, spec, retail },
      attentionScore: 0,
    }
  }

  const candidates = []

  if (spec && isExtremeHigh(spec.netPercentile) && spec.direction === 'long') {
    candidates.push({
      stateId: 'CROWDED_LONG',
      priority: 90,
      reason: `Non-commercials at ${fmtPct(spec.netPercentile)} net percentile with net ${fmtDelta(spec.net)} — crowded long positioning.`,
    })
  }
  if (spec && isExtremeLow(spec.netPercentile) && spec.direction === 'short') {
    candidates.push({
      stateId: 'CROWDED_SHORT',
      priority: 90,
      reason: `Non-commercials at ${fmtPct(spec.netPercentile)} net percentile with net ${fmtDelta(spec.net)} — crowded short positioning.`,
    })
  }

  if (retail && isExtremeHigh(retail.netPercentile)) {
    candidates.push({
      stateId: 'RETAIL_BULL_EXT',
      priority: 85,
      reason: `Non-reportables at ${fmtPct(retail.netPercentile)} net percentile — retail proxy at a bullish extreme.`,
    })
  }
  if (retail && isExtremeLow(retail.netPercentile)) {
    candidates.push({
      stateId: 'RETAIL_BEAR_EXT',
      priority: 85,
      reason: `Non-reportables at ${fmtPct(retail.netPercentile)} net percentile — retail proxy at a bearish extreme.`,
    })
  }

  if (spec && significantDecline(spec.change13w, spec.net)) {
    candidates.push({
      stateId: 'SPEC_LIQ',
      priority: 82,
      reason: `Large non-commercial liquidation over 13 weeks (${fmtDelta(spec.change13w)} contracts).`,
    })
  } else if (spec && significantDecline(spec.change4w, spec.net)) {
    candidates.push({
      stateId: 'SPEC_LIQ',
      priority: 78,
      reason: `Non-commercials cutting net exposure over 4 weeks (${fmtDelta(spec.change4w)}).`,
    })
  }

  if (spec && significantRise(spec.change4w, spec.net)) {
    candidates.push({
      stateId: 'SPEC_EXP',
      priority: 75,
      reason: `Non-commercials expanding net exposure over 4 weeks (${fmtDelta(spec.change4w)}).`,
    })
  }

  if (comm && spec && comm.change4w > 0 && spec.change4w < 0) {
    const retailSells = !retail || retail.change4w <= 0
    candidates.push({
      stateId: 'COMM_ACCUM',
      priority: retailSells ? 92 : 86,
      reason: retailSells
        ? 'Commercials buying while specs and retail sell.'
        : 'Commercials adding net while non-commercials reduce — hedger accumulation pattern.',
    })
  }

  if (comm && spec && comm.change4w < 0 && spec.change4w > 0) {
    candidates.push({
      stateId: 'COMM_DIST',
      priority: 88,
      reason: 'Commercials reducing net while non-commercials add — distribution candidate.',
    })
  }

  if (comm && spec && retail) {
    const allLong = comm.net > 0 && spec.net > 0 && retail.net > 0
    const allShort = comm.net < 0 && spec.net < 0 && retail.net < 0
    if (allLong) {
      candidates.push({
        stateId: 'BULL_ALIGN',
        priority: 70,
        reason: 'Commercials and specs positioned in the same direction (net long).',
      })
    }
    if (allShort) {
      candidates.push({
        stateId: 'BEAR_ALIGN',
        priority: 70,
        reason: 'Commercials and specs positioned in the same direction (net short).',
      })
    }
  }

  if (!candidates.length) {
    return {
      state: MARKET_STATE_LABELS.MIXED,
      stateId: 'MIXED',
      reason: 'No unusual cross-cohort pattern — positioning mixed or within normal 3Y ranges.',
      metrics: { comm, spec, retail },
      attentionScore: 5,
    }
  }

  candidates.sort((a, b) => b.priority - a.priority)
  const top = candidates[0]
  return {
    state: MARKET_STATE_LABELS[top.stateId] || top.stateId,
    stateId: top.stateId,
    reason: top.reason,
    metrics: { comm, spec, retail },
    attentionScore: top.priority,
    allCandidates: candidates,
  }
}

/** Map instrument id → market state for scanner. */
export function buildMarketStatesIndex(legacyStore, marketIds, asOfDate = null) {
  const instruments = legacyStore?.instruments || {}
  const out = new Map()
  for (const marketId of marketIds || []) {
    const inst = instruments[marketId]
    if (!inst) {
      out.set(marketId, {
        market: marketId,
        state: MARKET_STATE_LABELS.MIXED,
        stateId: 'MIXED',
        reason: 'No legacy COT instrument mapping.',
        attentionScore: 0,
      })
      continue
    }
    const classified = classifyMarketState(inst, asOfDate)
    out.set(marketId, { market: marketId, ...classified })
  }
  return out
}

export function attentionSortedStates(statesIndex) {
  return [...statesIndex.values()]
    .filter((s) => s.attentionScore > 5)
    .sort((a, b) => b.attentionScore - a.attentionScore || a.market.localeCompare(b.market))
}
