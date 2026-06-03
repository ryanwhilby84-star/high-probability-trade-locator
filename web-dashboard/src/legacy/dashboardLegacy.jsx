import React from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import { computeInstrumentIntelligence, RELATED_SECTOR_PEERS } from '../marketIntelligence.js'
import { buildMarketBriefing } from '../marketBriefing.js'
import { MacroRelationshipMap, MacroRelationshipOverlayChart, humanMacroMapUnavailableReason } from '../MacroRelationshipMap.jsx'
import { expectsMacroRelationshipMap, marketsMacroAlign, readMacroFreshness, resolveMacroRelationshipMap } from '../macroRelationshipMapData.js'
import { MacroHealthPanel } from '../components/MacroHealthPanel.jsx'
import { LiveMarketContextSection } from '../LiveMarketContextSection.jsx'
import { buildInstitutionalDecisionDigest } from '../institutionalPositioningDigest.js'
import { buildWeekBackdropDigest } from '../macroReadableDigest.js'
import {
  TRACKED_MARKET_IDS as TRACKED_MARKETS,
  canonicalMarketId as canonical,
  normalizeReportDate as normalizeDate,
  recordCalendarDate,
  recordCotReportDate,
  resolveRowForMarketWeek,
  defaultDashboardWeek,
  logCotResolutionForWeek,
  isCotRowResolved,
} from '../marketResolution.js'
import '../tailwind.css'
import '../styles.css'

const rowDate = (r = {}) => recordCalendarDate(r) || recordCotReportDate(r) || ''
const display = (v) => (v === null || v === undefined || v === '' ? '—' : v)

/** @param {unknown} sig */
function formatRegimeSignalLabel(sig) {
  const s = String(sig || '').trim().replace(/_/g, ' ')
  if (!s) return ''
  return s.charAt(0).toUpperCase() + s.slice(1)
}

function hasRealValue(v) {
  if (v === null || v === undefined) return false
  if (typeof v === 'string') {
    const s = v.trim().toLowerCase()
    if (!s || s === 'n/a' || s === 'nan' || s === 'null' || s === 'undefined' || s === '—') return false
  }
  return true
}

const fmtNum = (v) => {
  if (v === null || v === undefined || v === '' || v === 'N/A') return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return String(v)
  return n.toLocaleString(undefined, { maximumFractionDigits: 0 })
}

const fmtHistPct = (v) => {
  if (v === null || v === undefined || v === '') return '—'
  const n = Number(v)
  return Number.isFinite(n) ? n.toFixed(1) : '—'
}

function traderHistoryPositioningRead(ctx, insufficient) {
  if (insufficient) {
    return 'There is not enough COT history in this slice to say where positioning sits versus old extremes — treat size and conviction with extra care.'
  }
  const net = Number(ctx?.current_net_percentile)
  const lo = Number(ctx?.current_long_percentile)
  const sh = Number(ctx?.current_short_percentile)
  const bits = []
  if (Number.isFinite(net)) {
    if (net >= 93) bits.push('Net positioning is near the top of your loaded sample — the street is very long-biased.')
    else if (net >= 75) bits.push('Net is clearly on the high side of its usual range — leaning long-heavy, but not necessarily at a blow-off.')
    else if (net >= 56) bits.push('Net is moderately long versus this history window.')
    else if (net >= 44) bits.push('Net is near the middle of its historical band for this dataset.')
    else if (net >= 25) bits.push('Net is moderately short-biased versus this window.')
    else if (net >= 8) bits.push('Net is on the low side of its range — participants are leaning short.')
    else bits.push('Net is down near historical lows for this sample — structurally bearish positioning.')
  }
  if (Number.isFinite(lo) && lo >= 90) bits.push('Gross longs are unusually large versus this history.')
  if (Number.isFinite(sh) && sh >= 90) bits.push('Gross shorts are unusually large versus this history.')
  return bits.length ? bits.join(' ') : 'Use the deep audit if you need the exact percentile table.'
}

/** Old JSON used flat all_time_* + historical_*; new JSON uses nested contexts. */
function legacyHistoryContextsFromRow(row) {
  if (!row) return null
  const hasLegacy =
    row.all_time_long_max != null
    || row.all_time_long_min != null
    || row.all_time_net_max != null
    || row.all_time_net_min != null
  if (!hasLegacy) return null
  const exp = {
    rows_used: row.historical_percentile_n_joint ?? null,
    earliest_report_date: row.historical_series_earliest_date ?? null,
    latest_report_date: row.historical_series_report_date ?? null,
    long_max: row.all_time_long_max ?? null,
    long_min: row.all_time_long_min ?? null,
    short_max: row.all_time_short_max ?? null,
    short_min: row.all_time_short_min ?? null,
    net_max: row.all_time_net_max ?? null,
    net_min: row.all_time_net_min ?? null,
    current_long_percentile: row.current_long_percentile ?? null,
    current_short_percentile: row.current_short_percentile ?? null,
    current_net_percentile: row.current_net_percentile ?? null,
    current_net_rank_label: row.current_net_rank_label ?? null,
    summary: row.historical_context_summary ?? null,
  }
  return { expanding: exp, full_loaded: { ...exp } }
}

function enrichRowHistoryContext(row) {
  if (!row || typeof row !== 'object') return row
  const ex = row.expanding_history_context
  const fl = row.full_loaded_history_context
  if (ex && fl) return row
  const leg = legacyHistoryContextsFromRow(row)
  if (!leg) return row
  return {
    ...row,
    expanding_history_context: ex || leg.expanding,
    full_loaded_history_context: fl || leg.full_loaded,
  }
}

function isInsufficientHistContext(ctx) {
  if (!ctx || typeof ctx !== 'object') return true
  const ru = ctx.rows_used
  if (ru == null || ru === '' || Number(ru) < 1) return true
  const hasAny = [ctx.long_max, ctx.long_min, ctx.short_max, ctx.short_min, ctx.net_max, ctx.net_min].some(
    (v) => v != null && v !== '' && Number.isFinite(Number(v)),
  )
  return !hasAny
}

const INSUFFICIENT_HIST = 'Insufficient history loaded'

function fmtHistOrInsufficient(v, insufficient) {
  if (insufficient) return INSUFFICIENT_HIST
  return fmtNum(v)
}

function fmtHistPctOrInsufficient(v, insufficient) {
  if (insufficient) return INSUFFICIENT_HIST
  return fmtHistPct(v)
}

const pctLong = (longV, shortV) => {
  const L = Number(longV)
  const S = Number(shortV)
  if (!Number.isFinite(L) || !Number.isFinite(S) || L + S <= 0) return '—'
  return `${((100 * L) / (L + S)).toFixed(1)}%`
}

/** UI tone for positioning state (Phase 3 colour coding) */
const stateToneClass = (state) => {
  const s = String(state || '')
  if (s === 'N/A' || s.startsWith('Neutral')) return 'tone-neutral'
  if (s === 'Bullish Strengthening') return 'tone-green'
  if (s === 'Accumulation') return 'tone-teal'
  if (s === 'Bearish Strengthening') return 'tone-red'
  if (s === 'Distribution') return 'tone-rose'
  if (['Short Covering', 'Bearish Improving'].includes(s)) return 'tone-amber'
  if (['Bullish Weakening', 'Bullish Softening'].includes(s)) return 'tone-amber'
  if (s.startsWith('Transition')) return 'tone-amber'
  return 'tone-neutral'
}

/** White Oak–style history depth (13-row window for Max / Min / 13-wk avg) */
const HISTORY_WEEKS = 13

function rowOiTotal(r) {
  const L = Number(r?.long_value)
  const S = Number(r?.short_value)
  if (!Number.isFinite(L) || !Number.isFinite(S)) return NaN
  return L + S
}

function pctLongNumber(r) {
  const L = Number(r?.long_value)
  const S = Number(r?.short_value)
  if (!Number.isFinite(L) || !Number.isFinite(S) || L + S <= 0) return NaN
  return (100 * L) / (L + S)
}

function pctShortNumber(r) {
  const p = pctLongNumber(r)
  return Number.isFinite(p) ? 100 - p : NaN
}

function buildMarketHistory(allRows, market, asOfDate, maxWeeks = HISTORY_WEEKS) {
  const asOf = normalizeDate(asOfDate)
  const byCotDate = new Map()
  allRows
    .filter(
      (r) =>
        canonical(r.market || r.raw_cftc_market_name || '') === market
        && normalizeDate(rowDate(r)) <= asOf
        && isCotRowResolved(r),
    )
    .forEach((r) => {
      const cotKey = recordCotReportDate(r) || normalizeDate(rowDate(r))
      if (!cotKey) return
      const calKey = normalizeDate(rowDate(r))
      const prev = byCotDate.get(cotKey)
      if (!prev || calKey.localeCompare(normalizeDate(rowDate(prev))) > 0) {
        byCotDate.set(cotKey, r)
      }
    })
  const rows = [...byCotDate.values()].sort((a, b) =>
    recordCotReportDate(a).localeCompare(recordCotReportDate(b)),
  )
  return rows.slice(-maxWeeks)
}

function computeWoWindowStats(chronoRows) {
  const pick = (fn) => chronoRows.map(fn).filter((x) => Number.isFinite(x))
  const longs = pick((r) => Number(r.long_value))
  const shorts = pick((r) => Number(r.short_value))
  const totals = pick((r) => rowOiTotal(r))
  const nets = pick((r) => Number(r.net_value))
  const pLongs = pick((r) => pctLongNumber(r))
  const pShorts = pick((r) => pctShortNumber(r))
  const w1s = pick((r) => {
    const nd = Number(r._netDelta1w)
    if (Number.isFinite(nd)) return nd
    return Number(r.one_week_net_change)
  })
  const longDeltas = pick((r) => Number(r._longDelta1w))
  const shortDeltas = pick((r) => Number(r._shortDelta1w))
  const totalDeltas = pick((r) => Number(r._totalDelta1w))
  const stat = (arr) => {
    if (!arr.length) return { max: NaN, min: NaN, avg: NaN }
    return { max: Math.max(...arr), min: Math.min(...arr), avg: arr.reduce((a, b) => a + b, 0) / arr.length }
  }
  return {
    long: stat(longs),
    short: stat(shorts),
    total: stat(totals),
    pctLong: stat(pLongs),
    pctShort: stat(pShorts),
    net: stat(nets),
    w1: stat(w1s),
    longDelta: stat(longDeltas),
    shortDelta: stat(shortDeltas),
    totalDelta: stat(totalDeltas),
    windowN: chronoRows.length,
  }
}

const LEG_FLOW_EPS = 1

/** 0 = neutral … 4 = extreme intensity band for COT heatmap cells */
function rank01(value, min, max) {
  const v = Number(value)
  const lo = Number(min)
  const hi = Number(max)
  if (!Number.isFinite(v) || !Number.isFinite(lo) || !Number.isFinite(hi)) return 0
  if (hi === lo) return v >= lo ? 0.5 : 0
  return Math.max(0, Math.min(1, (v - lo) / (hi - lo)))
}

function bandFromRank(t) {
  if (t < 0.12) return 0
  if (t < 0.32) return 1
  if (t < 0.52) return 2
  if (t < 0.74) return 3
  return 4
}

/**
 * @param {number} value
 * @param {number} min
 * @param {number} max
 * @param {'bull'|'bear'|'short'|'oi'|'neutral'} direction palette
 */
function heatClass(value, min, max, direction = 'bull') {
  const band = bandFromRank(rank01(value, min, max))
  return `cot-heat-${direction}-${band}`
}

function heatCellProps(className = 'cot-heat-neutral-0') {
  return { className }
}

function heatClassSigned(value, values, invert = false) {
  const raw = Number(value)
  if (!Number.isFinite(raw)) return 'cot-heat-neutral-0'
  const v = invert ? -raw : raw
  if (Math.abs(v) <= LEG_FLOW_EPS) return 'cot-heat-neutral-0'
  const palette = v > 0 ? 'bull' : 'bear'
  const absArr = values
    .map((x) => Math.abs(invert ? -Number(x) : Number(x)))
    .filter((x) => Number.isFinite(x))
  const mag = Math.abs(v)
  if (!absArr.length) return `cot-heat-${palette}-2`
  const band = bandFromRank(rank01(mag, Math.min(...absArr), Math.max(...absArr)))
  return `cot-heat-${palette}-${Math.max(1, band)}`
}

function signedDeltaHeat(delta, deltas, invert = false) {
  return heatCellProps(heatClassSigned(delta, deltas, invert))
}

/** Long level vs 13W range: high = green intensity, low = pale red, mid = neutral. */
function longLevelHeat(value, min, max) {
  const v = Number(value)
  const lo = Number(min)
  const hi = Number(max)
  if (!Number.isFinite(v) || !Number.isFinite(lo) || !Number.isFinite(hi)) return heatCellProps()
  if (hi === lo) return heatCellProps('cot-heat-neutral-1')
  const t = rank01(v, lo, hi)
  if (t >= 0.52) {
    const band = bandFromRank((t - 0.52) / 0.48)
    return heatCellProps(`cot-heat-bull-${Math.max(1, band)}`)
  }
  if (t <= 0.48) {
    const band = bandFromRank((0.48 - t) / 0.48)
    return heatCellProps(`cot-heat-bear-${Math.max(1, band)}`)
  }
  const band = bandFromRank(1 - Math.abs(t - 0.5) / 0.04)
  return heatCellProps(`cot-heat-neutral-${Math.min(2, band)}`)
}

/** Short level vs 13W range: high = red intensity, low = pale green, mid = neutral. */
function shortLevelHeat(value, min, max) {
  const v = Number(value)
  const lo = Number(min)
  const hi = Number(max)
  if (!Number.isFinite(v) || !Number.isFinite(lo) || !Number.isFinite(hi)) return heatCellProps()
  if (hi === lo) return heatCellProps('cot-heat-neutral-1')
  const t = rank01(v, lo, hi)
  if (t >= 0.52) {
    const band = bandFromRank((t - 0.52) / 0.48)
    return heatCellProps(`cot-heat-bear-${Math.max(1, band)}`)
  }
  if (t <= 0.48) {
    const band = bandFromRank((0.48 - t) / 0.48)
    return heatCellProps(`cot-heat-bull-${Math.max(1, band)}`)
  }
  const band = bandFromRank(1 - Math.abs(t - 0.5) / 0.04)
  return heatCellProps(`cot-heat-neutral-${Math.min(2, band)}`)
}

/** Net level vs 13W range: positive high = green, negative low = red, mid = grey/amber. */
function netLevelHeat(value, min, max) {
  const v = Number(value)
  const lo = Number(min)
  const hi = Number(max)
  if (!Number.isFinite(v) || !Number.isFinite(lo) || !Number.isFinite(hi)) return heatCellProps()
  if (hi === lo) {
    return heatCellProps(v >= 0 ? 'cot-heat-bull-2' : 'cot-heat-bear-2')
  }
  const mid = (hi + lo) / 2
  const span = Math.max((hi - lo) / 2, 1)
  if (Math.abs(v - mid) <= span * 0.1) {
    return heatCellProps(`cot-heat-neutral-${bandFromRank(0.35)}`)
  }
  if (v > mid) {
    const band = bandFromRank(rank01(v, mid, hi))
    return heatCellProps(`cot-heat-bull-${Math.max(1, band)}`)
  }
  const band = bandFromRank(rank01(v, lo, mid))
  return heatCellProps(`cot-heat-bear-${Math.max(1, band)}`)
}

/** Total OI level vs 13W range: high = gold intensity, low = pale neutral. */
function totalOiLevelHeat(value, min, max) {
  const v = Number(value)
  const lo = Number(min)
  const hi = Number(max)
  if (!Number.isFinite(v) || !Number.isFinite(lo) || !Number.isFinite(hi)) return heatCellProps()
  if (hi === lo) return heatCellProps('cot-heat-oi-2')
  const band = bandFromRank(rank01(v, lo, hi))
  return heatCellProps(`cot-heat-oi-${band}`)
}

function netHeatStyle(net, min, max) {
  return netLevelHeat(net, min, max)
}

function numOrNaN(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : NaN
}

/** Sort oldest → newest, attach weekly leg deltas + participation label (UI-only; uses existing row fields). */
function enrichCotHistoryWithParticipation(chrono) {
  const asc = [...chrono].sort((a, b) => normalizeDate(rowDate(a)).localeCompare(normalizeDate(rowDate(b))))
  return asc.map((row, i) => {
    const prev = i > 0 ? asc[i - 1] : null
    let ld = numOrNaN(row.one_week_long_change)
    let sd = numOrNaN(row.one_week_short_change)
    let nd = numOrNaN(row.one_week_net_change)
    if (prev) {
      const l0 = numOrNaN(prev.long_value)
      const l1 = numOrNaN(row.long_value)
      const s0 = numOrNaN(prev.short_value)
      const s1 = numOrNaN(row.short_value)
      if (!Number.isFinite(ld) && Number.isFinite(l0) && Number.isFinite(l1)) ld = l1 - l0
      if (!Number.isFinite(sd) && Number.isFinite(s0) && Number.isFinite(s1)) sd = s1 - s0
      if (!Number.isFinite(nd) && Number.isFinite(l1) && Number.isFinite(s1) && Number.isFinite(l0) && Number.isFinite(s0)) {
        nd = l1 - s1 - (l0 - s0)
      }
    }
    const t1 = rowOiTotal(row)
    const t0 = prev ? rowOiTotal(prev) : NaN
    const td = Number.isFinite(t1) && Number.isFinite(t0) ? t1 - t0 : NaN
    const part = classifyParticipationFlow(ld, sd, nd)
    return { ...row, _longDelta1w: ld, _shortDelta1w: sd, _netDelta1w: nd, _totalDelta1w: td, _participation: part }
  })
}

function classifyParticipationFlow(ld, sd, nd) {
  if (!Number.isFinite(ld) || !Number.isFinite(sd)) {
    return {
      category: '—',
      summary: 'Leg deltas unavailable for this row (missing prior week in window or non-numeric legs).',
      tooltip: 'Compare with adjacent weeks once a full two-week window is visible.',
      tone: 'neutral',
    }
  }
  const lu = ld > LEG_FLOW_EPS
  const lDown = ld < -LEG_FLOW_EPS
  const lFlat = Math.abs(ld) <= LEG_FLOW_EPS
  const su = sd > LEG_FLOW_EPS
  const sDown = sd < -LEG_FLOW_EPS
  const sFlat = Math.abs(sd) <= LEG_FLOW_EPS
  const netUp = Number.isFinite(nd) && nd > LEG_FLOW_EPS
  const netDown = Number.isFinite(nd) && nd < -LEG_FLOW_EPS

  let category = 'Neutral Rotation'
  let summary = 'Mixed or muted week-to-week change in managed-money legs.'
  let tone = 'neutral'

  if (lu && sDown) {
    category = 'Short Covering'
    summary = 'Longs added while shorts were cut — classic covering flow; often supportive for price discovery vs prior week.'
    tone = 'bull'
  } else if (lDown && su) {
    category = 'Bearish Strengthening'
    summary = 'Longs reduced while shorts built — institutions leaning more bearish on the week.'
    tone = 'bear'
  } else if (lu && su) {
    if (netUp && ld >= su * 0.85) {
      category = 'Bullish Strengthening'
      summary = 'Both legs grew but net exposure rose with longs leading — participation expanding on the long side.'
      tone = 'bull'
    } else {
      category = 'Two-Way Expansion'
      summary = 'Longs and shorts both increased — open interest building on both sides; expect choppier two-way trade.'
      tone = 'expand'
    }
  } else if (lDown && sDown) {
    category = 'Participation Collapse'
    summary = 'Both longs and shorts contracted — open interest shrinking; conviction/participation cooling.'
    tone = 'collapse'
  } else if (lu && sFlat) {
    category = 'Long Build'
    summary = 'Longs rose with shorts roughly flat — incremental long accumulation.'
    tone = 'bull'
  } else if (lFlat && sDown) {
    category = 'Short Covering'
    summary = 'Shorts were cut with longs roughly flat — covering pressure on the short side.'
    tone = 'bull'
  } else if (lDown && sFlat) {
    category = 'Bearish Strengthening'
    summary = 'Longs were cut while shorts were flat — long liquidation / risk-reduction bias.'
    tone = 'bear'
  } else if (lFlat && su) {
    category = 'Bearish Strengthening'
    summary = 'Shorts built with longs flat — fresh short participation without long support.'
    tone = 'bear'
  }

  const netPhrase = netUp ? 'Net exposure increased week-on-week.' : netDown ? 'Net exposure decreased week-on-week.' : 'Net exposure was little changed week-on-week.'
  const pressure = netUp && (tone === 'bull' || tone === 'expand') ? 'Pressure on net positioning is building.' : netDown && tone === 'bear' ? 'Bearish pressure is building on net.' : 'Pressure is relatively balanced vs the prior report.'
  const tooltip = `${summary} ${netPhrase} ${pressure}`

  return { category, summary, tooltip, tone }
}

function participationCellStyle(tone) {
  const map = {
    bull: 'cot-heat-bull-3',
    bear: 'cot-heat-bear-3',
    expand: 'cot-heat-oi-3',
    collapse: 'cot-heat-neutral-2',
    neutral: 'cot-heat-neutral-1',
  }
  return heatCellProps(map[tone] || map.neutral)
}

function deltaArrow(d) {
  const x = Number(d)
  if (!Number.isFinite(x)) return '↔'
  if (x > LEG_FLOW_EPS) return '↑'
  if (x < -LEG_FLOW_EPS) return '↓'
  return '↔'
}

function fmtDeltaCell(d) {
  const x = Number(d)
  if (!Number.isFinite(x)) return '—'
  const a = deltaArrow(x)
  const n = Math.abs(x) >= 1000 ? x.toLocaleString(undefined, { maximumFractionDigits: 0 }) : x.toFixed(0)
  return `${a} ${n}`
}

function fmtPct1(v) {
  if (!Number.isFinite(v)) return '—'
  return `${v.toFixed(1)}%`
}

function fmtStatNum(v) {
  if (!Number.isFinite(v)) return '—'
  return Math.abs(v) >= 1000 ? v.toLocaleString(undefined, { maximumFractionDigits: 0 }) : v.toFixed(1)
}

function MacroAuditBlock({ audit }) {
  if (!audit || typeof audit !== 'object') {
    return <div className="macro-audit-panel"><strong>Macro audit</strong> — unavailable.</div>
  }
  if (audit.available === false) {
    return (
      <div className="macro-audit-panel">
        <strong>Macro audit</strong>
        <p style={{ margin: '8px 0 0', color: '#94a3b8', fontSize: '0.85rem' }}>
          {display(audit.reason)}
          {Array.isArray(audit.missing_inputs) && audit.missing_inputs.length ? ` Missing: ${audit.missing_inputs.join(', ')}.` : ''}
        </p>
      </div>
    )
  }
  const L = audit.levels || {}
  const D1 = audit.one_week_deltas_pp || {}
  const fedCh = audit.fed_funds_changes_pp || {}
  const th = audit.thresholds || {}
  const sm = audit.score_mapping || {}
  const rr = audit.resolved_regime || {}
  const offLines = Array.isArray(audit.risk_off_contribution_lines) ? audit.risk_off_contribution_lines : []
  const onLines = Array.isArray(audit.risk_on_contribution_lines) ? audit.risk_on_contribution_lines : []
  const ct = audit.counts || {}
  return (
    <div className="macro-audit-panel">
      <h4 style={{ margin: '0 0 12px', color: '#94a3b8', fontSize: '0.75rem', letterSpacing: '0.08em', textTransform: 'uppercase' }}>
        Treasury snapshot &amp; macro score (audit)
      </h4>
      <div style={{ fontSize: '0.82rem', color: '#94a3b8', marginBottom: '12px' }}>
        Snapshot date: <strong style={{ color: '#e2e8f0' }}>{display(audit.rates_snapshot_date)}</strong>. {display(audit.fred_series_note)}
      </div>
      <div style={{ fontSize: '0.78rem', color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '6px' }}>Levels &amp; 1w deltas (% pts)</div>
      <ul style={{ margin: '0 0 12px', paddingLeft: '18px', fontSize: '0.84rem', color: '#cbd5e1' }}>
        <li>DGS2: {display(L.dgs2)} · 1w {display(D1.dgs2)}</li>
        <li>DGS10: {display(L.dgs10)} · 1w {display(D1.dgs10)}</li>
        <li>DGS30: {display(L.dgs30)} · 1w {display(D1.dgs30)}</li>
        <li>Curve (T10Y2Y / synthetic): {display(L.t10y2y_or_synthetic)} · 1w {display(D1.t10y2y)}</li>
        <li>Fed funds (DFF): {display(L.fed_funds_dff)} · 1w {display(fedCh['1w'])} · 4w {display(fedCh['4w'])}</li>
      </ul>
      <div style={{ fontSize: '0.75rem', color: '#64748b', marginBottom: '10px', whiteSpace: 'pre-wrap' }}>
        {display(th.yield_rule)} {display(th.curve_rule)}
      </div>
      <div style={{ fontSize: '0.8rem', color: '#94a3b8', marginBottom: '8px' }}>
        Restrictive-side checks passed: <strong style={{ color: '#e2e8f0' }}>{display(ct.risk_off_aligned)}</strong>
        {' · '}
        Easing-side checks passed: <strong style={{ color: '#e2e8f0' }}>{display(ct.risk_on_aligned)}</strong>
        {' · '}
        Resolved lean: <strong style={{ color: '#e2e8f0' }}>{display(audit.winner)}</strong>
      </div>
      {offLines.length ? (
        <ul style={{ margin: '0 0 10px', paddingLeft: '18px', fontSize: '0.8rem', color: '#fca5a5' }}>
          {offLines.map((line, i) => (
            <li key={`off-${i}`} style={{ marginBottom: '6px', whiteSpace: 'pre-wrap' }}>{line}</li>
          ))}
        </ul>
      ) : null}
      {onLines.length ? (
        <ul style={{ margin: '0 0 10px', paddingLeft: '18px', fontSize: '0.8rem', color: '#86efac' }}>
          {onLines.map((line, i) => (
            <li key={`on-${i}`} style={{ marginBottom: '6px', whiteSpace: 'pre-wrap' }}>{line}</li>
          ))}
        </ul>
      ) : null}
      <div style={{ fontSize: '0.84rem', color: '#cbd5e1', marginBottom: '8px' }}>
        <strong>Score mapping:</strong> {display(sm.formula)} — {display(sm.score_map_explained)} → tally = {display(sm.aligned_count_used_for_score)} →{' '}
        <strong>macro_score = {display(sm.macro_score_from_audit)}</strong>
      </div>
      <div style={{ fontSize: '0.84rem', color: '#cbd5e1' }}>
        <strong>Resolved (this row):</strong> {display(rr.macro_signal)} / score {display(rr.macro_score)} · {display(rr.rates_bias)} · curve{' '}
        {display(rr.curve_state || rr.curve_context)} · policy {display(rr.policy_pressure)}
      </div>
      {rr.macro_rationale ? (
        <div style={{ marginTop: '10px', fontSize: '0.82rem', color: '#94a3b8', lineHeight: 1.5, whiteSpace: 'pre-wrap' }}>
          <strong style={{ color: '#cbd5e1' }}>Macro rationale:</strong> {display(rr.macro_rationale)}
        </div>
      ) : null}
      {rr.liquidity_regime ? (
        <div style={{ marginTop: '8px', fontSize: '0.78rem', color: '#64748b' }}>
          <strong>Liquidity regime:</strong> {display(rr.liquidity_regime)}
        </div>
      ) : null}
      {audit.tie_break_note ? <div style={{ marginTop: '10px', fontSize: '0.78rem', color: '#64748b', whiteSpace: 'pre-wrap' }}>{audit.tie_break_note}</div> : null}
    </div>
  )
}

function GlobalMarketRegimePanel({ regime, weekLabel }) {
  const g = regime || {}
  const digest = React.useMemo(() => buildWeekBackdropDigest(regime), [regime])
  const snap = display(g?.rates_snapshot_date)
  const bullets = [
    g?.rates_pressure,
    g?.liquidity_regime,
    g?.usd_impulse && String(g.usd_impulse).toLowerCase().includes('not modeled') ? null : g?.usd_impulse,
    g?.inflation_regime,
  ]
    .filter((x) => hasRealValue(x))
    .slice(0, 4)
  const chipSignalRaw =
    g?.resolved_macro_signal != null && String(g.resolved_macro_signal).trim() !== ''
      ? String(g.resolved_macro_signal).trim()
      : ''
  const chipSignal = chipSignalRaw ? formatRegimeSignalLabel(chipSignalRaw) : null
  const chipScore =
    g?.resolved_macro_score != null && g.resolved_macro_score !== '' && Number.isFinite(Number(g.resolved_macro_score))
      ? Number(g.resolved_macro_score)
      : null
  const riskOne = hasRealValue(g?.risk_regime) ? clipUiStr(g.risk_regime, 140) : null
  return (
    <section className="regime-strip-section" aria-label="Global market regime">
      <h2 className="regime-strip-heading">Week backdrop</h2>
      <p className="regime-strip-meta">
        {weekLabel ? `COT week ${weekLabel}.` : ''}
        {snap !== '—' ? ` Rates snapshot ${snap}.` : ''}
      </p>
      {!regime ? (
        <p className="regime-strip-empty">Week backdrop block is missing — rebuild the weekly confluence export.</p>
      ) : (
        <>
          <p className="regime-strip-one">
            <span className="regime-strip-k">Regime</span>
            {digest.regimeLabel}
          </p>
          {digest.convictionLevel ? (
            <p className="regime-strip-one">
              <span className="regime-strip-k">Conviction</span>
              {digest.convictionLevel}
              {digest.convictionDetail ? ` (${clipUiStr(digest.convictionDetail, 120)})` : ''}
            </p>
          ) : null}
          <details className="regime-deep">
            <summary className="regime-deep-sum">Export detail &amp; audit (optional)</summary>
            <div className="regime-deep-body">
              <div className="regime-fast-row" aria-label="Resolved tags">
                {chipSignal ? (
                  <span className="regime-chip regime-chip-signal" title="Resolved macro signal">
                    {chipSignal}
                  </span>
                ) : null}
                {chipScore != null ? (
                  <span className="regime-chip" title="Resolved macro score (same-week model)">
                    Score {chipScore}
                  </span>
                ) : null}
                {hasRealValue(g.curve_state) ? <span className="regime-chip">Curve {display(g.curve_state)}</span> : null}
              </div>
              {riskOne ? <p className="regime-strip-one">{riskOne}</p> : null}
              {hasRealValue(g.summary) ? <p className="regime-strip-summary">{clipUiStr(g.summary, 280)}</p> : null}
              {hasRealValue(g.macro_rationale) ? (
                <p className="regime-strip-summary" style={{ fontSize: '0.82rem', color: '#94a3b8', marginTop: 0 }}>
                  <strong style={{ color: '#cbd5e1' }}>Macro rationale:</strong> {display(g.macro_rationale)}
                </p>
              ) : null}
              <ul className="regime-strip-bullets">
                {bullets.length ? (
                  bullets.map((b, i) => <li key={i}>{display(b)}</li>)
                ) : (
                  <li>No extra regime bullets in this export.</li>
                )}
              </ul>
              {hasRealValue(g?.summary_technical) ? (
                <p
                  className="regime-strip-summary"
                  style={{ fontSize: '0.75rem', color: '#64748b', marginTop: '12px', lineHeight: 1.5, whiteSpace: 'pre-wrap' }}
                >
                  <strong style={{ color: '#94a3b8' }}>Technical alignment (audit only):</strong> {display(g.summary_technical)}
                </p>
              ) : null}
              {hasRealValue(g?.rates_pressure_technical) &&
              String(g.rates_pressure_technical).trim() !== String(g.rates_pressure || '').trim() ? (
                <p
                  className="regime-strip-summary"
                  style={{ fontSize: '0.75rem', color: '#64748b', marginTop: '8px', lineHeight: 1.5, whiteSpace: 'pre-wrap' }}
                >
                  <strong style={{ color: '#94a3b8' }}>Rates feed (raw):</strong> {display(g.rates_pressure_technical)}
                </p>
              ) : null}
            </div>
          </details>
        </>
      )}
    </section>
  )
}

function HistoricalModeBlock({ title, subtitle, ctx, headingStyle }) {
  const insufficient = isInsufficientHistContext(ctx)
  const blurb = traderHistoryPositioningRead(ctx, insufficient)
  return (
    <>
      <h4 className="wo-cot-section-title" style={headingStyle || undefined}>{title}</h4>
      <p className="wo-cot-hint wo-cot-hint-tight">{subtitle}</p>
      <p
        className="wo-cot-trader-blurb"
        style={{ margin: '0 0 14px', fontSize: '0.88rem', color: '#e2e8f0', lineHeight: 1.55 }}
      >
        {blurb}
      </p>
      <details className="hist-deep-audit" style={{ marginBottom: '6px' }}>
        <summary
          style={{
            cursor: 'pointer',
            fontSize: '0.78rem',
            color: '#94a3b8',
            listStyle: 'none',
            marginBottom: '10px',
          }}
        >
          Deep audit — percentile table &amp; engine summary
        </summary>
        <p className="wo-cot-meta-line">
          <strong>Rows used:</strong>{' '}
          {insufficient ? INSUFFICIENT_HIST : display(ctx?.rows_used)}
          {' · '}
          <strong>Date range:</strong>{' '}
          {insufficient ? (
            INSUFFICIENT_HIST
          ) : (
            <>
              {display(ctx?.earliest_report_date)} → {display(ctx?.latest_report_date)}
            </>
          )}
        </p>
        <div className="wo-cot-context-summary" style={{ whiteSpace: 'pre-wrap' }}>{display(ctx?.summary)}</div>
        <div className="wo-cot-hist-table-wrap">
          <table className="wo-cot-hist-table">
            <thead>
              <tr>
                <th />
                <th>Historical Max</th>
                <th>Historical Min</th>
                <th>Percentile (0–100)</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <th scope="row">Long</th>
                <td>{fmtHistOrInsufficient(ctx?.long_max, insufficient)}</td>
                <td>{fmtHistOrInsufficient(ctx?.long_min, insufficient)}</td>
                <td>{fmtHistPctOrInsufficient(ctx?.current_long_percentile, insufficient)}</td>
              </tr>
              <tr>
                <th scope="row">Short</th>
                <td>{fmtHistOrInsufficient(ctx?.short_max, insufficient)}</td>
                <td>{fmtHistOrInsufficient(ctx?.short_min, insufficient)}</td>
                <td>{fmtHistPctOrInsufficient(ctx?.current_short_percentile, insufficient)}</td>
              </tr>
              <tr>
                <th scope="row">Net</th>
                <td>{fmtHistOrInsufficient(ctx?.net_max, insufficient)}</td>
                <td>{fmtHistOrInsufficient(ctx?.net_min, insufficient)}</td>
                <td>{fmtHistPctOrInsufficient(ctx?.current_net_percentile, insufficient)}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <p className="wo-cot-meta-line" style={{ marginTop: '10px' }}>
          <strong>Net rank (historical):</strong>{' '}
          {insufficient ? INSUFFICIENT_HIST : display(ctx?.current_net_rank_label)}
        </p>
      </details>
    </>
  )
}

function clipUiStr(s, n) {
  const t = String(s || '').trim()
  if (t.length <= n) return t
  return `${t.slice(0, n - 1)}…`
}

/** Mirrors backend ui_pack when JSON predates server bundle. */
function buildFallbackUiPack(row) {
  const g = row?.global_market_regime && typeof row.global_market_regime === 'object' ? row.global_market_regime : null
  const intel = row?.instrument_intel_context && typeof row.instrument_intel_context === 'object' ? row.instrument_intel_context : {}
  const inter = row?.intermarket_impulse_context && typeof row.intermarket_impulse_context === 'object' ? row.intermarket_impulse_context : {}
  const sentTxt = String(intel.sentiment_interference || '')
  const su = sentTxt.toUpperCase()
  let sentiment = 'Distortion unclear'
  if (su.includes('EXTREME')) sentiment = 'Extreme distortion'
  else if (su.includes('HIGH') && (su.includes('DISTORTION') || su.includes('INTERFERENCE'))) sentiment = 'High distortion'
  else if (su.includes('MODERATE')) sentiment = 'Moderate distortion'
  else if (su.includes('LOW') && (su.includes('DISTORTION') || su.includes('INTERFERENCE') || su.includes('LOW.'))) sentiment = 'Low distortion'

  const mr = String(row?.macro_regime || '').toLowerCase()
  let macroLabel = 'Macro unavailable'
  if (mr && mr !== 'n/a') {
    if (mr.includes('risk_on')) macroLabel = 'Supportive'
    else if (mr.includes('risk_off')) macroLabel = 'Restrictive'
    else macroLabel = 'Mixed / neutral'
  }

  const ps = String(row?.positioning_state || '').trim()
  const cotFlow = ps && ps.toUpperCase() !== 'N/A' ? ps : String(row?.cot_bias || 'N/A')

  const conf = String(inter.intermarket_confirmation || '').toUpperCase()
  const impulseDisp = hasRealValue(inter.intermarket_confirmation) ? String(inter.intermarket_confirmation) : 'N/A'

  let regime = 'Unknown'
  if (g?.resolved_macro_signal) {
    const rs = String(g.resolved_macro_signal).replace(/_/g, ' ')
    regime = rs.charAt(0).toUpperCase() + rs.slice(1)
  } else if (row?.macro_regime && String(row.macro_regime).toUpperCase() !== 'N/A') {
    regime = String(row.macro_regime).replace(/_/g, ' ')
  }

  let environment = 'Balanced'
  if (conf === 'CONFIRMING' && sentiment.includes('Low')) environment = 'Constructive'
  else if (conf === 'MIXED' || sentiment.includes('Moderate')) environment = 'Mixed — monitor alignment'
  else if (['DIVERGING', 'WARNING'].includes(conf) || sentiment.includes('High') || sentiment.includes('Extreme')) {
    environment = 'Conflicted / elevated noise'
  } else if (macroLabel === 'Restrictive' && conf !== 'CONFIRMING') environment = 'Cautious backdrop'

  const psLower = ps.toLowerCase()
  let cotT = 'neutral'
  if (psLower.includes('strengthening') && psLower.includes('bear')) cotT = 'warn'
  else if (psLower.includes('strengthening') && psLower.includes('bull')) cotT = 'support'
  else if (psLower.includes('weakening') || psLower.includes('distribution') || psLower.includes('transition')) cotT = 'caution'

  let macroT = 'neutral'
  if (mr.includes('risk_on')) macroT = 'support'
  else if (mr.includes('risk_off')) macroT = 'caution'

  let sentT = 'neutral'
  if (su.includes('EXTREME') || (su.includes('HIGH') && su.replace(/\s/g, '').includes('DISTORTION'))) sentT = 'warn'
  else if (su.includes('MODERATE')) sentT = 'caution'
  else if (su.includes('LOW')) sentT = 'support'

  let impT = 'neutral'
  if (conf === 'CONFIRMING') impT = 'support'
  else if (conf === 'MIXED') impT = 'caution'
  else if (conf === 'DIVERGING' || conf === 'WARNING') impT = 'warn'

  const regT = macroT
  let envT = 'neutral'
  const el = environment.toLowerCase()
  if (el.includes('conflicted') || el.includes('monitor')) envT = 'caution'
  else if (el.includes('hostile') || el.includes('elevated')) envT = 'warn'
  else if (el.includes('constructive')) envT = 'support'

  const macro_impact_bullets = []
  if (g) {
    if (hasRealValue(g.rates_pressure)) macro_impact_bullets.push(clipUiStr(g.rates_pressure, 72))
    if (hasRealValue(g.liquidity_regime)) macro_impact_bullets.push(clipUiStr(g.liquidity_regime, 72))
    const usd = g.usd_impulse
    if (hasRealValue(usd) && !String(usd).toLowerCase().includes('not modeled')) {
      macro_impact_bullets.push(clipUiStr(usd, 72))
    }
    if (hasRealValue(g.inflation_regime) && macro_impact_bullets.length < 4) {
      macro_impact_bullets.push(clipUiStr(g.inflation_regime, 72))
    }
  }
  if (macro_impact_bullets.length < 2 && row?.macro_regime && String(row.macro_regime).toUpperCase() !== 'N/A') {
    macro_impact_bullets.unshift(`Macro label: ${row.macro_regime}`)
  }

  const flPct = row?.full_loaded_history_context?.current_net_percentile ?? row?.current_net_percentile
  const sentiment_bullets = [`Interference: ${sentiment}`]
  const p = Number(flPct)
  if (Number.isFinite(p) && p >= 0 && p <= 100) {
    if (p >= 90 || p <= 10) sentiment_bullets.push('Positioning looks stretched versus its historical range')
    else if (p >= 75 || p <= 25) sentiment_bullets.push('Positioning shows a clear lean versus history')
    else sentiment_bullets.push('Positioning sits near the middle of its historical range')
  }
  if ((sentTxt || '').toLowerCase().includes('crowding')) sentiment_bullets.push('Crowding narrative flagged in text layer')

  const sup = (Array.isArray(inter.supporting_drivers) ? inter.supporting_drivers : []).slice(0, 4)
  const con = (Array.isArray(inter.conflicting_drivers) ? inter.conflicting_drivers : []).slice(0, 4)

  const news_bullets = []
  const nc = String(intel.news_catalysts || '')
  if (nc) {
    const raw = nc.replace('Recurring channels to scan (not exhaustive):', '').trim()
    raw.split(',').map((x) => x.trim()).filter((x) => x.length > 3).slice(0, 4).forEach((x) => news_bullets.push(clipUiStr(x, 88)))
  }
  if (!news_bullets.length) {
    news_bullets.push('News catalyst text not present on this row.')
  }

  const final_context_line = clipUiStr(`${environment}. Macro: ${macroLabel}. Impulse: ${impulseDisp}.`, 160)

  return {
    executive: {
      cot_flow: cotFlow,
      macro: macroLabel,
      sentiment,
      impulse: impulseDisp,
      regime,
      environment,
      tones: {
        cot: cotT,
        macro: macroT,
        sentiment: sentT,
        impulse: impT,
        regime: regT,
        environment: envT,
      },
    },
    macro_impact_bullets: macro_impact_bullets.slice(0, 4),
    news_catalyst_bullets: news_bullets.slice(0, 4),
    sentiment_bullets: sentiment_bullets.slice(0, 4),
    intermarket_supporting: sup,
    intermarket_conflicting: con,
    intermarket_impulse_score: inter.impulse_score != null && inter.impulse_score !== '' ? inter.impulse_score : null,
    final_context_line,
  }
}

function ExecutiveMarketStrip({ pack }) {
  const e = pack?.executive || {}
  const t = e.tones || {}
  const chips = [
    ['COT', e.cot_flow, t.cot],
    ['Macro', e.macro, t.macro],
    ['Sentiment', e.sentiment, t.sentiment],
    ['Impulse', e.impulse, t.impulse],
    ['Regime', e.regime, t.regime],
    ['Environment', e.environment, t.environment],
  ]
  return (
    <div className="exec-strip" role="group" aria-label="Executive summary">
      {chips.map(([lab, val, tone]) => (
        <div key={lab} className={`exec-chip exec-tone-${tone || 'neutral'}`}>
          <div className="exec-chip-lab">{lab}</div>
          <div className="exec-chip-val">{display(val)}</div>
        </div>
      ))}
    </div>
  )
}

function MarketBriefingPanel({ row, pack, peersByMarket, globalMarketRegime, latestParticipation }) {
  const intel = React.useMemo(
    () =>
      computeInstrumentIntelligence(
        row,
        pack,
        peersByMarket || {},
        globalMarketRegime,
        latestParticipation || null,
      ),
    [
      row,
      pack,
      peersByMarket,
      globalMarketRegime,
      latestParticipation?.category,
      latestParticipation?.summary,
    ],
  )
  const b = React.useMemo(() => buildMarketBriefing(row, pack, intel, globalMarketRegime), [row, pack, intel, globalMarketRegime])
  const Line = ({ k, v, multiline }) => (
    <div className="mbrief-row">
      <div className="mbrief-k">{k}</div>
      <div className={`mbrief-v${multiline ? ' mbrief-v--multiline' : ''}`}>{display(v)}</div>
    </div>
  )
  return (
    <section className="market-briefing" aria-label="Market briefing">
      <h4 className="market-briefing-title">At-a-glance briefing</h4>
      <p className="market-briefing-hint">Same data as the tables below — quick scan before you open detail blocks.</p>
      <div className="market-briefing-grid">
        <Line k="Bias" v={b.bias} />
        <Line k="Positioning" v={b.positioning} />
        <Line k="Pressure" v={b.pressure} />
        <Line k="Macro" v={b.macro} multiline />
        <Line k="Intermarket" v={b.intermarket} />
        <Line k="Event risk" v={b.eventRisk} />
        <Line k="Trade environment" v={b.tradeEnvironment} />
        <Line k="Watch next" v={b.watchNext} />
      </div>
    </section>
  )
}

function PositioningDecisionSupport({ row }) {
  const d = React.useMemo(() => buildInstitutionalDecisionDigest(row), [row])
  return (
    <div className="pds-wrap" aria-label="Institutional positioning read">
      <h4 className="pds-section-title">Institutional read</h4>
      <div className="pds-block">
        <div className="pds-block-title">Institutional positioning</div>
        <dl className="pds-dl">
          <dt>Current stance</dt>
          <dd>{display(d.positioning.stance)}</dd>
          <dt>Weekly change</dt>
          <dd>{display(d.positioning.weekly)}</dd>
          <dt>4-week change</dt>
          <dd>{display(d.positioning.fourWeek)}</dd>
          <dt>Meaning</dt>
          <dd>{display(d.positioning.meaning)}</dd>
        </dl>
      </div>
      <div className="pds-block">
        <div className="pds-block-title">Trader read</div>
        <p className="pds-one">{display(d.traderRead)}</p>
      </div>
      <div className="pds-block">
        <div className="pds-block-title">Action bias</div>
        <p className="pds-one">{display(d.actionBias)}</p>
      </div>
    </div>
  )
}

export function buildMarketHistoryForMarket(allRows, market, asOfDate, maxWeeks = HISTORY_WEEKS) {
  return buildMarketHistory(allRows, market, asOfDate, maxWeeks)
}

export { enrichRowHistoryContext, buildFallbackUiPack, stateToneClass }

function InstrumentDetail({
  row,
  historyRows,
  peersByMarket = {},
  globalMarketRegime = null,
  relationshipMapData = null,
  hideWeatherPlaceholder = false,
  workspaceMode = false,
  economicCalendar = null,
  weatherContext = null,
  weatherLoadError = null,
}) {
  const L = row?.long_value
  const S = row?.short_value
  const N = row?.net_value
  const chrono = historyRows || []
  const chronoEnriched = enrichCotHistoryWithParticipation(chrono)
  const latestParticipation = chronoEnriched.length ? chronoEnriched[chronoEnriched.length - 1]._participation : null
  const stats = computeWoWindowStats(chronoEnriched)
  const netsForHeat = chronoEnriched.map((r) => Number(r.net_value)).filter(Number.isFinite)
  const longsForHeat = chronoEnriched.map((r) => Number(r.long_value)).filter(Number.isFinite)
  const shortsForHeat = chronoEnriched.map((r) => Number(r.short_value)).filter(Number.isFinite)
  const netDeltasForHeat = chronoEnriched.map((r) => Number(r._netDelta1w)).filter(Number.isFinite)
  const longDeltasForHeat = chronoEnriched.map((r) => Number(r._longDelta1w)).filter(Number.isFinite)
  const shortDeltasForHeat = chronoEnriched.map((r) => Number(r._shortDelta1w)).filter(Number.isFinite)
  const totalDeltasForHeat = chronoEnriched.map((r) => Number(r._totalDelta1w)).filter(Number.isFinite)
  const totalsForHeat = chronoEnriched.map((r) => rowOiTotal(r)).filter(Number.isFinite)
  const displayDesc = [...chronoEnriched].reverse()
  const avgLabel = stats?.windowN >= HISTORY_WEEKS ? '13W Avg' : `13W Avg (${stats?.windowN || 0} reports)`
  const heatRanges = {
    long: {
      min: Number.isFinite(stats?.long?.min) ? stats.long.min : Math.min(...longsForHeat),
      max: Number.isFinite(stats?.long?.max) ? stats.long.max : Math.max(...longsForHeat),
    },
    short: {
      min: Number.isFinite(stats?.short?.min) ? stats.short.min : Math.min(...shortsForHeat),
      max: Number.isFinite(stats?.short?.max) ? stats.short.max : Math.max(...shortsForHeat),
    },
    total: {
      min: Number.isFinite(stats?.total?.min) ? stats.total.min : Math.min(...totalsForHeat),
      max: Number.isFinite(stats?.total?.max) ? stats.total.max : Math.max(...totalsForHeat),
    },
    net: {
      min: Number.isFinite(stats?.net?.min) ? stats.net.min : Math.min(...netsForHeat),
      max: Number.isFinite(stats?.net?.max) ? stats.net.max : Math.max(...netsForHeat),
    },
  }
  const subRaw = String(row?.raw_cftc_market_name || '').trim()
  const tg = String(row?.trader_group_used || '')
  const subTrader = tg.includes('Noncommercial') || row?.position_source_family === 'legacy_noncommercial'
    ? 'Non-commercial (Legacy CFTC)'
    : tg.includes('lev_money')
      ? 'Leveraged funds (CFTC)'
      : 'Asset managers & large specs (CFTC "managed money" line)'
  const pack = row?.ui_pack && typeof row.ui_pack === 'object' ? row.ui_pack : buildFallbackUiPack(row)

  const sumRow = (label, sLong, sShort, sLd, sSd, sTot, sPl, sPs, sNet, sW1) => (
    <tr key={label} className="wo-cot-summary-row">
      <td className="wo-cot-summary-label">{label}</td>
      <td>{fmtStatNum(sLong)}</td>
      <td>{fmtStatNum(sShort)}</td>
      <td>{fmtStatNum(sLd)}</td>
      <td>{fmtStatNum(sSd)}</td>
      <td>{fmtStatNum(sTot)}</td>
      <td>{fmtPct1(sPl)}</td>
      <td>{fmtPct1(sPs)}</td>
      <td className="wo-cot-net-col wo-cot-net-summary">{fmtStatNum(sNet)}</td>
      <td>{fmtStatNum(sW1)}</td>
      <td>—</td>
      <td>—</td>
    </tr>
  )

  return (
    <div className="detail-panel detail-panel-terminal">
      <div className="wo-cot-header wo-cot-header-compact">
        <h3 className="wo-cot-title">{row?.market || '—'}</h3>
        <p className="wo-cot-sub">{subTrader}{subRaw ? ` · ${subRaw}` : ''}</p>
      </div>

      {!workspaceMode ? (
        <MarketBriefingPanel
          row={row}
          pack={pack}
          peersByMarket={peersByMarket}
          globalMarketRegime={globalMarketRegime}
          latestParticipation={latestParticipation}
        />
      ) : null}

      <LiveMarketContextSection
        row={row}
        pack={pack}
        globalMarketRegime={globalMarketRegime}
        globalCalendar={economicCalendar}
        weatherContext={weatherContext}
        weatherLoadError={weatherLoadError}
      />

      <ExecutiveMarketStrip pack={pack} />

      <MacroRelationshipMap
        market={row?.market}
        row={row}
        latestParticipation={latestParticipation}
        relationshipMapData={relationshipMapData}
        hideWeatherPlaceholder={hideWeatherPlaceholder}
      />

      <section className="cot-backbone" aria-label="Institutional positioning backbone">
        <h4 className="backbone-title">Institutional positioning</h4>
        <p className="backbone-lede">Managed-money snapshot for this COT week — levels and deltas below.</p>

        <h4 className="backbone-subh">Snapshot — contract levels &amp; week-over-week changes</h4>
        <div className="snapshot-grid">
          <div className="snapshot-card">
            <div className="lbl">Positioning state</div>
            <div className="val" style={{ fontSize: '0.88rem' }}>
              <span className={`badge-state ${stateToneClass(row?.positioning_state)}`}>{display(row?.positioning_state)}</span>
            </div>
            <div className="sub">Strengthening / weakening read</div>
          </div>
          <div className="snapshot-card"><div className="lbl">COT bias</div><div className="val">{display(row?.cot_bias)}</div><div className="sub">directional read</div></div>
          <div className="snapshot-card"><div className="lbl">COT score</div><div className="val">{display(row?.cot_score)}</div><div className="sub">0–10 engine score</div></div>
          <div className="snapshot-card"><div className="lbl">Net</div><div className="val">{fmtNum(N)}</div><div className="sub">contracts</div></div>
          <div className="snapshot-card"><div className="lbl">Longs</div><div className="val">{fmtNum(L)}</div><div className="sub">contracts</div></div>
          <div className="snapshot-card"><div className="lbl">Shorts</div><div className="val">{fmtNum(S)}</div><div className="sub">contracts</div></div>
          <div className="snapshot-card"><div className="lbl">% Long</div><div className="val">{pctLong(L, S)}</div><div className="sub">of long+short</div></div>
          <div className="snapshot-card"><div className="lbl">% Short</div><div className="val">{pctLong(S, L)}</div><div className="sub">of short+long</div></div>
          <div className="snapshot-card"><div className="lbl">Net vs prior week</div><div className="val">{fmtNum(row?.one_week_net_change)}</div><div className="sub">one report</div></div>
          <div className="snapshot-card"><div className="lbl">Net vs four reports</div><div className="val">{fmtNum(row?.four_week_net_change)}</div><div className="sub">about one month</div></div>
          <div className="snapshot-card"><div className="lbl">Macro regime</div><div className="val" style={{ fontSize: '0.95rem' }}>{display(row?.macro_regime)}</div><div className="sub">same-week filter</div></div>
          <div className="snapshot-card"><div className="lbl">Macro score</div><div className="val">{display(row?.macro_score)}</div><div className="sub">macro confluence score</div></div>
          {(() => {
            const rm = row?.rates_macro && typeof row.rates_macro === 'object' ? row.rates_macro : null
            if (!rm) return null
            return (
              <>
                <div className="snapshot-card"><div className="lbl">Rates macro signal</div><div className="val" style={{ fontSize: '0.9rem' }}>{display(rm.macro_signal)}</div><div className="sub">same-week Treasury read</div></div>
                <div className="snapshot-card"><div className="lbl">Rates macro score</div><div className="val">{display(rm.macro_score)}</div><div className="sub">internal confluence count</div></div>
                <div className="snapshot-card"><div className="lbl">Rates bias</div><div className="val" style={{ fontSize: '0.88rem' }}>{display(rm.rates_bias)}</div><div className="sub">yield path label</div></div>
                <div className="snapshot-card"><div className="lbl">Curve state</div><div className="val" style={{ fontSize: '0.88rem' }}>{display(rm.curve_state)}</div><div className="sub">2s10s slope read</div></div>
              </>
            )
          })()}
          <div className="snapshot-card"><div className="lbl">Final context</div><div className="val" style={{ fontSize: '0.9rem' }}>{display(row?.final_context)}</div><div className="sub">COT × macro confluence</div></div>
          <div className="snapshot-card snapshot-card-wide">
            <div className="lbl">Zone focus</div>
            <div className="val" style={{ fontSize: '0.88rem' }}>{display(row?.zone_focus)}</div>
            <div className="sub">{display(row?.zone_to_watch)}</div>
          </div>
        </div>

        {!workspaceMode ? <PositioningDecisionSupport row={row} /> : null}

        {!workspaceMode ? (
        <div className="narrative-block backbone-narrative">
          <h4>Setup &amp; plan</h4>
          <p style={{ margin: '0 0 10px', color: '#e2e8f0', lineHeight: 1.55 }}>
            <strong style={{ color: '#94a3b8' }}>Zone focus:</strong> {display(row?.zone_focus)}
          </p>
          <p style={{ margin: '0 0 10px', color: '#e2e8f0', lineHeight: 1.55 }}>
            <strong style={{ color: '#94a3b8' }}>Setup type:</strong> {display(row?.setup_type)}
          </p>
          <p style={{ margin: '0 0 10px', color: '#e2e8f0', lineHeight: 1.55 }}>
            <strong style={{ color: '#94a3b8' }}>Confidence:</strong> {display(row?.confidence_label)}
          </p>
          <p style={{ margin: '0 0 10px', color: '#f1f5f9', lineHeight: 1.55, whiteSpace: 'pre-wrap' }}>
            <strong style={{ color: '#94a3b8' }}>Invalidation:</strong> {display(row?.invalidation_note)}
          </p>
          <p style={{ margin: 0, color: '#fde68a', lineHeight: 1.55, whiteSpace: 'pre-wrap' }}>
            <strong style={{ color: '#94a3b8' }}>Next data watch:</strong> {display(row?.next_data_watch)}
          </p>
        </div>
        ) : null}

        {!workspaceMode ? (
        <>
        <HistoricalModeBlock
          title="History (no look-ahead)"
          subtitle="Uses only reports on or before the week you selected — good for backtests."
          ctx={row?.expanding_history_context}
        />
        <HistoricalModeBlock
          title="Full file extremes"
          subtitle="Min/max and rank versus every report stored for this contract in the file, no matter which week you picked."
          ctx={row?.full_loaded_history_context}
          headingStyle={{ marginTop: '22px' }}
        />
        </>
        ) : null}

        <h4 className="wo-cot-section-title" style={{ marginTop: '22px' }}>Recent positioning trail</h4>
        <p className="wo-cot-hint">
          Last {chronoEnriched.length} reports in view: <strong>13W high / low / average</strong> for context (not the full archive).
          {' '}Newest row at top. Heat on long/short = size versus this window; week columns = change from the prior COT print.
        </p>
        <div className="wo-cot-table-wrap">
          <table className="wo-cot-table wo-cot-table-participation">
            <thead>
              <tr>
                <th>Date</th>
                <th>Long</th>
                <th>Short</th>
                <th title="Change in longs vs prior COT week">Long Wk</th>
                <th title="Change in shorts vs prior COT week">Short Wk</th>
                <th>Total OI</th>
                <th>% Long</th>
                <th>% Short</th>
                <th>Net</th>
                <th title="Net change vs prior COT week">Net Wk</th>
                <th title="Independent long/short flow classification">Participation</th>
                <th>State</th>
              </tr>
            </thead>
            <tbody>
              {!displayDesc.length ? (
                <tr>
                  <td colSpan={12} style={{ padding: '20px', textAlign: 'center', color: '#64748b' }}>
                    No COT history rows for this contract in the current file.
                  </td>
                </tr>
              ) : null}
              {stats && stats.windowN > 0 && displayDesc.length ? (
                <>
                  {sumRow('13W Max', stats.long.max, stats.short.max, stats.longDelta.max, stats.shortDelta.max, stats.total.max, stats.pctLong.max, stats.pctShort.max, stats.net.max, stats.w1.max)}
                  {sumRow('13W Min', stats.long.min, stats.short.min, stats.longDelta.min, stats.shortDelta.min, stats.total.min, stats.pctLong.min, stats.pctShort.min, stats.net.min, stats.w1.min)}
                  {sumRow(avgLabel, stats.long.avg, stats.short.avg, stats.longDelta.avg, stats.shortDelta.avg, stats.total.avg, stats.pctLong.avg, stats.pctShort.avg, stats.net.avg, stats.w1.avg)}
                </>
              ) : null}
              {displayDesc.map((h) => {
                const d = rowDate(h)
                const isCurrent = normalizeDate(d) === normalizeDate(rowDate(row))
                const netVal = Number(h.net_value)
                const ld = h._longDelta1w
                const sd = h._shortDelta1w
                const nd = Number.isFinite(h._netDelta1w) ? h._netDelta1w : numOrNaN(h.one_week_net_change)
                const td = h._totalDelta1w
                const part = h._participation || { category: '—', tooltip: '', summary: '' }
                const stateLine = String(h.positioning_state || '').trim()
                const rowTip = [
                  `Report ${d}: ${part.summary}`,
                  `Legs vs prior week — longs ${fmtDeltaCell(ld)}, shorts ${fmtDeltaCell(sd)}, net ${fmtDeltaCell(nd)}.`,
                  part.tooltip,
                  stateLine ? `Engine state: ${stateLine}.` : '',
                ]
                  .filter(Boolean)
                  .join(' ')
                return (
                  <tr
                    key={d}
                    className={isCurrent ? 'wo-cot-data-row wo-row-current' : 'wo-cot-data-row'}
                    title={rowTip}
                    style={{ cursor: 'help' }}
                  >
                    <td className="wo-cot-date">{display(d)}</td>
                    <td className={longLevelHeat(h.long_value, heatRanges.long.min, heatRanges.long.max).className}>
                      {fmtNum(h.long_value)}
                    </td>
                    <td className={shortLevelHeat(h.short_value, heatRanges.short.min, heatRanges.short.max).className}>
                      {fmtNum(h.short_value)}
                    </td>
                    <td className={signedDeltaHeat(ld, longDeltasForHeat).className}>{fmtDeltaCell(ld)}</td>
                    <td className={signedDeltaHeat(sd, shortDeltasForHeat, true).className}>{fmtDeltaCell(sd)}</td>
                    <td
                      className={totalOiLevelHeat(rowOiTotal(h), heatRanges.total.min, heatRanges.total.max).className}
                    >
                      {fmtNum(rowOiTotal(h))}
                    </td>
                    <td>{fmtPct1(pctLongNumber(h))}</td>
                    <td>{fmtPct1(pctShortNumber(h))}</td>
                    <td
                      className={`wo-cot-net-col ${netHeatStyle(netVal, heatRanges.net.min, heatRanges.net.max).className}`}
                    >
                      {fmtNum(h.net_value)}
                    </td>
                    <td className={signedDeltaHeat(nd, netDeltasForHeat).className}>{fmtDeltaCell(nd)}</td>
                    <td className={`wo-cot-participation-cell ${participationCellStyle(part.tone).className}`}>
                      <span className="wo-cot-participation-label">{display(part.category)}</span>
                      <span className="wo-cot-participation-sub">{display(part.summary).slice(0, 72)}{String(part.summary || '').length > 72 ? '…' : ''}</span>
                    </td>
                    <td className="wo-cot-state-cell"><span className={`badge-state ${stateToneClass(h.positioning_state)}`}>{display(h.positioning_state)}</span></td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>

        <details className="engine-scoring-details" style={{ marginTop: '14px' }}>
          <summary className="engine-scoring-sum" style={{ cursor: 'pointer', color: '#94a3b8', fontSize: '0.85rem' }}>
            Engine detail — how this row was scored (optional)
          </summary>
          <h4 className="backbone-subh" style={{ marginTop: '12px' }}>Scoring narrative</h4>
          <p className="backbone-prose">{display(row?.cot_reason)}</p>
          <h4 className="backbone-subh">Positioning interpretation (technical)</h4>
          <p style={{ margin: 0, color: '#cbd5e1', lineHeight: 1.55 }}>{display(row?.positioning_interpretation)}</p>
        </details>
      </section>

      <section className="final-context-strip" aria-label="Final context">
        <span className="final-context-k">Final context</span>
        <span className="final-context-v">{display(pack.final_context_line)}</span>
      </section>

      <details className="audit-disclosure" open={workspaceMode ? false : undefined}>
        <summary>Expand deep audit (macro detail, engine notes, dataset)</summary>
        <div className="audit-inner">
          {hasRealValue(row?.final_context_reason) ? (
            <p className="audit-rationale">
              <span className="audit-rationale-k">Confluence rationale</span>
              {display(row.final_context_reason)}
            </p>
          ) : null}
          {row?.rates_macro?.macro_rationale && hasRealValue(row.rates_macro.macro_rationale) ? (
            <p className="audit-rationale">
              <span className="audit-rationale-k">Rates-layer rationale (row)</span>
              {display(row.rates_macro.macro_rationale)}
              {hasRealValue(row.rates_macro.liquidity_regime) ? (
                <span className="audit-rationale-sub"> Liquidity note: {display(row.rates_macro.liquidity_regime)}</span>
              ) : null}
            </p>
          ) : null}
          <MacroAuditBlock audit={row?.macro_audit} />
          <p className="wo-cot-meta-line audit-debug">
            <strong>Dataset</strong> — loaded COT rows for this market: {display(row?.loadedSeriesRowCount)}
          </p>
        </div>
      </details>
    </div>
  )
}

function DashboardMacroGallery({ maps }) {
  return (
    <section className="dashboard-macro-gallery" aria-label="Macro overlays for all tracked markets">
      <h2 className="dmg-title">Macro overlays (price vs driver)</h2>
      <p className="dmg-lede">
        Pulled from <code className="regime-code">macro_relationship_maps</code> in the weekly JSON. Same charts as expanded row detail; compact view for scanning. Rebased % from the start of each window.
      </p>
      <div className="dmg-health">
        <MacroHealthPanel maps={maps} />
      </div>
      <div className="dmg-grid">
        {TRACKED_MARKETS.map((market) => {
          const rm = resolveMacroRelationshipMap(maps, market)
          const expected = expectsMacroRelationshipMap(market)
          const live =
            rm &&
            rm.available === true &&
            marketsMacroAlign(rm.market || '', market)
          const fresh = readMacroFreshness(rm)
          return (
            <article key={market} className="dmg-card">
              <header className="dmg-card-h">
                <h3 className="dmg-card-title">{market}</h3>
                {fresh && (live || fresh.carriedOver) ? (
                  <span className={`dmg-fresh dmg-fresh--${fresh.tone}`} title={`data_status: ${fresh.status}`}>
                    {fresh.label}
                  </span>
                ) : expected ? (
                  <span className="dmg-pending">Data source pending</span>
                ) : (
                  <span className="dmg-na">No overlay slot</span>
                )}
              </header>
              {live ? (
                <>
                  <p className="dmg-pair">
                    {String(rm.price_series_display || 'Price')} vs {String(rm.driver_series_display || rm.driver_label || 'Macro driver')}
                  </p>
                  <MacroRelationshipOverlayChart rm={rm} compact />
                </>
              ) : (
                <div className="dmg-placeholder">
                  {expected
                    ? rm?.available === false
                      ? humanMacroMapUnavailableReason(rm?.error)
                      : 'Rebuild the weekly confluence export with macro maps enabled so this contract appears here.'
                    : 'Macro price overlays target the core futures set in this build; use row detail for other context.'}
                </div>
              )}
            </article>
          )
        })}
      </div>
    </section>
  )
}

const completenessScore = (row = {}) => {
  const fields = [
    row.raw_cftc_market_name,
    row.long_value,
    row.short_value,
    row.net_value,
    row.one_week_net_change,
    row.four_week_net_change,
    row.cot_bias,
    row.cot_score,
    row.cot_reason,
    row.macro_regime || row.macro_signal,
    row.macro_score,
    row.final_context,
    row.final_context_reason,
    row.positioning_state,
    row.institutional_flow_summary,
    row.zone_focus,
    row.setup_type,
    row.confidence_label,
    row.invalidation_note,
    row.next_data_watch,
    row.flow_change_summary,
    row.expanding_history_context?.summary || row.historical_context_summary,
    row.expanding_history_context?.current_net_percentile ?? row.current_net_percentile,
  ]
  return fields.reduce((acc, v) => acc + (hasRealValue(v) ? 1 : 0), 0)
}

const sanitizeInvalidNumericLiterals = (text = '') => text.replace(/\b(?:NaN|Infinity|-Infinity|undefined)\b/g, 'null')

const sanitizeObject = (value, stats = { sanitized: false, replacements: 0 }) => {
  if (Array.isArray(value)) return value.map((item) => sanitizeObject(item, stats))
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, sanitizeObject(v, stats)]))
  if (value === undefined || value === null) return null
  if (typeof value === 'number' && !Number.isFinite(value)) {
    stats.sanitized = true
    stats.replacements += 1
    return null
  }
  return value
}

const safeJsonParse = (text = '') => {
  try {
    return { parsed: JSON.parse(text), sanitized: false, replacements: 0 }
  } catch (err) {
    const repaired = sanitizeInvalidNumericLiterals(text)
    const replacements = ((text.match(/\b(?:NaN|Infinity|-Infinity|undefined)\b/g)) || []).length
    const parsed = JSON.parse(repaired)
    return { parsed, sanitized: true, replacements, parseError: err }
  }
}

function App() {
  const [data, setData] = React.useState([])
  const [date, setDate] = React.useState('')
  const [latestCotReportDate, setLatestCotReportDate] = React.useState('')
  const [expanded, setExpanded] = React.useState({})
  const [macroRelationshipMaps, setMacroRelationshipMaps] = React.useState({})
  const [globalRegimeFromPayload, setGlobalRegimeFromPayload] = React.useState(null)

  React.useEffect(() => {
    fetch('/data/confluence_history_latest.json')
      .then((r) => r.text())
      .then((text) => {
        const parsedResult = safeJsonParse(text)
        const stats = { sanitized: parsedResult.sanitized, replacements: parsedResult.replacements }
        const payload = sanitizeObject(parsedResult.parsed, stats)
        const rows = Array.isArray(payload?.records) ? payload.records : (Array.isArray(payload) ? payload : [])
        setData(rows)
        const metaLatest = payload?.latest_cot_report_date != null ? String(payload.latest_cot_report_date) : ''
        setLatestCotReportDate(metaLatest)
        const ds = [...new Set(rows.map(rowDate).filter(Boolean))].sort()
        setDate(defaultDashboardWeek(rows, metaLatest || ds.at(-1) || ''))
        const mrm =
          payload?.macro_relationship_maps && typeof payload.macro_relationship_maps === 'object'
            ? payload.macro_relationship_maps
            : {}
        setMacroRelationshipMaps(mrm)
        const gro = payload?.global_market_regime_latest_week
        setGlobalRegimeFromPayload(gro && typeof gro === 'object' ? gro : null)
      })
      .catch(() => {
        setData([])
        setLatestCotReportDate('')
        setMacroRelationshipMaps({})
        setGlobalRegimeFromPayload(null)
      })
  }, [])

  const dates = React.useMemo(() => [...new Set(data.map((r) => normalizeDate(rowDate(r))).filter(Boolean))].sort(), [data])
  const week = React.useMemo(
    () => data
      .filter((r) => normalizeDate(rowDate(r)) === normalizeDate(date))
      .map((r) => {
        const marketSource = r.market || r.raw_cftc_market_name || ''
        return { ...r, market_key: canonical(marketSource) }
      }),
    [data, date],
  )

  React.useEffect(() => {
    if (!date || !data.length) return
    logCotResolutionForWeek(data, date, TRACKED_MARKETS)
  }, [data, date])

  const historyByMarket = React.useMemo(() => {
    const m = new Map()
    TRACKED_MARKETS.forEach((mk) => {
      m.set(mk, buildMarketHistory(data, mk, date))
    })
    return m
  }, [data, date])

  const marketRows = React.useMemo(() => {
    const weekNorm = normalizeDate(date)
    const byMarket = new Map()
    week.forEach((row) => {
      if (!TRACKED_MARKETS.includes(row.market_key)) return
      const prev = byMarket.get(row.market_key)
      if (!prev) {
        byMarket.set(row.market_key, row)
        return
      }
      const prevScore = completenessScore(prev)
      const nextScore = completenessScore(row)
      if (nextScore > prevScore) byMarket.set(row.market_key, row)
    })
    return TRACKED_MARKETS.map((market) => {
      let row = byMarket.get(market)
      if (!isCotRowResolved(row)) {
        const resolved = resolveRowForMarketWeek(data, market, weekNorm)
        if (resolved.row) {
          row = {
            ...resolved.row,
            market_key: market,
            _cot_resolve_mode: resolved.matchMode,
            _cot_calendar_week: weekNorm,
          }
        }
      }
      const loadedSeriesRowCount = data.filter(
        (x) => canonical(x.market || x.raw_cftc_market_name || '') === market,
      ).length
      return enrichRowHistoryContext({
        market,
        latest_report_date: row?.latest_report_date || row?.date || date,
        cot_bias: row?.cot_bias,
        cot_score: row?.cot_score,
        cot_reason: row?.cot_reason || row?.cot_reasoning || row?.cot_context,
        macro_regime: row?.macro_regime || row?.macro_signal,
        macro_score: row?.macro_score,
        final_context: row?.final_context || row?.confluence_bias,
        technical_action_note: row?.technical_action_note || row?.summary || row?.technical_note || row?.trade_readiness,
        final_context_reason: row?.final_context_reason,
        raw_cftc_market_name: row?.raw_cftc_market_name,
        trader_group_used: row?.trader_group_used,
        long_value: row?.long_value,
        short_value: row?.short_value,
        net_value: row?.net_value,
        previous_week_net: row?.previous_week_net,
        one_week_net_change: row?.one_week_net_change,
        four_week_net_change: row?.four_week_net_change,
        bias_rule_used: row?.bias_rule_used,
        score_rule_used: row?.score_rule_used,
        final_calculated_cot_bias: row?.final_calculated_cot_bias,
        final_calculated_cot_score: row?.final_calculated_cot_score,
        positioning_state: row?.positioning_state,
        four_week_positioning_story: row?.four_week_positioning_story,
        positioning_interpretation: row?.positioning_interpretation,
        one_week_long_change: row?.one_week_long_change,
        one_week_short_change: row?.one_week_short_change,
        macro_audit: row?.macro_audit,
        rates_macro: row?.rates_macro,
        institutional_flow_summary: row?.institutional_flow_summary,
        zone_focus: row?.zone_focus,
        zone_to_watch: row?.zone_to_watch,
        pressure_summary: row?.pressure_summary,
        flow_change_summary: row?.flow_change_summary,
        trader_action_note: row?.trader_action_note,
        setup_type: row?.setup_type,
        confidence_label: row?.confidence_label,
        invalidation_note: row?.invalidation_note,
        next_data_watch: row?.next_data_watch,
        expanding_history_context: row?.expanding_history_context,
        full_loaded_history_context: row?.full_loaded_history_context,
        all_time_long_max: row?.all_time_long_max,
        all_time_long_min: row?.all_time_long_min,
        all_time_short_max: row?.all_time_short_max,
        all_time_short_min: row?.all_time_short_min,
        all_time_net_max: row?.all_time_net_max,
        all_time_net_min: row?.all_time_net_min,
        current_long_percentile: row?.current_long_percentile,
        current_short_percentile: row?.current_short_percentile,
        current_net_percentile: row?.current_net_percentile,
        current_net_rank_label: row?.current_net_rank_label,
        historical_series_earliest_date: row?.historical_series_earliest_date,
        historical_series_report_date: row?.historical_series_report_date,
        historical_percentile_n_joint: row?.historical_percentile_n_joint,
        historical_context_summary: row?.historical_context_summary,
        loadedSeriesRowCount,
        global_market_regime: row?.global_market_regime,
        instrument_intel_context: row?.instrument_intel_context,
        intermarket_impulse_context: row?.intermarket_impulse_context,
        ui_pack: row?.ui_pack,
      })
    })
  }, [week, date, data])

  const tracked = marketRows.filter((r) => r.cot_score !== undefined && r.cot_score !== null && r.cot_score !== 'N/A')

  const globalMarketRegime = React.useMemo(() => {
    const hit = week.find((r) => r.global_market_regime && typeof r.global_market_regime === 'object')
    return hit?.global_market_regime ?? globalRegimeFromPayload ?? null
  }, [week, globalRegimeFromPayload])

  const ratesMacroPreview = React.useMemo(() => {
    const hit = week.find((r) => r.rates_macro && typeof r.rates_macro === 'object')
    return hit?.rates_macro ?? null
  }, [week])

  const peersByMarket = React.useMemo(
    () => Object.fromEntries(marketRows.map((r) => [r.market, r])),
    [marketRows],
  )

  const toggleExpand = (market) => {
    setExpanded((s) => ({ ...s, [market]: !s[market] }))
  }

  return (
    <div className="app">
      <header className="dashboard-header">
        <h1>Market intelligence — COT &amp; confluence</h1>
        <p className="tagline">
          Week-level scan; each row expands into a structured context stack (executive strip → compact panels → audit). Data-first, discretionary execution.
        </p>
        <div className="controls-bar">
          <div>
            <label htmlFor="wk">COT report week</label>
            <div>
              <select id="wk" value={date} onChange={(e) => setDate(e.target.value)}>
                {dates.map((d) => <option key={d} value={d}>{d}</option>)}
              </select>
            </div>
          </div>
          {latestCotReportDate ? (
            <div className="meta-strip">
              Latest COT date in dataset: <strong>{latestCotReportDate}</strong>
            </div>
          ) : null}
        </div>
      </header>

      <details className="intel-v1-preview intel-v1-details">
        <summary className="intel-v1-preview-title intel-v1-details-sum">Rates &amp; regime snapshot</summary>
        <div className="intel-v1-details-body">
          <p className="intel-v1-preview-note">
            Technical fields from confluence JSON — not trade advice. FRED-backed lines appear when <code className="regime-code">rates_macro</code> is present.
          </p>
          <ul className="intel-v1-preview-lines">
          <li>
            <span className="intel-v1-k">Rates macro</span>{' '}
            {ratesMacroPreview
              ? `${display(ratesMacroPreview.macro_signal)} · score ${display(ratesMacroPreview.macro_score)} · ${display(ratesMacroPreview.rates_bias)} · curve ${display(ratesMacroPreview.curve_state)}`
              : 'source unavailable — rebuild confluence JSON'}
          </li>
          <li>
            <span className="intel-v1-k">Macro rationale</span>{' '}
            {ratesMacroPreview ? display(ratesMacroPreview.macro_rationale) : 'source unavailable'}
          </li>
          <li>
            <span className="intel-v1-k">Liquidity regime</span>{' '}
            {ratesMacroPreview ? display(ratesMacroPreview.liquidity_regime) : 'source unavailable'}
          </li>
          <li>
            <span className="intel-v1-k">Global regime</span>{' '}
            {globalMarketRegime?.resolved_macro_signal != null && globalMarketRegime?.resolved_macro_signal !== ''
              ? formatRegimeSignalLabel(globalMarketRegime.resolved_macro_signal)
              : display(globalMarketRegime?.risk_regime)}
          </li>
        </ul>
        </div>
      </details>

      <GlobalMarketRegimePanel regime={globalMarketRegime} weekLabel={date} />

      <DashboardMacroGallery maps={macroRelationshipMaps} />

      <div className="table-wrap">
        <table className="flow-table">
          <thead>
            <tr>
              <th>Market</th>
              <th>Report</th>
              <th>Positioning state</th>
              <th>Institutional flow</th>
              <th>COT bias</th>
              <th>COT score</th>
              <th>Macro</th>
              <th>Macro score</th>
              <th>Rates signal</th>
              <th>Curve</th>
              <th>Final context</th>
              <th>Zone Focus</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {marketRows.map((r) => (
              <React.Fragment key={r.market}>
                <tr className="data-row" onClick={() => toggleExpand(r.market)}>
                  <td className="col-market">{r.market}</td>
                  <td className="col-tight">{display(r.latest_report_date)}</td>
                  <td>
                    <span className={`badge-state ${stateToneClass(r.positioning_state)}`}>{display(r.positioning_state)}</span>
                  </td>
                  <td className="col-flow">
                    <div className="flow-clamp">{display(r.institutional_flow_summary)}</div>
                  </td>
                  <td>{display(r.cot_bias)}</td>
                  <td className="col-tight">{display(r.cot_score)}</td>
                  <td className="col-tight">{display(r.macro_regime)}</td>
                  <td className="col-tight">{display(r.macro_score)}</td>
                  <td className="col-tight">{display(r.rates_macro?.macro_signal)}</td>
                  <td className="col-tight">{display(r.rates_macro?.curve_state)}</td>
                  <td className="col-tight">{display(r.final_context)}</td>
                  <td className="col-zone-focus">
                    <div className="flow-clamp">{display(r.zone_focus)}</div>
                  </td>
                  <td>
                    <button
                      type="button"
                      className="expand-btn"
                      onClick={(e) => {
                        e.stopPropagation()
                        toggleExpand(r.market)
                      }}
                    >
                      {expanded[r.market] ? '▾ Detail' : '▸ Detail'}
                    </button>
                  </td>
                </tr>
                {expanded[r.market] ? (
                  <tr>
                    <td colSpan={13}>
                      <InstrumentDetail
                        row={r}
                        historyRows={historyByMarket.get(r.market) || []}
                        peersByMarket={peersByMarket}
                        globalMarketRegime={globalMarketRegime}
                        relationshipMapData={resolveMacroRelationshipMap(macroRelationshipMaps, r.market) ?? null}
                      />
                    </td>
                  </tr>
                ) : null}
              </React.Fragment>
            ))}
          </tbody>
        </table>
      </div>

      <div className="charts">
        <div className="panel">
          <h3>COT score distribution (selected week)</h3>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={tracked}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis dataKey="market" tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <YAxis domain={[0, 10]} tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <Tooltip contentStyle={{ background: '#0f172a', border: '1px solid #334155' }} />
              <Bar dataKey="cot_score" fill="#3b82f6" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  )
}

export { InstrumentDetail }
