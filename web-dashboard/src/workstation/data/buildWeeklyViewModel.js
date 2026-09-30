/**
 * Frontend weekly-view model for the COT workstation inspector.
 * Assembles timeline + research payload (including weekly_inspector series).
 *
 * IMPORTANT: participant percentiles and 1W/4W/12W contract changes are
 * calculated from genuine COT observations only. Price-only timeline rows must
 * never be treated as extra COT reports.
 */

import { eventBadge, eventTone } from '../researchEventUi.js'
import { stateLabelFromTemperature } from './expandWeeklyInspector.js'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)
const ROLLING_PERCENTILE_WEEKS = 156
const RUNTIME_PERCENTILE_MEASURE = 'net_positioning_3y_rolling_percentile_runtime'
const RUNTIME_PERCENTILE_LABEL = 'Net positioning percentile (rolling 3Y, point-in-time)'

function sliceDate(v) {
  return String(v || '').slice(0, 10)
}

function daysBetween(a, b) {
  const da = Date.parse(`${String(a).slice(0, 10)}T00:00:00Z`)
  const db = Date.parse(`${String(b).slice(0, 10)}T00:00:00Z`)
  if (!Number.isFinite(da) || !Number.isFinite(db)) return Infinity
  return Math.abs(db - da) / 86400000
}

/** Tie-aware empirical percentile rank, matching the Python research engine. */
export function empiricalPercentileRank(values, value) {
  if (!isNum(value)) return null
  const finite = (values || []).filter(isNum)
  if (!finite.length) return null
  if (finite.length === 1) return 50
  let below = 0
  let equal = 0
  for (const v of finite) {
    if (v < value) below += 1
    else if (v === value) equal += 1
  }
  return (100 * (below + 0.5 * equal)) / finite.length
}

function round2(v) {
  return isNum(v) ? Math.round(v * 100) / 100 : null
}

/**
 * Build point-in-time rolling-3Y percentile stats from the actual COT net
 * observations present on the timeline. The inspector calendar is deliberately
 * NOT used as a filter: it can lag the latest report by a week. Since the binder
 * leaves COT net values null on price-only rows, a finite net value is itself the
 * authoritative proof that this row is a genuine COT observation.
 *
 * inspectorByDate remains in the signature for compatibility with existing
 * callers/tests but is intentionally ignored.
 */
export function buildRuntimeRollingPercentiles(
  rows,
  inspectorByDate,
  netKey,
  windowWeeks = ROLLING_PERCENTILE_WEEKS,
) {
  void inspectorByDate
  const observations = []
  for (const row of rows || []) {
    const date = sliceDate(row?.date || row?.label)
    const value = row?.[netKey]
    if (!date || !isNum(value)) continue
    observations.push({ date, value })
  }

  const stats = new Map()
  const pcts = []
  for (let i = 0; i < observations.length; i += 1) {
    const start = Math.max(0, i - windowWeeks + 1)
    const window = observations.slice(start, i + 1).map((o) => o.value)
    const pct = round2(empiricalPercentileRank(window, observations[i].value))
    pcts.push(pct)
    stats.set(observations[i].date, {
      percentile: pct,
      observationCount: window.length,
      percentileChange1w: null,
      percentileChange4w: null,
      percentileChange12w: null,
    })
  }

  for (let i = 0; i < observations.length; i += 1) {
    const stat = stats.get(observations[i].date)
    if (!stat) continue
    const delta = (lag) =>
      i >= lag && isNum(pcts[i]) && isNum(pcts[i - lag])
        ? round2(pcts[i] - pcts[i - lag])
        : null
    stat.percentileChange1w = delta(1)
    stat.percentileChange4w = delta(4)
    stat.percentileChange12w = delta(12)
  }
  return stats
}

function applyRuntimePercentile(target, stat) {
  if (!target || !stat || !isNum(stat.percentile)) return
  target.percentile = stat.percentile
  target.percentileChange1w = stat.percentileChange1w
  target.percentileChange4w = stat.percentileChange4w
  target.percentileChange12w = stat.percentileChange12w
  target.percentileObservationCount = stat.observationCount
  target.measure = RUNTIME_PERCENTILE_MEASURE
  target.percentileSource = 'timeline_cot_rolling_156'
  target.isExtreme = stat.percentile >= 90 || stat.percentile <= 10
  target.summaryLine = null
}

/**
 * Resolve weekly_inspector pack for a timeline date.
 * Exact COT report date wins; otherwise as-of match the latest inspector week
 * on or before the date within 7 days.
 */
export function resolveInspectorWeekForDate(inspectorByDate, date) {
  const d = sliceDate(date)
  if (!d || !inspectorByDate?.size) return null
  if (inspectorByDate.has(d)) {
    return { week: inspectorByDate.get(d), asOfDate: d, exact: true }
  }
  let best = null
  for (const key of inspectorByDate.keys()) {
    if (key <= d && (!best || key > best)) best = key
  }
  if (!best || daysBetween(best, d) > 7) return null
  return { week: inspectorByDate.get(best), asOfDate: best, exact: false }
}

/** Required derived fields that must never render as Unavailable when history exists. */
export function missingRequiredInspectorFields(week) {
  if (!week) return ['week']
  const missing = []
  for (const [key, label] of [
    ['commercial', 'commercial'],
    ['nonCommercial', 'non_commercial'],
    ['nonReportable', 'non_reportable'],
  ]) {
    const p = week[key]
    if (!p) {
      missing.push(label)
      continue
    }
    for (const field of [
      'net',
      'change1w',
      'change4w',
      'change12w',
      'percentile',
      'percentileChange1w',
      'percentileChange4w',
      'percentileChange12w',
      'percentileObservationCount',
      'stateLabel',
      'temperature',
    ]) {
      const v = p[field]
      if (v == null || v === '' || v === 'Unavailable') {
        missing.push(`${label}.${field}`)
        continue
      }
      if (
        [
          'net',
          'change1w',
          'change4w',
          'change12w',
          'percentile',
          'percentileChange1w',
          'percentileChange4w',
          'percentileChange12w',
          'percentileObservationCount',
        ].includes(field) &&
        !isNum(v)
      ) {
        missing.push(`${label}.${field}`)
      }
    }
    if (isNum(p.percentile) && (p.percentile < 0 || p.percentile > 100)) {
      missing.push(`${label}.percentile_out_of_range`)
    }
  }
  const sp = week.spreads || {}
  for (const field of [
    'commercialPercentile',
    'noncommercialPercentile',
    'nonreportablePercentile',
    'relationship',
    'flow',
  ]) {
    const v = sp[field]
    if (v == null || v === '' || v === 'unavailable' || v === 'Unavailable') {
      missing.push(`cross_group.${field}`)
    }
  }
  if (isNum(sp.commercialPercentile) && isNum(sp.noncommercialPercentile) && sp.commNc) {
    const expected = sp.commercialPercentile - sp.noncommercialPercentile
    if (
      sp.commNc.valueKind === 'percentile_spread' &&
      isNum(sp.commNc.value) &&
      Math.abs(sp.commNc.value - expected) > 0.05
    ) {
      missing.push('cross_group.c_nc_spread_mismatch')
    }
  }
  return missing
}

export function researchEventId(event) {
  if (!event) return null
  return [
    sliceDate(event.date),
    event.event_type || '',
    event.group || '',
    event.side || '',
    event.label || '',
  ].join('|')
}

function directionFromDelta(delta) {
  if (!isNum(delta) || delta === 0) return 'flat'
  return delta > 0 ? 'rising' : 'falling'
}

/**
 * Change over N genuine COT reports, not N raw timeline rows. Price-only rows
 * are skipped because their participant net is null.
 */
function changeOverObserved(rows, index, key, reportsBack) {
  const cur = rows[index]
  const curValue = cur?.[key]
  if (!isNum(curValue) || reportsBack < 1) return null

  let remaining = reportsBack
  for (let i = index - 1; i >= 0; i -= 1) {
    const priorValue = rows[i]?.[key]
    if (!isNum(priorValue)) continue
    remaining -= 1
    if (remaining === 0) return curValue - priorValue
  }
  return null
}

function emptyParticipant() {
  return {
    net: null,
    change1w: null,
    change4w: null,
    change12w: null,
    percentile: null,
    percentileChange1w: null,
    percentileChange4w: null,
    percentileChange12w: null,
    percentileObservationCount: null,
    flowDirection: null,
    directionArrow: null,
    temperature: null,
    stateLabel: null,
    isExtreme: false,
    measure: RUNTIME_PERCENTILE_MEASURE,
    percentileSource: null,
    extremeState: null,
    rotationState: null,
    direction: 'flat',
    summaryLine: null,
  }
}

function participantFromRow(rows, index, netKey, wowKey) {
  const row = rows[index]
  if (!row || !isNum(row[netKey])) return emptyParticipant()

  // Prefer exact source WoW when present, but derive every wider horizon from
  // genuine report observations so inserted price rows cannot break 4W/12W.
  const change1w = isNum(row[wowKey])
    ? row[wowKey]
    : changeOverObserved(rows, index, netKey, 1)
  return {
    ...emptyParticipant(),
    net: row[netKey],
    change1w,
    change4w: changeOverObserved(rows, index, netKey, 4),
    change12w: changeOverObserved(rows, index, netKey, 12),
    direction: directionFromDelta(change1w),
  }
}

/** Merge backend weekly_inspector packed group onto participant. */
function enrichFromInspectorPack(target, pack, { values = true } = {}) {
  if (!pack || typeof pack !== 'object') return

  if (values) {
    if (isNum(pack.net)) target.net = pack.net
    if (isNum(pack.weekly_change)) target.change1w = pack.weekly_change
    if (isNum(pack.four_week_change)) target.change4w = pack.four_week_change
    if (isNum(pack.twelve_week_change)) target.change12w = pack.twelve_week_change
    if (isNum(pack.percentile)) target.percentile = pack.percentile
    if (isNum(pack.percentile_change_1w)) target.percentileChange1w = pack.percentile_change_1w
    if (isNum(pack.percentile_change_4w)) target.percentileChange4w = pack.percentile_change_4w
    if (isNum(pack.percentile_change_12w)) target.percentileChange12w = pack.percentile_change_12w
    if (isNum(pack.percentile_observation_count)) {
      target.percentileObservationCount = pack.percentile_observation_count
    }
  }

  // Metadata can safely be borrowed from the most recent inspector pack when
  // the compact export is one report behind. Numeric values never are.
  if (pack.direction) target.flowDirection = pack.direction
  if (pack.direction_arrow) target.directionArrow = pack.direction_arrow
  if (pack.temperature) target.temperature = pack.temperature
  target.stateLabel = stateLabelFromTemperature(pack.temperature, pack.state_label || null)
  if (typeof pack.is_extreme === 'boolean') target.isExtreme = pack.is_extreme
  if (pack.measure && values) target.measure = pack.measure
  target.direction = directionFromDelta(target.change1w)
}

function enrichFromResearchParticipant(target, src) {
  if (!src || typeof src !== 'object') return
  if (isNum(src.net) && !isNum(target.net)) target.net = src.net
  if (!isNum(target.percentile)) {
    if (isNum(src.long_history_percentile)) target.percentile = src.long_history_percentile
    else if (isNum(src.percentile)) target.percentile = src.percentile
    else if (isNum(src?.percentiles?.long_history)) target.percentile = src.percentiles.long_history
  }
  const v1 = src?.velocity?.['1w']
  if (isNum(v1?.net_change) && !isNum(target.change1w)) target.change1w = v1.net_change
  if (isNum(v1?.percentile_change) && !isNum(target.percentileChange1w)) {
    target.percentileChange1w = v1.percentile_change
  }
  const v4 = src?.velocity?.['4w']
  if (isNum(v4?.net_change) && !isNum(target.change4w)) target.change4w = v4.net_change
  if (isNum(v4?.percentile_change) && !isNum(target.percentileChange4w)) {
    target.percentileChange4w = v4.percentile_change
  }
  const v12 = src?.velocity?.['12w']
  if (isNum(v12?.net_change) && !isNum(target.change12w)) target.change12w = v12.net_change
  target.direction = directionFromDelta(target.change1w)
}

function enrichFromCurrentState(target, state) {
  if (!state || typeof state !== 'object') return
  if (isNum(state.net) && !isNum(target.net)) target.net = state.net
  const pct = state?.percentiles?.long_history
  if (isNum(pct) && !isNum(target.percentile)) target.percentile = pct
  const v1 = state?.velocity?.['1w']
  if (isNum(v1?.net_change) && !isNum(target.change1w)) target.change1w = v1.net_change
  if (isNum(v1?.percentile_change) && !isNum(target.percentileChange1w)) {
    target.percentileChange1w = v1.percentile_change
  }
  const v4 = state?.velocity?.['4w']
  if (isNum(v4?.net_change) && !isNum(target.change4w)) target.change4w = v4.net_change
  if (isNum(v4?.percentile_change) && !isNum(target.percentileChange4w)) {
    target.percentileChange4w = v4.percentile_change
  }
  const v12 = state?.velocity?.['12w']
  if (isNum(v12?.net_change) && !isNum(target.change12w)) target.change12w = v12.net_change
  target.direction = directionFromDelta(target.change1w)
}

function alignment(aDir, bDir) {
  if (aDir === 'flat' || bDir === 'flat') return 'flat'
  if (!aDir || !bDir) return 'unavailable'
  return aDir === bDir ? 'aligned' : 'opposed'
}

function activeStatesFromEvents(events, group) {
  let extremeState = null
  let rotationState = null
  for (const e of events || []) {
    if (String(e.group || '') !== group) continue
    const type = String(e.event_type || '')
    if (type === 'absolute_extreme' || type === 'local_extreme') {
      extremeState = e.label || e.side || type
    }
    if (type === 'major_rotation' || type === 'rapid_velocity') {
      rotationState = e.label || e.side || type
    }
  }
  return { extremeState, rotationState }
}

function fmtContracts(n) {
  if (!isNum(n)) return null
  return Math.round(Math.abs(n)).toLocaleString('en-US')
}

function ordinal(n) {
  if (!isNum(n)) return null
  const v = Math.round(n)
  const mod10 = v % 10
  const mod100 = v % 100
  let suffix = 'th'
  if (mod10 === 1 && mod100 !== 11) suffix = 'st'
  else if (mod10 === 2 && mod100 !== 12) suffix = 'nd'
  else if (mod10 === 3 && mod100 !== 13) suffix = 'rd'
  return `${v}${suffix}`
}

function relationshipLabel(raw) {
  const m = {
    aligned: 'Aligned',
    opposed: 'Opposed',
    strong_opposition: 'Strong opposition',
    mixed: 'Mixed',
    unavailable: 'Unavailable',
  }
  return m[raw] || raw || 'Unavailable'
}

function flowLabel(raw) {
  const m = {
    opposition_widening_rapidly: 'Opposition widening rapidly',
    opposition_narrowing_rapidly: 'Opposition narrowing rapidly',
    opposition_widening: 'Opposition widening',
    opposition_narrowing: 'Opposition narrowing',
    spread_widening: 'Spread widening',
    spread_narrowing: 'Spread narrowing',
    stable: 'Stable',
    unavailable: 'Unavailable',
  }
  return m[raw] || raw || 'Unavailable'
}

/** Deterministic plain-English summary — fixed rules, no LLM. */
export function buildWeekSummaryText(week) {
  if (!week) return 'Unavailable'
  const parts = []
  const c = week.commercial
  const nc = week.nonCommercial

  if (c?.summaryLine) parts.push(c.summaryLine)
  else {
    const cCh = c?.change1w
    const cPct = ordinal(c?.percentile)
    if (isNum(cCh) && cPct) {
      const verb = cCh > 0 ? 'rose' : cCh < 0 ? 'fell' : 'was unchanged'
      const by = cCh === 0 ? '' : ` by ${fmtContracts(cCh)} contracts`
      parts.push(`Commercial positioning ${verb}${by} to the ${cPct} net percentile.`)
    }
  }

  if (nc?.summaryLine) parts.push(nc.summaryLine)
  else {
    const ncCh = nc?.change1w
    const ncPct = ordinal(nc?.percentile)
    if (isNum(ncCh) && ncPct) {
      const verb = ncCh > 0 ? 'rose' : ncCh < 0 ? 'fell' : 'was unchanged'
      const by = ncCh === 0 ? '' : ` by ${fmtContracts(ncCh)} contracts`
      parts.push(`Non-Commercial positioning ${verb}${by} to the ${ncPct} net percentile.`)
    }
  }

  const rel = week.spreads?.relationship
  const flow = week.spreads?.flow
  if (rel === 'opposed' || rel === 'strong_opposition') {
    parts.push(
      `Commercials and Non-Commercials are in ${relationshipLabel(rel).toLowerCase()}` +
        (flow && flow !== 'stable' && flow !== 'unavailable'
          ? ` — ${flowLabel(flow).toLowerCase()}.`
          : '.'),
    )
  } else if (rel === 'aligned') {
    parts.push('Commercial and Non-Commercial positioning are aligned.')
  }

  const active = []
  if (c?.extremeState) active.push('a Commercial extreme')
  if (c?.rotationState) active.push('a Commercial rotation')
  if (nc?.extremeState) active.push('a Non-Commercial extreme')
  if (nc?.rotationState) active.push('a Non-Commercial rotation')
  if (week.nonReportable?.extremeState) active.push('a Non-Reportable extreme')
  if (week.nonReportable?.rotationState) active.push('a Non-Reportable rotation')
  const divs = (week.events || []).filter((e) => e.event_type === 'comm_nr_divergence')
  if (divs.length) active.push('a Commercial–NR divergence')

  if (active.length === 1) parts.push(`${active[0][0].toUpperCase()}${active[0].slice(1)} is active.`)
  else if (active.length > 1) {
    const last = active[active.length - 1]
    parts.push(`${active.slice(0, -1).join(', ')} and ${last} are active.`)
  }

  return parts.length
    ? parts.join(' ')
    : 'Weekly positioning data is available; no notable event narrative for this week.'
}

/**
 * @param {object} opts
 * @param {Array} opts.timelineRows
 * @param {object|null} opts.researchBlock
 * @param {string} opts.instrument
 * @param {string|null} opts.loadedLatestDate
 * @param {boolean} [opts.staleView]
 */
export function buildWeeklyViewModel({
  timelineRows = [],
  researchBlock = null,
  instrument = '',
  loadedLatestDate = null,
  staleView = false,
} = {}) {
  const rows = Array.isArray(timelineRows) ? timelineRows : []
  const latestDate = sliceDate(
    rows.length ? rows[rows.length - 1]?.date || rows[rows.length - 1]?.label : null,
  )
  const sourceLatest = sliceDate(loadedLatestDate || researchBlock?.source_week || latestDate)

  const eventsByDate = new Map()
  for (const event of researchBlock?.markers || []) {
    const d = sliceDate(event.date)
    if (!d) continue
    if (!eventsByDate.has(d)) eventsByDate.set(d, [])
    eventsByDate.get(d).push({
      ...event,
      id: researchEventId(event),
      tone: eventTone(event),
      badge: eventBadge(event),
    })
  }

  const spreadByDate = new Map()
  for (const row of researchBlock?.spread_series || []) {
    const d = sliceDate(row.date)
    if (!d) continue
    spreadByDate.set(d, row)
  }

  // Generated inspector is metadata only and may lag the latest report.
  const inspectorByDate = new Map()
  const inspectorWeeks = researchBlock?.weekly_inspector?.weeks || []
  for (const w of inspectorWeeks) {
    const d = sliceDate(w.date)
    if (d) inspectorByDate.set(d, w)
  }

  // Runtime percentile series is built from genuine COT rows, independent of
  // whether the generated inspector has caught up to the latest report.
  const commercialRuntime = buildRuntimeRollingPercentiles(rows, inspectorByDate, 'commercial_net')
  const nonCommercialRuntime = buildRuntimeRollingPercentiles(rows, inspectorByDate, 'institutional_net')
  const nonReportableRuntime = buildRuntimeRollingPercentiles(rows, inspectorByDate, 'retail_net')

  const current = researchBlock?.current_state || null
  const currentDate = sliceDate(current?.commercial?.date || current?.spread?.date)
  const measureLabel = RUNTIME_PERCENTILE_LABEL

  const weeklyView = Object.create(null)

  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i]
    const date = sliceDate(row.date || row.label)
    if (!date) continue

    const rowHasCot =
      isNum(row.commercial_net) || isNum(row.institutional_net) || isNum(row.retail_net)

    const events = eventsByDate.get(date) || []
    const commercial = participantFromRow(rows, i, 'commercial_net', 'commercial_wow')
    const nonCommercial = participantFromRow(rows, i, 'institutional_net', 'institutional_wow')
    const nonReportable = participantFromRow(rows, i, 'retail_net', 'retail_wow')

    const inspResolved = resolveInspectorWeekForDate(inspectorByDate, date)
    const insp = inspResolved?.week || null

    // Only genuine COT rows may receive COT inspector data. If the compact
    // inspector lags the latest report, borrow metadata only from the previous
    // report; never borrow its nets, changes or percentiles.
    if (insp && rowHasCot) {
      const useNumericValues = Boolean(inspResolved?.exact)
      enrichFromInspectorPack(commercial, insp.commercial, { values: useNumericValues })
      enrichFromInspectorPack(nonCommercial, insp.noncommercial, { values: useNumericValues })
      enrichFromInspectorPack(nonReportable, insp.nonreportable, { values: useNumericValues })
      if (useNumericValues) {
        commercial.summaryLine = insp.summaries?.commercial || null
        nonCommercial.summaryLine = insp.summaries?.noncommercial || null
        nonReportable.summaryLine = insp.summaries?.nonreportable || null
      }
    }

    // Runtime percentiles are authoritative and bind to this exact COT report.
    // Price-only rows intentionally receive none, preventing duplicate weeks.
    if (rowHasCot) {
      applyRuntimePercentile(commercial, commercialRuntime.get(date))
      applyRuntimePercentile(nonCommercial, nonCommercialRuntime.get(date))
      applyRuntimePercentile(nonReportable, nonReportableRuntime.get(date))
    }

    for (const ev of events) {
      enrichFromResearchParticipant(commercial, ev.commercial)
      enrichFromResearchParticipant(nonCommercial, ev.noncommercial)
      enrichFromResearchParticipant(nonReportable, ev.nonreportable)
    }

    if (current && currentDate === date && rowHasCot) {
      enrichFromCurrentState(commercial, current.commercial)
      enrichFromCurrentState(nonCommercial, current.noncommercial)
      enrichFromCurrentState(nonReportable, current.nonreportable)
    }

    const cStates = activeStatesFromEvents(events, 'commercial')
    const ncStates = activeStatesFromEvents(events, 'noncommercial')
    const nrStates = activeStatesFromEvents(events, 'nonreportable')
    commercial.extremeState = cStates.extremeState
    commercial.rotationState = cStates.rotationState
    nonCommercial.extremeState = ncStates.extremeState
    nonCommercial.rotationState = ncStates.rotationState
    nonReportable.extremeState = nrStates.extremeState
    nonReportable.rotationState = nrStates.rotationState

    const spreadRow = spreadByDate.get(date)
    const cross = rowHasCot ? insp?.cross || null : null

    let commNrValue = isNum(commercial.percentile) && isNum(nonReportable.percentile)
      ? commercial.percentile - nonReportable.percentile
      : isNum(cross?.comm_nr_spread)
        ? cross.comm_nr_spread
        : isNum(spreadRow?.spread)
          ? spreadRow.spread
          : null
    let commNrPct = isNum(cross?.comm_nr_spread_percentile)
      ? cross.comm_nr_spread_percentile
      : isNum(spreadRow?.spread_percentile)
        ? spreadRow.spread_percentile
        : null

    if (currentDate === date && rowHasCot && isNum(current?.spread?.spread)) {
      if (!isNum(commNrValue)) commNrValue = current.spread.spread
      if (!isNum(commNrPct)) commNrPct = current.spread.spread_percentile
    }
    for (const ev of events) {
      if (!isNum(commNrValue) && isNum(ev?.spread?.value)) commNrValue = ev.spread.value
      if (!isNum(commNrPct) && isNum(ev?.spread?.percentile)) commNrPct = ev.spread.percentile
    }

    const commNcContracts =
      isNum(commercial.net) && isNum(nonCommercial.net)
        ? commercial.net - nonCommercial.net
        : null
    const commNcPctSpread =
      isNum(commercial.percentile) && isNum(nonCommercial.percentile)
        ? round2(commercial.percentile - nonCommercial.percentile)
        : null
    const commNcPctSpreadPct = isNum(cross?.comm_nc_spread_percentile)
      ? cross.comm_nc_spread_percentile
      : null

    let freshness = 'historical'
    if (date === latestDate) {
      freshness = staleView || (sourceLatest && sourceLatest !== latestDate) ? 'stale' : 'latest'
    }

    const week = {
      date,
      time: isNum(row.time) ? row.time : null,
      instrument,
      freshness,
      measureLabel,
      isCotReport: rowHasCot,
      inspectorAsOfDate: rowHasCot ? inspResolved?.asOfDate || null : null,
      inspectorExact: rowHasCot && Boolean(inspResolved?.exact),
      price: {
        close: isNum(row.close) ? row.close : isNum(row.price) ? row.price : null,
      },
      commercial,
      nonCommercial,
      nonReportable,
      spreads: {
        commNc: {
          value: isNum(commNcPctSpread) ? commNcPctSpread : commNcContracts,
          percentile: commNcPctSpreadPct,
          change1w:
            isNum(commercial.percentileChange1w) && isNum(nonCommercial.percentileChange1w)
              ? round2(commercial.percentileChange1w - nonCommercial.percentileChange1w)
              : null,
          change4w:
            isNum(commercial.percentileChange4w) && isNum(nonCommercial.percentileChange4w)
              ? round2(commercial.percentileChange4w - nonCommercial.percentileChange4w)
              : null,
          valueKind: isNum(commNcPctSpread) ? 'percentile_spread' : 'contract_spread',
        },
        commNr: { value: commNrValue, percentile: commNrPct },
        commNcAlignment: alignment(commercial.direction, nonCommercial.direction),
        commNrAlignment: alignment(commercial.direction, nonReportable.direction),
        relationship: cross?.relationship || null,
        flow: cross?.flow || null,
        commercialPercentile: commercial.percentile,
        noncommercialPercentile: nonCommercial.percentile,
        nonreportablePercentile: nonReportable.percentile,
      },
      events,
      activeDivergence: events
        .filter((e) => e.event_type === 'comm_nr_divergence')
        .map((e) => e.label || e.side || 'Divergence'),
      activeEventNames: events.map((e) => e.label || e.badge || e.event_type).filter(Boolean),
    }
    week.summary = buildWeekSummaryText(week)
    week.integrityMissing = rowHasCot ? missingRequiredInspectorFields(week) : []
    week.integrityOk = rowHasCot ? week.integrityMissing.length === 0 : true
    weeklyView[date] = week
  }

  return {
    weeklyView,
    latestDate: latestDate || null,
    dates: rows.map((r) => sliceDate(r.date || r.label)).filter(Boolean),
    measureLabel,
    inspectorWeekCount: inspectorByDate.size,
    percentileSource: 'timeline_cot_rolling_156',
  }
}

export function resolveInspectedWeek({
  weeklyView,
  selectedWeek,
  hoveredWeek,
  latestDate,
} = {}) {
  const map = weeklyView || {}
  if (selectedWeek && map[selectedWeek]) return map[selectedWeek]
  if (hoveredWeek && map[hoveredWeek]) return map[hoveredWeek]
  if (latestDate && map[latestDate]) return map[latestDate]
  return null
}
