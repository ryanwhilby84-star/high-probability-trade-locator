import React from 'react'
import { positioningBasis, formatRangeIndex, describeIndexSummary } from './positioningBasis.js'

import { shortSampleQuality, fmtRet } from './researchEventUi.js'
import { BasicLookbackPanel } from './BasicLookbackPanel.jsx'

function fmtNum(v) {
  if (v == null || !Number.isFinite(Number(v))) return 'Unavailable'
  return Number(v).toLocaleString('en-US', { maximumFractionDigits: 0 })
}

function fmtSigned(v) {
  if (v == null || !Number.isFinite(Number(v))) return 'Unavailable'
  const n = Number(v)
  const sign = n > 0 ? '+' : ''
  return `${sign}${n.toLocaleString('en-US', { maximumFractionDigits: 0 })}`
}

function fmtPrice(v) {
  if (v == null || !Number.isFinite(Number(v))) return 'Unavailable'
  const n = Number(v)
  if (Math.abs(n) >= 100) return n.toLocaleString('en-US', { maximumFractionDigits: 2 })
  return n.toLocaleString('en-US', { maximumFractionDigits: 5 })
}

function fmtPctPts(v) {
  if (v == null || !Number.isFinite(Number(v))) return 'Unavailable'
  const n = Number(v)
  const sign = n > 0 ? '+' : ''
  return `${sign}${n.toFixed(1)}`
}

function freshnessLabel(f) {
  if (f === 'latest') return 'Latest'
  if (f === 'stale') return 'Stale'
  return 'Historical'
}

function relationshipLabel(v) {
  const m = {
    aligned: 'Aligned',
    opposed: 'Opposed',
    strong_opposition: 'Strong opposition',
    mixed: 'Mixed',
  }
  return m[v] || 'Unavailable'
}

function flowLabel(v) {
  const m = {
    opposition_widening_rapidly: 'Opposition widening rapidly',
    opposition_narrowing_rapidly: 'Opposition narrowing rapidly',
    opposition_widening: 'Opposition widening',
    opposition_narrowing: 'Opposition narrowing',
    spread_widening: 'Spread widening',
    spread_narrowing: 'Spread narrowing',
    stable: 'Stable',
  }
  return m[v] || 'Unavailable'
}

function groupTitle(group) {
  if (group === 'commercial') return 'COMMERCIAL'
  if (group === 'noncommercial') return 'NON-COMMERCIAL'
  if (group === 'nonreportable') return 'NON-REPORTABLE'
  return String(group || 'EVENT').toUpperCase()
}

function Metric({ label, value, className = '', title = '' }) {
  return (
    <div className={`cot-ws-insp-metric ${className}`.trim()} title={title || undefined}>
      <span className="cot-ws-insp-metric-label">{label} (3Y)</span>
      <span className="cot-ws-insp-metric-value">{value}</span>
    </div>
  )
}

function FlowBadge({ p, percentile, rangeId }) {
  if (!p) return null
  const temp = p.temperature || 'neutral'
  const arrow = p.directionArrow || '·'
  const label = p.stateLabel || '—'
  const pct =
    percentile != null && Number.isFinite(Number(percentile))
      ? `${formatRangeIndex(percentile)} / 100 · ${rangeId.toUpperCase()}`
      : '—'
  return (
    <div
      className={`cot-ws-insp-flow cot-ws-insp-flow--${temp}`}
      title="Selected-window COT range index; rotation state and arrow use the separate 3Y baseline"
    >
      <span className="cot-ws-insp-flow-pct">{pct}</span>
      <span className="cot-ws-insp-flow-arrow" aria-hidden="true">
        {arrow}
      </span>
      <span className="cot-ws-insp-flow-label">{label}</span>
    </div>
  )
}

function ParticipantColumn({ title, p, rangeId = "3y", history, date, netKey }) {
  if (!p) {
    return (
      <div className="cot-ws-insp-col">
        <h4 className="cot-ws-insp-col-title">{title}</h4>
        <p className="cot-ws-insp-muted">Unavailable</p>
      </div>
    )
  }
  const temp = p.temperature || 'neutral'
  // The chart series can be shorter than the independent historical COT export.
  // Trust only an explicitly exported rank for the selected window; the exporter
  // is responsible for coverage validation and leaves unsupported ranks null.
  // ALL uses the full available historical COT range, not the 3Y baseline.
  const percentileRangeId = rangeId
  const rawRank = p.percentilesByWindow?.[percentileRangeId]
  const rank = rawRank != null && Number.isFinite(Number(rawRank)) && Number(rawRank) >= 0 && Number(rawRank) <= 100
    ? Number(rawRank)
    : null
  const pctile =
    rank == null ? 'Insufficient history' : `${formatRangeIndex(rank)} / 100`
  const basis = positioningBasis(history, date, netKey, rangeId)
  const mismatch = basis && rank != null && (basis.current !== p.net || basis.index == null || Math.abs(basis.index - rank) > 0.02)
  const pct4 =
    p.percentileChange4w == null || !Number.isFinite(Number(p.percentileChange4w))
      ? 'Unavailable'
      : `${fmtPctPts(p.percentileChange4w)} ${p.directionArrow || ''}`.trim()
  return (
    <div className={`cot-ws-insp-col cot-ws-insp-col--temp-${temp}`}>
      <h4 className="cot-ws-insp-col-title">{title}</h4>
      <FlowBadge p={p} percentile={mismatch ? null : rank} rangeId={rangeId} />
      {mismatch ? <p role="alert">DATA MISMATCH: exported index does not match this chart history. Do not interpret this value.</p> : null}
      <div className="cot-ws-insp-metrics">
        <Metric label="Net" value={fmtNum(p.net)} />
        <Metric label="1W" value={fmtSigned(p.change1w)} />
        <Metric label="4W" value={fmtSigned(p.change4w)} />
        <Metric label="12W" value={fmtSigned(p.change12w)} />
        <Metric
          label={`${percentileRangeId.toUpperCase()} COT range index`}
          value={mismatch ? 'Data mismatch' : pctile}
          title={`CFTC net positioning min-max range over ${percentileRangeId.toUpperCase()} available history`}
        />
        <Metric
          label="3Y index Δ · 1W"
          value={`${fmtPctPts(p.percentileChange1w)} ${p.directionArrow || ''}`.trim()}
          title="Change in 3Y COT range index over 1 week (index points)"
        />
        <Metric
          label="3Y index Δ · 4W"
          value={pct4}
          title="Change in 3Y COT range index over 4 weeks (index points)"
        />
        <Metric
          label="3Y index Δ · 12W"
          value={fmtPctPts(p.percentileChange12w)}
          title="Change in 3Y COT range index over 12 weeks (index points)"
        />
        <Metric
          label="3Y analysis observations"
          value={
            p.percentileObservationCount != null
              ? String(p.percentileObservationCount) + ' (3Y baseline)'
              : 'Unavailable'
          }
        />
        <Metric label="3Y extreme" value={p.extremeState || (p.isExtreme ? 'Yes' : 'None')} />
        <Metric label="3Y rotation" value={p.rotationState || 'None'} />
        <Metric
          label="3Y state"
          value={p.stateLabel || 'Unavailable'}
          className="cot-ws-insp-metric--state"
        />
      </div>
      {basis ? <details className="cot-ws-insp-index-basis"><summary>{rangeId.toUpperCase()} calculation evidence</summary><p>{basis.start} to {basis.end} · {basis.count} valid weekly observations</p><p>Net {fmtNum(basis.current)} · minimum {fmtNum(basis.minimum)} · maximum {fmtNum(basis.maximum)}</p><p>(Net − minimum) ÷ (maximum − minimum) × 100 = {basis.index == null ? 'Unavailable (flat range)' : formatRangeIndex(basis.index)}</p><p>This measures distance within the positioning range, not the percentage of historical weeks below today.</p></details> : null}
      {p.summaryLine ? <p className="cot-ws-insp-group-summary"><strong>3Y analysis: </strong>{describeIndexSummary(p.summaryLine)}</p> : null}
    </div>
  )
}

function EventNarratives({ events, selectedEventId, onSelectEvent }) {
  const list = Array.isArray(events) ? events : []
  if (!list.length) return null
  return (
    <div className="cot-ws-insp-narratives" aria-label="Active events this week">
      {list.map((ev) => {
        const selected = selectedEventId === ev.id
        const title = `${groupTitle(ev.group)} ${
          String(ev.event_type || '').includes('rotation') ? 'ROTATION' : 'EVENT'
        }`
        return (
          <button
            key={ev.id}
            type="button"
            className={`cot-ws-insp-narrative cot-ws-insp-narrative--${ev.tone || 'extreme'}${
              selected ? ' is-selected' : ''
            }`}
            onClick={() => onSelectEvent?.(ev)}
          >
            <span className="cot-ws-insp-narrative-kicker">{title}</span>
            <span className="cot-ws-insp-narrative-label">{ev.label || ev.event_type}</span>
            {ev.explanation ? (
              <span className="cot-ws-insp-narrative-body">{ev.explanation}</span>
            ) : null}
          </button>
        )
      })}
    </div>
  )
}

/**
 * Click-to-open weekly inspector drawer with directional flow.
 * Renders nothing when closed — charts reclaim the space.
 */
export function WeeklyInspector({
  week,
  open = false,
  selectedEventId = null,
  onClose,
  onJumpToWeek,
  onSelectEvent,
  analysisOpen = false,
  onToggleAnalysis,
  rangeId = "3y",
  history = [],
}) {
  const [expanded, setExpanded] = React.useState(false)

  React.useEffect(() => {
    if (!open) setExpanded(false)
  }, [open, week?.date])

  if (!open || !week) return null

  const selectedEvent =
    (week.events || []).find((e) => e.id === selectedEventId) ||
    (week.events || [])[0] ||
    null
  const analogues = selectedEvent?.analogues || {}
  const o4 = (analogues.outcomes_by_horizon || {})['4'] || {}
  const o8 = (analogues.outcomes_by_horizon || {})['8'] || {}
  const o12 = (analogues.outcomes_by_horizon || {})['12'] || {}
  const eventNames = week.activeEventNames || []
  const sp = week.spreads || {}
  const integrityMissing = Array.isArray(week.integrityMissing) ? week.integrityMissing : []
  const integrityFailed = week.integrityOk === false || integrityMissing.length > 0

  return (
    <aside
      className={`cot-ws-weekly-inspector cot-ws-weekly-inspector--drawer${
        expanded ? ' is-expanded' : ' is-collapsed'
      }${integrityFailed ? ' cot-ws-weekly-inspector--integrity-fail' : ''}`}
      aria-label="Weekly inspector"
      data-open="1"
      data-selected-week={week.date || ''}
      data-selected-event={selectedEventId || ''}
      data-integrity={integrityFailed ? 'fail' : 'pass'}
    >
      <div className="cot-ws-insp-top">
        <span className="cot-ws-insp-kicker">Weekly Inspector</span>
        <span className="cot-ws-insp-title">{week.instrument || 'Instrument'}</span>
        <span className="cot-ws-insp-date">{week.date}</span>
        <span className="cot-ws-insp-price">
          Close <strong>{fmtPrice(week.price?.close)}</strong>
        </span>
        <span className={`cot-ws-insp-fresh cot-ws-insp-fresh--${week.freshness}`}>
          {freshnessLabel(week.freshness)}
        </span>
        <span className="cot-ws-insp-locked">Locked</span>
        <span
          className="cot-ws-insp-measure"
          title={week.measureLabel || 'COT range index, not statistical percentile rank'}
        >
          COT range index
        </span>
        <div className="cot-ws-insp-head-actions">
          {onToggleAnalysis ? (
            <button
              type="button"
              className={`cot-ws-insp-btn cot-ws-insp-btn--ghost${
                analysisOpen ? ' is-active' : ''
              }`}
              aria-pressed={analysisOpen}
              onClick={onToggleAnalysis}
              title="Open Weekly Analysis"
            >
              Analysis
            </button>
          ) : null}
          {week.time != null && onJumpToWeek ? (
            <button
              type="button"
              className="cot-ws-insp-btn cot-ws-insp-btn--ghost"
              onClick={() => onJumpToWeek(week.date)}
              title="Jump chart viewport to this week"
            >
              Jump
            </button>
          ) : null}
          <button
            type="button"
            className="cot-ws-insp-btn cot-ws-insp-btn--ghost"
            aria-expanded={expanded}
            onClick={() => setExpanded((v) => !v)}
            title={expanded ? 'Collapse detail' : 'Expand detail'}
          >
            {expanded ? 'Less' : 'More'}
          </button>
          <button
            type="button"
            className="cot-ws-insp-btn"
            onClick={onClose}
            title="Close weekly inspector"
          >
            Close
          </button>
        </div>
      </div>

      {integrityFailed ? (
        <div className="cot-ws-insp-integrity-fail" role="alert">
          <strong>DATA INTEGRITY FAILURE</strong>
          <p>Derived COT statistics are incomplete for this instrument.</p>
          <p>
            Instrument: <code>{week.instrument || '—'}</code>
          </p>
          <p>
            Report week: <code>{week.date || '—'}</code>
            {week.inspectorAsOfDate && week.inspectorAsOfDate !== week.date
              ? ` (inspector as-of ${week.inspectorAsOfDate})`
              : ''}
          </p>
          <p>Stage: Derived COT</p>
          <p>Missing fields:</p>
          <ul className="cot-ws-insp-integrity-missing">
            {(integrityMissing.length ? integrityMissing : ['required derived fields']).map(
              (f) => (
                <li key={f}>{f}</li>
              ),
            )}
          </ul>
        </div>
      ) : null}

      {!integrityFailed ? (
        <>
          <EventNarratives
            events={week.events}
            selectedEventId={selectedEventId}
            onSelectEvent={onSelectEvent}
          />

          <p className="cot-ws-insp-group-summary"><strong>{rangeId.toUpperCase()} COT range index (0–100).</strong> Not a statistical percentile rank. Participant movement, rotation and written participant analysis below use a separate 3Y baseline.</p>
          <BasicLookbackPanel week={week} />

          <div className="cot-ws-insp-columns" role="group" aria-label="Weekly breakdown">
            <ParticipantColumn title="Commercial" p={week.commercial} rangeId={rangeId} history={history} date={week.date} netKey="commercial_net" />
            <ParticipantColumn title="Non-Commercial" p={week.nonCommercial} rangeId={rangeId} history={history} date={week.date} netKey="institutional_net" />
            <ParticipantColumn title="Non-Reportable" p={week.nonReportable} rangeId={rangeId} history={history} date={week.date} netKey="retail_net" />
            <div className="cot-ws-insp-col cot-ws-insp-col--cross">
              <h4 className="cot-ws-insp-col-title">Cross-Group · baselines labelled</h4>
              <div className="cot-ws-insp-metrics">
                <Metric
                  label="C · 3Y index"
                  value={
                    formatRangeIndex(sp.commercialPercentile) === '—'
                      ? 'Unavailable'
                      : formatRangeIndex(sp.commercialPercentile)
                  }
                />
                <Metric
                  label="NC · 3Y index"
                  value={
                    formatRangeIndex(sp.noncommercialPercentile) === '—'
                      ? 'Unavailable'
                      : formatRangeIndex(sp.noncommercialPercentile)
                  }
                />
                <Metric
                  label="NR · 3Y index"
                  value={
                    formatRangeIndex(sp.nonreportablePercentile) === '—'
                      ? 'Unavailable'
                      : formatRangeIndex(sp.nonreportablePercentile)
                  }
                />
                <Metric
                  label="C−NC · 3Y index spread"
                  value={
                    sp.commNc?.valueKind === 'percentile_spread'
                      ? fmtPctPts(sp.commNc?.value)
                      : fmtNum(sp.commNc?.value)
                  }
                  title="Commercial net percentile minus Non-Commercial net percentile"
                />
                <Metric
                  label="3Y spread Δ · 1W"
                  value={fmtPctPts(sp.commNc?.change1w)}
                  title="1W change in Comm−NC percentile spread"
                />
                <Metric
                  label="3Y spread Δ · 4W"
                  value={fmtPctPts(sp.commNc?.change4w)}
                  title="4W change in Comm−NC percentile spread"
                />
                <Metric
                  label="C−NR · expanding index"
                  value={
                    formatRangeIndex(sp.commNr?.percentile) === '—'
                      ? 'Unavailable'
                      : formatRangeIndex(sp.commNr?.percentile)
                  }
                  title="Expanding percentile of Comm−NR spread"
                />
                <Metric label="Relation" value={relationshipLabel(sp.relationship)} />
                <Metric label="Flow" value={flowLabel(sp.flow)} />
                <Metric
                  label="Events"
                  value={eventNames.length ? eventNames.join(' · ') : 'None'}
                />
              </div>
            </div>
          </div>

          <div className="cot-ws-insp-summary-row">
            <p className="cot-ws-insp-summary"><strong>3Y analysis: </strong>{describeIndexSummary(week.summary)}</p>
          </div>

          {expanded ? (
            <div className="cot-ws-insp-detail">
              <div className="cot-ws-insp-detail-events">
                <div className="cot-ws-insp-detail-label">Events</div>
                {(week.events || []).length === 0 ? (
                  <p className="cot-ws-insp-muted">No detected events this week.</p>
                ) : (
                  <ul className="cot-ws-insp-events">
                    {week.events.map((ev) => (
                      <li key={ev.id}>
                        <button
                          type="button"
                          className={`cot-ws-insp-event${
                            selectedEventId === ev.id ? ' is-selected' : ''
                          } cot-ws-insp-event--${ev.tone || 'extreme'}`}
                          onClick={() => onSelectEvent?.(ev)}
                          title={ev.explanation || ev.label || ev.event_type}
                        >
                          <span className="cot-ws-insp-event-badge">{ev.badge}</span>
                          <span className="cot-ws-insp-event-label">
                            {ev.label || ev.event_type}
                          </span>
                        </button>
                        {selectedEventId === ev.id && ev.explanation ? (
                          <p className="cot-ws-insp-event-explain">{ev.explanation}</p>
                        ) : null}
                      </li>
                    ))}
                  </ul>
                )}
              </div>
              <div className="cot-ws-insp-detail-analogues">
                <div className="cot-ws-insp-detail-label">Historical Analogues</div>
                {!selectedEvent ? (
                  <p className="cot-ws-insp-muted">Select an event to load analogues.</p>
                ) : (
                  <>
                    <div className="cot-ws-insp-analogue-head">{selectedEvent.label}</div>
                    <div className="cot-ws-insp-cases">
                      <strong>{analogues.independent_case_count ?? 0}</strong> independent
                      cases · {shortSampleQuality(analogues.sample_quality)}
                    </div>
                    <div className="cot-ws-insp-horizons">
                      <span>4W {fmtRet(o4.median_return_pct)}</span>
                      <span>8W {fmtRet(o8.median_return_pct)}</span>
                      <span>12W {fmtRet(o12.median_return_pct)}</span>
                    </div>
                  </>
                )}
              </div>
            </div>
          ) : null}
        </>
      ) : null}
    </aside>
  )
}

/** Ultra-compact hover tip — date + active event names only. */
export function WeekHoverChip({ week }) {
  if (!week) return null
  const names = week.activeEventNames || []
  return (
    <div className="cot-ws-week-hover-chip" role="status">
      <div className="cot-ws-week-hover-chip-date">{week.date}</div>
      {names.length ? (
        <ul className="cot-ws-week-hover-chip-events">
          {names.slice(0, 4).map((n) => (
            <li key={n}>{n}</li>
          ))}
        </ul>
      ) : (
        <div className="cot-ws-week-hover-chip-none">No events</div>
      )}
    </div>
  )
}
