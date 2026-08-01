import React from 'react'

import { fmtPctile, shortSampleQuality, fmtRet } from './researchEventUi.js'

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
      <span className="cot-ws-insp-metric-label">{label}</span>
      <span className="cot-ws-insp-metric-value">{value}</span>
    </div>
  )
}

function FlowBadge({ p }) {
  if (!p) return null
  const temp = p.temperature || 'neutral'
  const arrow = p.directionArrow || '·'
  const label = p.stateLabel || '—'
  const pct =
    p.percentile != null && Number.isFinite(Number(p.percentile))
      ? `${Math.round(Number(p.percentile))}th`
      : '—'
  return (
    <div
      className={`cot-ws-insp-flow cot-ws-insp-flow--${temp}`}
      title="Net positioning expanding percentile + recent percentile-point change"
    >
      <span className="cot-ws-insp-flow-pct">{pct}</span>
      <span className="cot-ws-insp-flow-arrow" aria-hidden="true">
        {arrow}
      </span>
      <span className="cot-ws-insp-flow-label">{label}</span>
    </div>
  )
}

function ParticipantColumn({ title, p }) {
  if (!p) {
    return (
      <div className="cot-ws-insp-col">
        <h4 className="cot-ws-insp-col-title">{title}</h4>
        <p className="cot-ws-insp-muted">Unavailable</p>
      </div>
    )
  }
  const temp = p.temperature || 'neutral'
  const pctile =
    fmtPctile(p.percentile) === '—' ? 'Unavailable' : fmtPctile(p.percentile)
  const pct4 =
    p.percentileChange4w == null || !Number.isFinite(Number(p.percentileChange4w))
      ? 'Unavailable'
      : `${fmtPctPts(p.percentileChange4w)} ${p.directionArrow || ''}`.trim()
  return (
    <div className={`cot-ws-insp-col cot-ws-insp-col--temp-${temp}`}>
      <h4 className="cot-ws-insp-col-title">{title}</h4>
      <FlowBadge p={p} />
      <div className="cot-ws-insp-metrics">
        <Metric label="Net" value={fmtNum(p.net)} />
        <Metric label="1W" value={fmtSigned(p.change1w)} />
        <Metric label="4W" value={fmtSigned(p.change4w)} />
        <Metric label="12W" value={fmtSigned(p.change12w)} />
        <Metric
          label="Net percentile"
          value={pctile}
          title="Expanding net-positioning percentile through this week (no look-ahead)"
        />
        <Metric
          label="1W pct movement"
          value={`${fmtPctPts(p.percentileChange1w)} ${p.directionArrow || ''}`.trim()}
          title="Change in net percentile over 1 week (percentile points)"
        />
        <Metric
          label="4W pct movement"
          value={pct4}
          title="Change in net percentile over 4 weeks (percentile points)"
        />
        <Metric
          label="12W pct movement"
          value={fmtPctPts(p.percentileChange12w)}
          title="Change in net percentile over 12 weeks (percentile points)"
        />
        <Metric
          label="Obs count"
          value={
            p.percentileObservationCount != null
              ? String(p.percentileObservationCount)
              : 'Unavailable'
          }
        />
        <Metric label="Extreme" value={p.extremeState || (p.isExtreme ? 'Yes' : 'None')} />
        <Metric label="Rotation" value={p.rotationState || 'None'} />
        <Metric
          label="State"
          value={p.stateLabel || 'Unavailable'}
          className="cot-ws-insp-metric--state"
        />
      </div>
      {p.summaryLine ? <p className="cot-ws-insp-group-summary">{p.summaryLine}</p> : null}
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
          title={week.measureLabel || 'Net positioning expanding percentile'}
        >
          Net pctile
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

          <div className="cot-ws-insp-columns" role="group" aria-label="Weekly breakdown">
            <ParticipantColumn title="Commercial" p={week.commercial} />
            <ParticipantColumn title="Non-Commercial" p={week.nonCommercial} />
            <ParticipantColumn title="Non-Reportable" p={week.nonReportable} />
            <div className="cot-ws-insp-col cot-ws-insp-col--cross">
              <h4 className="cot-ws-insp-col-title">Cross-Group</h4>
              <div className="cot-ws-insp-metrics">
                <Metric
                  label="C pctile"
                  value={
                    fmtPctile(sp.commercialPercentile) === '—'
                      ? 'Unavailable'
                      : fmtPctile(sp.commercialPercentile)
                  }
                />
                <Metric
                  label="NC pctile"
                  value={
                    fmtPctile(sp.noncommercialPercentile) === '—'
                      ? 'Unavailable'
                      : fmtPctile(sp.noncommercialPercentile)
                  }
                />
                <Metric
                  label="NR pctile"
                  value={
                    fmtPctile(sp.nonreportablePercentile) === '—'
                      ? 'Unavailable'
                      : fmtPctile(sp.nonreportablePercentile)
                  }
                />
                <Metric
                  label="C−NC pct"
                  value={
                    sp.commNc?.valueKind === 'percentile_spread'
                      ? fmtPctPts(sp.commNc?.value)
                      : fmtNum(sp.commNc?.value)
                  }
                  title="Commercial net percentile minus Non-Commercial net percentile"
                />
                <Metric
                  label="C−NC 1W"
                  value={fmtPctPts(sp.commNc?.change1w)}
                  title="1W change in Comm−NC percentile spread"
                />
                <Metric
                  label="C−NC 4W"
                  value={fmtPctPts(sp.commNc?.change4w)}
                  title="4W change in Comm−NC percentile spread"
                />
                <Metric
                  label="C−NR pct"
                  value={
                    fmtPctile(sp.commNr?.percentile) === '—'
                      ? 'Unavailable'
                      : fmtPctile(sp.commNr?.percentile)
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
            <p className="cot-ws-insp-summary">{week.summary}</p>
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
