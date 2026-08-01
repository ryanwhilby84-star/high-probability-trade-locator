import React from 'react'

import {
  DEFAULT_LAYER_STATE,
  MARKER_LEGEND,
  RESEARCH_LAYERS,
  eventBadge,
  eventTone,
  fmtPctile,
  fmtRet,
  shortSampleQuality,
} from './researchEventUi.js'

function horizonBlock(label, o) {
  const n = o?.n
  const higher = o?.higher_count
  return (
    <div className="cot-ws-research-horizon">
      <div className="cot-ws-research-horizon-label">{label}</div>
      <div className="cot-ws-research-horizon-body">
        {n != null ? `${higher ?? '—'}/${n} higher` : '—'}
        <br />
        Median {fmtRet(o?.median_return_pct)}
      </div>
    </div>
  )
}

export function ResearchLayerBar({ layerState, onChange, eventCounts }) {
  return (
    <div className="cot-ws-research-bar" role="group" aria-label="Research event layers">
      {Object.values(RESEARCH_LAYERS).map((layer) => {
        const on = Boolean(layerState[layer.id])
        const count = eventCounts?.[layer.id] ?? 0
        return (
          <button
            key={layer.id}
            type="button"
            className={`cot-ws-research-toggle${on ? ' is-on' : ''}`}
            title={layer.hint}
            aria-pressed={on}
            onClick={() => onChange({ ...layerState, [layer.id]: !on })}
          >
            <span className="cot-ws-research-toggle-label">{layer.label}</span>
            <span className="cot-ws-research-toggle-count">{count}</span>
          </button>
        )
      })}
    </div>
  )
}

export function ResearchMarkerLegend() {
  return (
    <div className="cot-ws-research-legend" aria-label="Marker legend">
      {MARKER_LEGEND.map((row) => (
        <span key={row.key} className="cot-ws-research-legend-item">
          <span
            className="cot-ws-research-legend-shape"
            style={{ background: row.color, color: '#0f172a' }}
          >
            {row.shape}
          </span>
          {row.label}
        </span>
      ))}
    </div>
  )
}

function chipGroupTag(event) {
  const g = String(event?.group || '')
  if (g === 'noncommercial') return 'NC'
  if (g === 'nonreportable') return 'NR'
  if (g === 'commercial') return 'C'
  if (event?.event_type === 'comm_nr_divergence') return 'DIV'
  return ''
}

function countByTone(events) {
  const counts = { EX: 0, DIV: 0, ROT: 0, NR: 0 }
  for (const e of events || []) {
    const tag = eventBadge(e)
    if (counts[tag] != null) counts[tag] += 1
  }
  return counts
}

/**
 * Compact collapsible event navigator — collapsed by default (one thin row).
 * Chip click selects week/event; does not move the chart camera.
 */
export function ResearchEventNavigator({
  events,
  selectedDate,
  selectedEventId,
  onSelect,
  collapsed: collapsedProp,
  onCollapsedChange,
}) {
  const [internalCollapsed, setInternalCollapsed] = React.useState(true)
  const collapsed =
    typeof collapsedProp === 'boolean' ? collapsedProp : internalCollapsed
  const setCollapsed = onCollapsedChange || setInternalCollapsed
  const counts = React.useMemo(() => countByTone(events), [events])
  const total = events?.length ?? 0

  return (
    <div className="cot-ws-event-nav" aria-label="Event navigator">
      <div className="cot-ws-event-nav-row">
        <button
          type="button"
          className="cot-ws-event-nav-toggle"
          aria-expanded={!collapsed}
          onClick={() => setCollapsed(!collapsed)}
        >
          {collapsed ? '▸ Events' : '▾ Events'}
        </button>
        <span className="cot-ws-event-nav-counts" aria-label="Event counts by type">
          <span className="cot-ws-event-nav-count cot-ws-event-nav-count--extreme">
            EX {counts.EX}
          </span>
          <span className="cot-ws-event-nav-count cot-ws-event-nav-count--divergence">
            DIV {counts.DIV}
          </span>
          <span className="cot-ws-event-nav-count cot-ws-event-nav-count--rotation">
            ROT {counts.ROT}
          </span>
          <span className="cot-ws-event-nav-count cot-ws-event-nav-count--nr">
            NR {counts.NR}
          </span>
          <span className="cot-ws-event-nav-total">{total} shown</span>
        </span>
      </div>

      {!collapsed && total > 0 ? (
        <div className="cot-ws-research-chip-row" aria-label="Research events">
          {events.map((event) => {
            const tone = eventTone(event)
            const tag = eventBadge(event)
            const groupTag = chipGroupTag(event)
            const eventId = [
              String(event.date || '').slice(0, 10),
              event.event_type || '',
              event.group || '',
              event.side || '',
              event.label || '',
            ].join('|')
            const selected =
              (selectedEventId && eventId === selectedEventId) ||
              (!selectedEventId &&
                selectedDate &&
                String(event.date || '').slice(0, 10) ===
                  String(selectedDate).slice(0, 10))
            return (
              <button
                key={eventId}
                type="button"
                className={`cot-ws-research-chip cot-ws-research-chip--${tone}${
                  selected ? ' is-selected' : ''
                }`}
                title={event.label || tag}
                onClick={() => onSelect?.(event)}
              >
                <span className="cot-ws-research-chip-tag">{tag}</span>
                {groupTag ? (
                  <span className="cot-ws-research-chip-group">{groupTag}</span>
                ) : null}
                <span className="cot-ws-research-chip-date">{event.date}</span>
              </button>
            )
          })}
        </div>
      ) : null}
    </div>
  )
}

/** @deprecated Prefer ResearchEventNavigator — kept for import compatibility. */
export function ResearchEventChipRow(props) {
  return <ResearchEventNavigator {...props} collapsed={false} />
}

/**
 * Compact hover tip from weeklyView — date + event names only.
 * Never opens the inspector; no layout impact.
 */
export function WeeklyHoverTooltip({ week, hidden = false }) {
  if (!week || hidden) return null
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
        <div className="cot-ws-week-hover-chip-none">No events · click to inspect</div>
      )}
    </div>
  )
}

/** Compact hover tooltip — positioning + outcome preview. */
export function ResearchHoverTooltip({ event }) {
  if (!event) return null
  const c = event.commercial || {}
  const nr = event.nonreportable || {}
  const sp = event.spread || {}
  const a = event.analogues || {}
  const o4 = (a.outcomes_by_horizon || {})['4'] || {}
  const o8 = (a.outcomes_by_horizon || {})['8'] || {}
  const o12 = (a.outcomes_by_horizon || {})['12'] || {}
  const v4 = c.velocity?.['4w']?.percentile_change
  const v12 = c.velocity?.['12w']?.percentile_change

  return (
    <div className="cot-ws-research-tooltip" role="status">
      <div className="cot-ws-research-tooltip-title">{event.label || event.event_type}</div>
      <div className="cot-ws-research-tooltip-grid">
        <span>Date</span>
        <strong>{event.date}</strong>
        <span>Commercial</span>
        <strong>{fmtPctile(c.long_history_percentile)}</strong>
        <span>Non-Reportable</span>
        <strong>{fmtPctile(nr.long_history_percentile)}</strong>
        <span>Spread</span>
        <strong>{fmtPctile(sp.percentile)}</strong>
        <span>Comm 4W / 12W pct move</span>
        <strong>
          {v4 != null ? `${Number(v4).toFixed(0)}` : '—'} /{' '}
          {v12 != null ? `${Number(v12).toFixed(0)}` : '—'}
        </strong>
        <span>Analogues</span>
        <strong>{a.independent_case_count ?? '—'}</strong>
        <span>Fwd med 4W / 8W / 12W</span>
        <strong>
          {fmtRet(o4.median_return_pct)} · {fmtRet(o8.median_return_pct)} ·{' '}
          {fmtRet(o12.median_return_pct)}
        </strong>
      </div>
    </div>
  )
}

/** Selected-event research card — historical analogues front and center. */
export function ResearchAnalogueCard({ event, onClose, onJump }) {
  if (!event) return null
  const a = event.analogues || {}
  const o4 = (a.outcomes_by_horizon || {})['4'] || {}
  const o8 = (a.outcomes_by_horizon || {})['8'] || {}
  const o12 = (a.outcomes_by_horizon || {})['12'] || {}
  const c = event.commercial || {}
  const nr = event.nonreportable || {}
  const sp = event.spread || {}
  const n = a.independent_case_count ?? 0

  return (
    <aside className="cot-ws-research-card" aria-label="Historical analogues">
      <header className="cot-ws-research-card-head">
        <div>
          <div className="cot-ws-research-card-kicker">SELECTED EVENT</div>
          <div className="cot-ws-research-card-title">{event.label}</div>
          <div className="cot-ws-research-card-date">{event.date}</div>
        </div>
        <button type="button" className="cot-ws-research-card-close" onClick={onClose}>
          ×
        </button>
      </header>

      <div className="cot-ws-research-card-state">
        Comm {fmtPctile(c.long_history_percentile)} · NR {fmtPctile(nr.long_history_percentile)} ·
        Spread {fmtPctile(sp.percentile)}
      </div>

      <div className="cot-ws-research-card-section-title">HISTORICAL ANALOGUES</div>
      <div className="cot-ws-research-card-cases">
        <strong>{n}</strong> independent cases
      </div>

      <div className="cot-ws-research-horizons">
        {horizonBlock('4W', o4)}
        {horizonBlock('8W', o8)}
        {horizonBlock('12W', o12)}
      </div>

      <div className="cot-ws-research-card-quality">
        Sample quality: <strong>{shortSampleQuality(a.sample_quality)}</strong>
        {a.directional_tendency ? (
          <span className="cot-ws-research-card-tendency"> · {a.directional_tendency}</span>
        ) : null}
      </div>

      {(a.cases || []).length ? (
        <div className="cot-ws-research-card-jumps">
          <div className="cot-ws-research-card-section-title">Jump charts to case</div>
          <div className="cot-ws-analogue-jumps">
            {(a.cases || []).slice(0, 8).map((caze) => (
              <button
                key={caze.date}
                type="button"
                className="cot-ws-event-btn cot-ws-event-btn--ghost"
                onClick={() => onJump?.(caze.date)}
              >
                {caze.date}
              </button>
            ))}
          </div>
        </div>
      ) : null}

      <p className="cot-ws-research-card-note">
        Historical tendency only — not a buy/sell signal.
      </p>
    </aside>
  )
}

export { DEFAULT_LAYER_STATE }
