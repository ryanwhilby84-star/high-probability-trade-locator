import React from 'react'
import { resolveCalendarCatalysts, formatEventRow } from '../macroCalendarCatalyst.js'
import { resolveCalendarWireStatus } from '../liveFeedStatus.js'

function wireBadgeClass(status) {
  const u = String(status || '').toUpperCase()
  if (u === 'LIVE') return 'lmc-wire-live'
  if (u === 'STALE') return 'lmc-wire-stale'
  if (u === 'LOW CONFIDENCE') return 'lmc-wire-low'
  return 'lmc-wire-off'
}

function SurprisePill({ tone, label }) {
  const cls =
    tone === 'hawkish' || tone === 'bearish_commodity'
      ? 'cal-surprise cal-surprise-hot'
      : tone === 'dovish' || tone === 'bullish_commodity'
        ? 'cal-surprise cal-surprise-cool'
        : tone === 'mixed'
          ? 'cal-surprise cal-surprise-mixed'
          : 'cal-surprise cal-surprise-neutral'
  return <span className={cls}>{label}</span>
}

function ValueCell({ value, tone }) {
  const cls =
    tone === 'hot' ? 'cal-val cal-val-hot' : tone === 'cool' ? 'cal-val cal-val-cool' : 'cal-val'
  return <span className={cls}>{value}</span>
}

function EventTable({ title, events, emptyMsg }) {
  if (!events?.length) {
    return (
      <div className="mcat-block">
        <h4 className="mcat-subtitle">{title}</h4>
        <p className="mcat-empty">{emptyMsg}</p>
      </div>
    )
  }
  return (
    <div className="mcat-block">
      <h4 className="mcat-subtitle">{title}</h4>
      <table className="mcat-table mcat-table-calendar">
        <thead>
          <tr>
            <th>Time</th>
            <th>Event</th>
            <th>Country / FX</th>
            <th>Impact</th>
            <th>Actual</th>
            <th>Forecast</th>
            <th>Previous</th>
            <th>Surprise</th>
            <th>Markets</th>
            <th>Likely interpretation</th>
          </tr>
        </thead>
        <tbody>
          {events.map((ev, i) => {
            const r = formatEventRow(ev)
            return (
              <tr key={`${r.name}-${i}`}>
                <td className="mcat-mono">{r.when}</td>
                <td>
                  <div className="mcat-event-name">{r.name}</div>
                  <div className="mcat-meta">{r.source}</div>
                </td>
                <td>
                  {r.country}
                  {r.currency && r.currency !== '—' ? (
                    <span className="mcat-meta">{r.currency}</span>
                  ) : null}
                </td>
                <td>{r.impact}</td>
                <td>
                  <ValueCell value={r.actual} tone={r.actualTone} />
                </td>
                <td>
                  <ValueCell value={r.forecast} tone="neutral" />
                </td>
                <td>
                  <ValueCell value={r.previous} tone="neutral" />
                </td>
                <td>
                  <SurprisePill tone={r.surpriseTone} label={r.surprise} />
                </td>
                <td className="mcat-markets">{r.markets}</td>
                <td className="mcat-why">{r.interpretation}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

export function MacroCatalystPanel({ row, globalCalendar }) {
  const cal = React.useMemo(() => resolveCalendarCatalysts(row, globalCalendar), [row, globalCalendar])
  const wire = React.useMemo(() => resolveCalendarWireStatus(row, globalCalendar), [row, globalCalendar])

  if (!cal.wired) {
    return (
      <section className="mcat-section" aria-label="Red-folder events">
        <h3 className="mcat-title">
          Red-Folder Events <span className={`mcat-wire-badge ${wireBadgeClass(wire.status)}`}>{wire.status}</span>
        </h3>
        <p className="mcat-not-wired">{wire.detail || cal.message || 'NOT WIRED'}</p>
      </section>
    )
  }

  return (
    <section className="mcat-section" aria-label="Red-folder events">
      <h3 className="mcat-title">
        Red-Folder Events <span className={`mcat-wire-badge ${wireBadgeClass(wire.status)}`}>{wire.status}</span>
      </h3>
      {cal.provider ? <p className="mcat-meta-line">Provider: {cal.provider}</p> : null}
      {wire.status === 'LOW CONFIDENCE' ? <p className="mcat-meta-line">{wire.detail}</p> : null}
      <EventTable
        title="Upcoming high-impact"
        events={cal.upcoming_high_impact}
        emptyMsg="No high-impact releases scheduled in the current window."
      />
      <EventTable
        title="Latest released"
        events={cal.latest_released}
        emptyMsg="No recent releases with actuals in the current window."
      />
    </section>
  )
}
