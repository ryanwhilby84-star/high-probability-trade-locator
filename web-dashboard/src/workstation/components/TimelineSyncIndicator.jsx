import { useWeeklyTimelineOptional } from '../context/WeeklyTimelineContext.jsx'

/**
 * Shows the week currently under crosshair on the master candlestick chart.
 * Downstream panels (COT, seasonality) will read the same context.
 */
export function TimelineSyncIndicator({ fallbackDate = null }) {
  const timeline = useWeeklyTimelineOptional()
  const date = timeline?.activeWeekDate || fallbackDate

  if (!timeline) {
    return fallbackDate ? (
      <div className="irw-sync-chip irw-sync-chip--muted">
        Timeline sync pending · week {fallbackDate}
      </div>
    ) : null
  }

  return (
    <div className="irw-sync-chip" title="Synchronized to positioning workspace weekly timeline">
      <span className="irw-sync-chip-label">Timeline</span>
      <span className="irw-sync-chip-value">{date || '—'}</span>
      {timeline.hasValuationOverlay ? (
        <span className="irw-sync-chip-meta">
          {timeline.activeRow?.deviation_pct != null
            ? ` · dev ${timeline.activeRow.deviation_pct >= 0 ? '+' : ''}${Number(timeline.activeRow.deviation_pct).toFixed(1)}%`
            : ''}
        </span>
      ) : null}
    </div>
  )
}
