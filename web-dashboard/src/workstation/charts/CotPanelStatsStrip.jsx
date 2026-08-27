import React from 'react'

import { formatCotStatValue } from './cotPanelStats.js'

function StatItem({ label, value, signed = false }) {
  const text = formatCotStatValue(value, { signed })
  const tone =
    signed && typeof value === 'number' && Number.isFinite(value)
      ? value > 0
        ? 'up'
        : value < 0
          ? 'down'
          : 'flat'
      : 'neutral'

  return (
    <span className={`cot-ws-stat-item cot-ws-stat-item--${tone}`}>
      <span className="cot-ws-stat-item-label">{label}</span>
      <span className="cot-ws-stat-item-value">{text}</span>
    </span>
  )
}

/** Bloomberg-style positioning strip beneath a COT panel. */
export function CotPanelStatsStrip({ label, stats }) {
  if (!stats) return null

  return (
    <div className="cot-ws-panel-stats" aria-label={`${label} positioning statistics`}>
      <span className="cot-ws-panel-stats-group">{label}</span>
      <StatItem label="Current" value={stats.current} />
      <StatItem label="1W" value={stats.w1} signed />
      <StatItem label="4W" value={stats.w4} signed />
      <StatItem label="12W" value={stats.w12} signed />
    </div>
  )
}
