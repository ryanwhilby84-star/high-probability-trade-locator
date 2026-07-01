import React from 'react'

import { POSITIONING_RANGE_PRESETS } from '../cot/positioningChartMetrics.js'

export function PositioningChartChrome({
  fullSeries,
  rangeId,
  onRangeChange,
  compact = false,
  extraActions = null,
}) {
  const last = fullSeries[fullSeries.length - 1] || null
  const weekCount = fullSeries.length

  return (
    <div className={`pos-chart-chrome${compact ? ' pos-chart-chrome--compact' : ''}`}>
      <div className="pos-chart-chrome-top">
        <div className="pos-range-group" role="group" aria-label="Chart time range">
          {POSITIONING_RANGE_PRESETS.map((p) => (
            <button
              key={p.id}
              type="button"
              className={`pos-range-btn${rangeId === p.id ? ' active' : ''}`}
              onClick={() => onRangeChange(p.id)}
            >
              {p.label}
            </button>
          ))}
          {extraActions}
        </div>
        {compact ? (
          <span className="pos-chart-chrome-meta">
            {weekCount} weeks · latest <strong>{last?.date || last?.label || '—'}</strong>
          </span>
        ) : null}
      </div>
    </div>
  )
}
