import React from 'react'

import { DRAWING_TOOLS } from './chartDrawings.js'

const TOOL_BUTTONS = [
  { id: DRAWING_TOOLS.SELECT, label: 'Select', title: 'Select drawings' },
  { id: DRAWING_TOOLS.BOX, label: 'Box', title: 'Rectangle zone' },
  { id: DRAWING_TOOLS.HLINE, label: 'H-Line', title: 'Horizontal line' },
  { id: DRAWING_TOOLS.VLINE, label: 'V-Line', title: 'Vertical line (all panels)' },
  { id: DRAWING_TOOLS.TEXT, label: 'Text', title: 'Text note' },
]

export function ChartDrawingToolbar({
  tool,
  onToolChange,
  onDeleteSelected,
  onClearAll,
  selectedId,
  drawingCount = 0,
}) {
  return (
    <div className="chart-ws-draw-toolbar" role="toolbar" aria-label="Drawing tools">
      <span className="chart-ws-draw-label">Draw</span>
      {TOOL_BUTTONS.map((btn) => (
        <button
          key={btn.id}
          type="button"
          className={`chart-ws-draw-btn${tool === btn.id ? ' active' : ''}`}
          title={btn.title}
          aria-pressed={tool === btn.id}
          onClick={() => onToolChange(btn.id)}
        >
          {btn.label}
        </button>
      ))}
      <span className="chart-ws-draw-sep" aria-hidden="true" />
      <button
        type="button"
        className="chart-ws-draw-btn chart-ws-draw-btn--danger"
        disabled={!selectedId}
        onClick={onDeleteSelected}
        title="Delete selected drawing"
      >
        Delete
      </button>
      <button
        type="button"
        className="chart-ws-draw-btn chart-ws-draw-btn--danger"
        disabled={!drawingCount}
        onClick={onClearAll}
        title="Clear all drawings for this instrument"
      >
        Clear
      </button>
    </div>
  )
}
