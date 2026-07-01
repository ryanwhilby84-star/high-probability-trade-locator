import React from 'react'

import { WORKSTATION_DRAWING_TOOLS } from '../canvas/workstationDrawingTypes.js'

const TOOLS = [
  { id: WORKSTATION_DRAWING_TOOLS.SELECT, label: 'Select', title: 'Select drawings' },
  { id: WORKSTATION_DRAWING_TOOLS.VLINE, label: 'V-line', title: 'Vertical line (all panels)' },
  { id: WORKSTATION_DRAWING_TOOLS.HLINE, label: 'H-line', title: 'Horizontal line (this panel)' },
  { id: WORKSTATION_DRAWING_TOOLS.RECT, label: 'Rect', title: 'Rectangle (this panel)' },
]

export function WorkstationDrawingToolbar({
  activeTool,
  onToolChange,
  onClear,
  drawingCount = 0,
}) {
  return (
    <div className="ws-drawing-toolbar" role="toolbar" aria-label="Chart drawings">
      <div className="ws-drawing-toolbar-tools">
        {TOOLS.map((t) => (
          <button
            key={t.id}
            type="button"
            className={`ws-drawing-tool-btn${activeTool === t.id ? ' is-active' : ''}`}
            title={t.title}
            aria-pressed={activeTool === t.id}
            onClick={() => onToolChange?.(t.id)}
          >
            {t.label}
          </button>
        ))}
      </div>
      {drawingCount > 0 ? (
        <button type="button" className="ws-drawing-clear-btn" onClick={onClear}>
          Clear ({drawingCount})
        </button>
      ) : null}
    </div>
  )
}
