import React from 'react'

import { WORKSTATION_DRAWING_TOOLS } from '../canvas/workstationDrawingTypes.js'

const TOOLS = [
  { id: WORKSTATION_DRAWING_TOOLS.SELECT, label: 'Select', title: 'Select drawings' },
  { id: WORKSTATION_DRAWING_TOOLS.VLINE, label: 'Vertical', title: 'Place a vertical line across all panels' },
  { id: WORKSTATION_DRAWING_TOOLS.HLINE, label: 'Horizontal', title: 'Place a horizontal line on this panel' },
]

export function WorkstationDrawingToolbar({
  activeTool,
  onToolChange,
  onDeleteSelected,
  selectedId,
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
      <button
        type="button"
        className="ws-drawing-delete-btn"
        onClick={onDeleteSelected}
        disabled={!selectedId}
        title="Delete selected line (Delete key)"
      >
        Delete selected
      </button>
    </div>
  )
}
