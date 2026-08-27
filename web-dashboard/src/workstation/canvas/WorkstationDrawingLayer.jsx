import React from 'react'

/**
 * Placeholder for unified drawing overlay (vline/hline/rect).
 * Renderers will attach per chart engine (lightweight-charts + Recharts).
 */
export function WorkstationDrawingLayer({ marketId, panelId, drawings, activeTool, children }) {
  void marketId
  void panelId
  void drawings
  void activeTool
  return <>{children}</>
}
