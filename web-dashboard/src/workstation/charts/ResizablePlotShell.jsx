import React from 'react'

import { WS_COT_PLOT_MAX, WS_COT_PLOT_MIN } from './workstationPanelSizing.js'

/**
 * Plot wrapper with a bottom drag handle — grows the workstation vertically on resize.
 */
export function ResizablePlotShell({ panelId, height, minHeight = WS_COT_PLOT_MIN, maxHeight = WS_COT_PLOT_MAX, onResize, children }) {
  const dragRef = React.useRef(null)

  const endDrag = React.useCallback(() => {
    dragRef.current = null
    document.body.classList.remove('ws-panel-resizing')
  }, [])

  React.useEffect(() => {
    const onMove = (e) => {
      const drag = dragRef.current
      if (!drag) return
      const next = drag.startHeight + (e.clientY - drag.startY)
      onResize?.(panelId, next)
    }
    const onUp = () => endDrag()

    document.addEventListener('pointermove', onMove)
    document.addEventListener('pointerup', onUp)
    document.addEventListener('pointercancel', onUp)
    return () => {
      document.removeEventListener('pointermove', onMove)
      document.removeEventListener('pointerup', onUp)
      document.removeEventListener('pointercancel', onUp)
    }
  }, [endDrag, onResize, panelId])

  const onHandleDown = (e) => {
    if (e.button !== 0) return
    e.preventDefault()
    e.stopPropagation()
    dragRef.current = { startY: e.clientY, startHeight: height }
    document.body.classList.add('ws-panel-resizing')
  }

  return (
    <div
      className="pos-chart-panel-plot pos-chart-panel-plot--synced pos-chart-panel-plot--cot"
      style={{
        height,
        minHeight,
        maxHeight,
      }}
      data-panel-plot={panelId}
    >
      <div className="ws-plot-canvas-host">{children}</div>
      <button
        type="button"
        className="ws-panel-resize-handle"
        aria-label="Resize panel height"
        onPointerDown={onHandleDown}
      />
    </div>
  )
}
