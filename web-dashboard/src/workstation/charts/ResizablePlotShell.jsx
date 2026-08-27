import React from 'react'

import {
  WS_COT_PLOT_HEIGHT,
  WS_COT_PLOT_MAX,
  WS_COT_PLOT_MIN,
} from './workstationPanelSizing.js'

/**
 * Plot wrapper with a bottom drag handle.
 * Pointer updates are animation-frame throttled so resizing stays smooth and the
 * chart does not get hammered with React/localStorage writes on every raw event.
 */
export function ResizablePlotShell({
  panelId,
  height,
  minHeight = WS_COT_PLOT_MIN,
  maxHeight = WS_COT_PLOT_MAX,
  onResize,
  children,
}) {
  const dragRef = React.useRef(null)
  const rafRef = React.useRef(null)
  const pendingHeightRef = React.useRef(null)

  const flushPending = React.useCallback(() => {
    rafRef.current = null
    const next = pendingHeightRef.current
    pendingHeightRef.current = null
    if (Number.isFinite(next)) onResize?.(panelId, next)
  }, [onResize, panelId])

  const scheduleResize = React.useCallback(
    (next) => {
      pendingHeightRef.current = Math.min(maxHeight, Math.max(minHeight, next))
      if (rafRef.current == null) rafRef.current = requestAnimationFrame(flushPending)
    },
    [flushPending, maxHeight, minHeight],
  )

  const endDrag = React.useCallback(() => {
    if (rafRef.current != null) {
      cancelAnimationFrame(rafRef.current)
      rafRef.current = null
    }
    const next = pendingHeightRef.current
    pendingHeightRef.current = null
    if (Number.isFinite(next)) onResize?.(panelId, next)
    dragRef.current = null
    document.body.classList.remove('ws-panel-resizing')
  }, [onResize, panelId])

  React.useEffect(() => {
    const onMove = (e) => {
      const drag = dragRef.current
      if (!drag) return
      scheduleResize(drag.startHeight + (e.clientY - drag.startY))
    }
    const onUp = () => endDrag()

    document.addEventListener('pointermove', onMove)
    document.addEventListener('pointerup', onUp)
    document.addEventListener('pointercancel', onUp)
    return () => {
      document.removeEventListener('pointermove', onMove)
      document.removeEventListener('pointerup', onUp)
      document.removeEventListener('pointercancel', onUp)
      if (rafRef.current != null) cancelAnimationFrame(rafRef.current)
    }
  }, [endDrag, scheduleResize])

  const onHandleDown = (e) => {
    if (e.button !== 0) return
    e.preventDefault()
    e.stopPropagation()
    dragRef.current = { startY: e.clientY, startHeight: height }
    document.body.classList.add('ws-panel-resizing')
  }

  const onHandleDoubleClick = (e) => {
    e.preventDefault()
    e.stopPropagation()
    onResize?.(panelId, WS_COT_PLOT_HEIGHT)
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
        title="Drag to resize · double-click to reset"
        onPointerDown={onHandleDown}
        onDoubleClick={onHandleDoubleClick}
      />
    </div>
  )
}
