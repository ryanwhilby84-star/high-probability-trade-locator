import React from 'react'

/** Drag handle between stacked workstation panels — adjusts flex weights (height only). */
export function CotPanelResizeHandle({ onDragDelta }) {
  const dragRef = React.useRef(null)

  React.useEffect(() => {
    const onMove = (e) => {
      const drag = dragRef.current
      if (!drag) return
      const delta = e.clientY - drag.lastY
      if (delta !== 0) {
        onDragDelta?.(delta, drag.containerHeight)
        drag.lastY = e.clientY
      }
    }
    const onUp = () => {
      dragRef.current = null
      document.body.classList.remove('cot-ws-panel-resizing')
    }

    document.addEventListener('pointermove', onMove)
    document.addEventListener('pointerup', onUp)
    document.addEventListener('pointercancel', onUp)
    return () => {
      document.removeEventListener('pointermove', onMove)
      document.removeEventListener('pointerup', onUp)
      document.removeEventListener('pointercancel', onUp)
    }
  }, [onDragDelta])

  const onPointerDown = (e) => {
    if (e.button !== 0) return
    e.preventDefault()
    e.stopPropagation()
    const container = e.currentTarget.closest('.cot-ws-panels')
    dragRef.current = {
      lastY: e.clientY,
      containerHeight: container?.clientHeight ?? 600,
    }
    document.body.classList.add('cot-ws-panel-resizing')
  }

  return (
    <button
      type="button"
      className="cot-ws-panel-resize-handle"
      aria-label="Resize panels"
      onPointerDown={onPointerDown}
    />
  )
}
