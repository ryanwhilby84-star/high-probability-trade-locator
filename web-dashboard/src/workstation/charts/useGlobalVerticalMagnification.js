import React from 'react'

import { hitPriceAxis } from './priceAxisHit.js'

/**
 * Per-pane vertical camera control on the right value axis.
 *
 * - Drag a pane's right axis → vertically scale ONLY that pane (`onMagnifyDelta`).
 * - Double-click a pane's right axis → Fit Y for ONLY that pane (`onFitY`).
 *
 * Each callback is scoped by the pane's `panelId`, so one pane's vertical camera
 * never affects another. The shared horizontal camera is untouched.
 */
export function useGlobalVerticalMagnification({
  containerRef,
  enabled,
  onMagnifyDelta,
  onFitY,
}) {
  const onMagnifyDeltaRef = React.useRef(onMagnifyDelta)
  const onFitYRef = React.useRef(onFitY)
  onMagnifyDeltaRef.current = onMagnifyDelta
  onFitYRef.current = onFitY

  React.useEffect(() => {
    const root = containerRef.current
    if (!root || !enabled) return undefined

    let axisDragging = false
    let dragPanelId = null
    let lastY = 0
    let activePointerId = null

    const onResizeHandle = (event) =>
      event.target instanceof Element &&
      Boolean(event.target.closest('.cot-ws-panel-resize-handle'))

    const setAxisCursor = (active) => {
      document.body.classList.toggle('cot-ws-axis-magnify', active)
    }

    const beginAxisDrag = (clientY, panelId, event) => {
      axisDragging = true
      dragPanelId = panelId
      lastY = clientY
      setAxisCursor(true)
      event.preventDefault()
      event.stopPropagation()
    }

    const onPointerDown = (event) => {
      if (event.button !== 0 || onResizeHandle(event)) return
      const hit = hitPriceAxis(event.clientX, event.clientY, root)
      if (!hit) return
      activePointerId = event.pointerId
      beginAxisDrag(event.clientY, hit.panelId, event)
      try {
        root.setPointerCapture(event.pointerId)
      } catch {
        /* ignore */
      }
    }

    const onMouseDown = (event) => {
      if (event.button !== 0 || onResizeHandle(event)) return
      const hit = hitPriceAxis(event.clientX, event.clientY, root)
      if (!hit) return
      beginAxisDrag(event.clientY, hit.panelId, event)
    }

    const onDoubleClick = (event) => {
      if (onResizeHandle(event)) return
      const hit = hitPriceAxis(event.clientX, event.clientY, root)
      if (!hit?.panelId) return
      // Own the reset ourselves so LWC's native double-click reset cannot fight it.
      event.preventDefault()
      event.stopPropagation()
      onFitYRef.current?.(hit.panelId)
    }

    const applyDelta = (clientY, event) => {
      const deltaY = clientY - lastY
      if (deltaY !== 0) {
        onMagnifyDeltaRef.current?.(deltaY, dragPanelId)
        lastY = clientY
      }
      event.preventDefault()
    }

    const onPointerMove = (event) => {
      if (!axisDragging || event.pointerId !== activePointerId) return
      applyDelta(event.clientY, event)
    }

    const onMouseMove = (event) => {
      if (!axisDragging || activePointerId != null) return
      applyDelta(event.clientY, event)
    }

    const endAxisDrag = (event) => {
      if (!axisDragging) return
      if (event?.pointerId != null && activePointerId != null && event.pointerId !== activePointerId) {
        return
      }
      axisDragging = false
      dragPanelId = null
      activePointerId = null
      setAxisCursor(false)
      try {
        if (event?.pointerId != null) root.releasePointerCapture(event.pointerId)
      } catch {
        /* ignore */
      }
    }

    const onHoverMove = (event) => {
      if (axisDragging) return
      const overAxis = Boolean(hitPriceAxis(event.clientX, event.clientY, root))
      root.classList.toggle('cot-ws-panels--axis-hover', overAxis)
    }

    const capture = { capture: true }
    root.addEventListener('pointerdown', onPointerDown, capture)
    root.addEventListener('pointermove', onPointerMove, capture)
    root.addEventListener('pointerup', endAxisDrag, capture)
    root.addEventListener('pointercancel', endAxisDrag, capture)
    root.addEventListener('mousedown', onMouseDown, capture)
    root.addEventListener('dblclick', onDoubleClick, capture)
    window.addEventListener('mousemove', onMouseMove, capture)
    window.addEventListener('mouseup', endAxisDrag, capture)
    root.addEventListener('mousemove', onHoverMove, capture)
    root.addEventListener('mouseleave', () => root.classList.remove('cot-ws-panels--axis-hover'), capture)

    return () => {
      root.removeEventListener('pointerdown', onPointerDown, capture)
      root.removeEventListener('pointermove', onPointerMove, capture)
      root.removeEventListener('pointerup', endAxisDrag, capture)
      root.removeEventListener('pointercancel', endAxisDrag, capture)
      root.removeEventListener('mousedown', onMouseDown, capture)
      root.removeEventListener('dblclick', onDoubleClick, capture)
      window.removeEventListener('mousemove', onMouseMove, capture)
      window.removeEventListener('mouseup', endAxisDrag, capture)
      root.removeEventListener('mousemove', onHoverMove, capture)
      setAxisCursor(false)
      root.classList.remove('cot-ws-panels--axis-hover')
    }
  }, [containerRef, enabled])
}
