import React from 'react'

import { hitPriceAxis } from './priceAxisHit.js'

function wheelIntensity(event) {
  let mag = Math.abs(event.deltaY)
  if (mag === 0) return 0
  if (event.deltaMode === 1) mag *= 16
  if (event.deltaMode === 2) mag *= 400
  if (mag < 8) return 1
  if (mag < 60) return 2
  return 3
}

/**
 * Captures wheel + drag on the workstation surface and drives the master camera.
 * Panes are passive — they do not own horizontal navigation.
 */
export function useMasterCameraGestures({
  containerRef,
  enabled,
  onPanDelta,
  onZoomAt,
  onDragStart,
  onDragEnd,
}) {
  const onPanDeltaRef = React.useRef(onPanDelta)
  const onZoomAtRef = React.useRef(onZoomAt)
  const onDragStartRef = React.useRef(onDragStart)
  const onDragEndRef = React.useRef(onDragEnd)
  onPanDeltaRef.current = onPanDelta
  onZoomAtRef.current = onZoomAt
  onDragStartRef.current = onDragStart
  onDragEndRef.current = onDragEnd

  React.useEffect(() => {
    const root = containerRef.current
    if (!root || !enabled) return undefined

    let dragging = false
    let lastX = 0
    let activePointerId = null
    let mouseDragging = false

    const isPlotTarget = (target) => {
      if (!(target instanceof Element)) return false
      return Boolean(target.closest('.cot-ws-panel-body'))
    }

    const isHorizontalPlotTarget = (event) => {
      if (!isPlotTarget(event.target)) return false
      return !hitPriceAxis(event.clientX, event.clientY, root)
    }

    const onWheel = (event) => {
      if (!isHorizontalPlotTarget(event)) return
      event.preventDefault()
      event.stopPropagation()
      const intensity = wheelIntensity(event)
      if (!intensity) return
      const zoomIn = event.deltaY < 0
      onZoomAtRef.current?.(event.clientX, root, zoomIn, { intensity })
    }

    const onPointerDown = (event) => {
      if (event.pointerType === 'mouse') return
      if (event.button !== 0 || !isHorizontalPlotTarget(event)) return
      dragging = true
      activePointerId = event.pointerId
      lastX = event.clientX
      onDragStartRef.current?.()
      try {
        root.setPointerCapture(event.pointerId)
      } catch {
        /* ignore */
      }
      event.preventDefault()
    }

    const onPointerMove = (event) => {
      if (event.pointerType === 'mouse') return
      if (!dragging || event.pointerId !== activePointerId) return
      const deltaX = event.clientX - lastX
      if (deltaX !== 0) {
        onPanDeltaRef.current?.(deltaX)
        lastX = event.clientX
      }
      event.preventDefault()
    }

    const endPointerDrag = (event) => {
      if (event.pointerType === 'mouse') return
      if (!dragging || (event.pointerId != null && event.pointerId !== activePointerId)) return
      dragging = false
      activePointerId = null
      onDragEndRef.current?.()
      try {
        root.releasePointerCapture(event.pointerId)
      } catch {
        /* ignore */
      }
    }

    const onMouseDown = (event) => {
      if (event.button !== 0 || !isHorizontalPlotTarget(event)) return
      mouseDragging = true
      lastX = event.clientX
      onDragStartRef.current?.()
      event.preventDefault()
    }

    const onMouseMove = (event) => {
      if (!mouseDragging) return
      const deltaX = event.clientX - lastX
      if (deltaX !== 0) {
        onPanDeltaRef.current?.(deltaX)
        lastX = event.clientX
      }
      event.preventDefault()
    }

    const endMouseDrag = () => {
      if (!mouseDragging) return
      mouseDragging = false
      onDragEndRef.current?.()
    }

    const capture = { capture: true }
    root.addEventListener('wheel', onWheel, { passive: false, capture: true })
    root.addEventListener('pointerdown', onPointerDown, capture)
    root.addEventListener('pointermove', onPointerMove, capture)
    root.addEventListener('pointerup', endPointerDrag, capture)
    root.addEventListener('pointercancel', endPointerDrag, capture)
    root.addEventListener('mousedown', onMouseDown, capture)
    window.addEventListener('mousemove', onMouseMove, capture)
    window.addEventListener('mouseup', endMouseDrag, capture)

    return () => {
      root.removeEventListener('wheel', onWheel, { capture: true })
      root.removeEventListener('pointerdown', onPointerDown, capture)
      root.removeEventListener('pointermove', onPointerMove, capture)
      root.removeEventListener('pointerup', endPointerDrag, capture)
      root.removeEventListener('pointercancel', endPointerDrag, capture)
      root.removeEventListener('mousedown', onMouseDown, capture)
      window.removeEventListener('mousemove', onMouseMove, capture)
      window.removeEventListener('mouseup', endMouseDrag, capture)
    }
  }, [containerRef, enabled])
}
