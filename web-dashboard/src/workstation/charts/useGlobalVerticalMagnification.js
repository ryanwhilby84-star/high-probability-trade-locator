import React from 'react'

import { hitPriceAxis } from './priceAxisHit.js'

/**
 * TradingView-style global vertical magnification — drag any panel's right value axis.
 * One shared factor; horizontal camera is untouched.
 */
export function useGlobalVerticalMagnification({
  containerRef,
  enabled,
  onMagnifyDelta,
}) {
  const onMagnifyDeltaRef = React.useRef(onMagnifyDelta)
  onMagnifyDeltaRef.current = onMagnifyDelta

  React.useEffect(() => {
    const root = containerRef.current
    if (!root || !enabled) return undefined

    let axisDragging = false
    let dragZone = 'cot'
    let lastY = 0
    let activePointerId = null

    const setAxisCursor = (active) => {
      document.body.classList.toggle('cot-ws-axis-magnify', active)
    }

    const beginAxisDrag = (clientY, zone, event) => {
      axisDragging = true
      dragZone = zone
      lastY = clientY
      setAxisCursor(true)
      event.preventDefault()
      event.stopPropagation()
    }

    const onPointerDown = (event) => {
      if (event.button !== 0) return
      const hit = hitPriceAxis(event.clientX, event.clientY, root)
      if (!hit) return
      activePointerId = event.pointerId
      beginAxisDrag(event.clientY, hit.zone, event)
      try {
        root.setPointerCapture(event.pointerId)
      } catch {
        /* ignore */
      }
    }

    const onMouseDown = (event) => {
      if (event.button !== 0) return
      const hit = hitPriceAxis(event.clientX, event.clientY, root)
      if (!hit) return
      beginAxisDrag(event.clientY, hit.zone, event)
    }

    const applyDelta = (clientY, event) => {
      const deltaY = clientY - lastY
      if (deltaY !== 0) {
        onMagnifyDeltaRef.current?.(deltaY, dragZone)
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
      window.removeEventListener('mousemove', onMouseMove, capture)
      window.removeEventListener('mouseup', endAxisDrag, capture)
      root.removeEventListener('mousemove', onHoverMove, capture)
      setAxisCursor(false)
      root.classList.remove('cot-ws-panels--axis-hover')
    }
  }, [containerRef, enabled])
}
