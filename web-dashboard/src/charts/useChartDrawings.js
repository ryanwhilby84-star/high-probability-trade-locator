import React from 'react'

import {
  createDrawingId,
  DRAWING_TOOLS,
  loadDrawings,
  saveDrawings,
} from './chartDrawings.js'

export function useChartDrawings(instrumentId) {
  const [tool, setTool] = React.useState(DRAWING_TOOLS.SELECT)
  const [drawings, setDrawings] = React.useState(() => loadDrawings(instrumentId))
  const [selectedId, setSelectedId] = React.useState(null)
  const [draft, setDraft] = React.useState(null)
  const dragRef = React.useRef(null)
  const interactionRefs = React.useRef({})

  React.useEffect(() => {
    setDrawings(loadDrawings(instrumentId))
    setSelectedId(null)
    setDraft(null)
    dragRef.current = null
  }, [instrumentId])

  const persist = React.useCallback(
    (next) => {
      setDrawings(next)
      saveDrawings(instrumentId, next)
    },
    [instrumentId],
  )

  const getRef = React.useCallback((panelId) => {
    if (!interactionRefs.current[panelId]) {
      interactionRefs.current[panelId] = { current: null }
    }
    return interactionRefs.current[panelId]
  }, [])

  const pointerContext = React.useCallback((e, panelId) => {
    const ref = interactionRefs.current[panelId]?.current
    if (!ref?.pointerToDataFromPlot) return null
    const rect = e.currentTarget.getBoundingClientRect()
    const plotX = e.clientX - rect.left
    const plotY = e.clientY - rect.top
    const { label, value } = ref.pointerToDataFromPlot(plotX, plotY)
    return {
      panelId,
      label,
      value,
      pointerX: plotX + ref.offset.left,
      pointerY: plotY + ref.offset.top,
      xForLabel: ref.xForLabel,
      yForValue: ref.yForValue,
      offset: ref.offset,
    }
  }, [])

  const onDrawPointerDown = React.useCallback(
    (e, panelId) => {
      if (e.button !== 0) return
      const ctx = pointerContext(e, panelId)
      if (!ctx?.label) return

      if (tool === DRAWING_TOOLS.SELECT) return

      if (tool === DRAWING_TOOLS.TEXT) {
        const text = window.prompt('Note text')
        if (!text?.trim()) return
        const next = [
          ...drawings,
          {
            id: createDrawingId(),
            type: 'text',
            panelId,
            date: ctx.label,
            value: ctx.value,
            text: text.trim(),
          },
        ]
        persist(next)
        setSelectedId(next[next.length - 1].id)
        return
      }

      if (tool === DRAWING_TOOLS.VLINE) {
        dragRef.current = {
          type: 'vline',
          panelId,
          date: ctx.label,
        }
        setDraft({ type: 'vline', date: ctx.label })
        return
      }

      if (tool === DRAWING_TOOLS.HLINE) {
        dragRef.current = {
          type: 'hline',
          panelId,
          value: ctx.value,
        }
        setDraft({ type: 'hline', panelId, value: ctx.value })
        return
      }

      if (tool === DRAWING_TOOLS.BOX) {
        dragRef.current = {
          type: 'box',
          panelId,
          dateStart: ctx.label,
          dateEnd: ctx.label,
          valueTop: ctx.value,
          valueBottom: ctx.value,
        }
        setDraft({
          type: 'box',
          panelId,
          dateStart: ctx.label,
          dateEnd: ctx.label,
          valueTop: ctx.value,
          valueBottom: ctx.value,
        })
      }
    },
    [tool, drawings, persist, pointerContext],
  )

  const onDrawPointerMove = React.useCallback(
    (e, panelId) => {
      const drag = dragRef.current
      if (!drag) return
      const ctx = pointerContext(e, panelId)
      if (!ctx?.label) return

      if (drag.type === 'box' && drag.panelId === panelId) {
        setDraft({
          type: 'box',
          panelId,
          dateStart: drag.dateStart,
          dateEnd: ctx.label,
          valueTop: drag.valueTop,
          valueBottom: ctx.value,
        })
      }
    },
    [pointerContext],
  )

  const onDrawPointerUp = React.useCallback(
    (e, panelId) => {
      const drag = dragRef.current
      if (!drag) return
      dragRef.current = null

      const ctx = pointerContext(e, panelId)
      if (!ctx) {
        setDraft(null)
        return
      }

      let nextDrawing = null
      if (drag.type === 'vline') {
        nextDrawing = { id: createDrawingId(), type: 'vline', date: drag.date || ctx.label }
      } else if (drag.type === 'hline' && typeof drag.value === 'number') {
        nextDrawing = { id: createDrawingId(), type: 'hline', panelId: drag.panelId, value: drag.value }
      } else if (drag.type === 'box' && drag.panelId === panelId) {
        nextDrawing = {
          id: createDrawingId(),
          type: 'box',
          panelId,
          dateStart: drag.dateStart,
          dateEnd: ctx.label,
          valueTop: drag.valueTop,
          valueBottom: ctx.value,
        }
      }

      setDraft(null)
      if (!nextDrawing) return

      const next = [...drawings, nextDrawing]
      persist(next)
      setSelectedId(nextDrawing.id)
    },
    [drawings, persist, pointerContext],
  )

  const deleteSelected = React.useCallback(() => {
    if (!selectedId) return
    const next = drawings.filter((d) => d.id !== selectedId)
    persist(next)
    setSelectedId(null)
  }, [selectedId, drawings, persist])

  const selectDrawing = React.useCallback((id) => {
    setSelectedId(id || null)
  }, [])

  const clearAll = React.useCallback(() => {
    if (!drawings.length) return
    if (!window.confirm('Clear all drawings for this instrument?')) return
    persist([])
    setSelectedId(null)
  }, [drawings.length, persist])

  const overlayActive = tool !== DRAWING_TOOLS.SELECT

  return {
    tool,
    setTool,
    drawings,
    selectedId,
    draft,
    getRef,
    onDrawPointerDown,
    onDrawPointerMove,
    onDrawPointerUp,
    deleteSelected,
    clearAll,
    overlayActive,
    selectDrawing,
  }
}
