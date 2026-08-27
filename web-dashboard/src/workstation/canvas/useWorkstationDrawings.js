import React from 'react'

import {
  createWorkstationDrawingId,
  workstationDrawingStorageKey,
} from './workstationDrawingTypes.js'

/**
 * Per-market drawing store with localStorage persistence.
 * Drawing tools are one-shot by default: place a line/shape, then return to Select.
 */
export function useWorkstationDrawings(marketId) {
  const [drawings, setDrawings] = React.useState([])
  const [activeTool, setActiveTool] = React.useState('select')
  const [selectedId, setSelectedId] = React.useState(null)

  React.useEffect(() => {
    if (!marketId || typeof localStorage === 'undefined') {
      setDrawings([])
      return
    }
    try {
      const raw = localStorage.getItem(workstationDrawingStorageKey(marketId))
      const parsed = raw ? JSON.parse(raw) : []
      setDrawings(Array.isArray(parsed) ? parsed : [])
    } catch {
      setDrawings([])
    }
    setSelectedId(null)
    setActiveTool('select')
  }, [marketId])

  React.useEffect(() => {
    const onKey = (event) => {
      if (event.key !== 'Escape') return
      const tag = String(event.target?.tagName || '').toLowerCase()
      if (tag === 'input' || tag === 'textarea' || event.target?.isContentEditable) return
      setActiveTool('select')
      setSelectedId(null)
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [])

  const persist = React.useCallback(
    (next) => {
      setDrawings((prev) => {
        const resolved = typeof next === 'function' ? next(prev) : next
        const list = Array.isArray(resolved) ? resolved : []
        if (marketId && typeof localStorage !== 'undefined') {
          try {
            localStorage.setItem(workstationDrawingStorageKey(marketId), JSON.stringify(list))
          } catch {
            /* ignore quota */
          }
        }
        return list
      })
    },
    [marketId],
  )

  const addDrawing = React.useCallback(
    (partial) => {
      const drawing = {
        id: createWorkstationDrawingId(),
        createdAt: Date.now(),
        ...partial,
      }
      persist((prev) => [...(prev || []), drawing])
      setSelectedId(drawing.id)
      setActiveTool('select')
      return drawing
    },
    [persist],
  )

  const updateDrawing = React.useCallback(
    (id, patch) => {
      persist((prev) => (prev || []).map((d) => (d.id === id ? { ...d, ...patch } : d)))
    },
    [persist],
  )

  const removeDrawing = React.useCallback(
    (id) => {
      if (!id) return
      persist((prev) => (prev || []).filter((d) => d.id !== id))
      setSelectedId((cur) => (cur === id ? null : cur))
    },
    [persist],
  )

  const clearDrawings = React.useCallback(() => {
    persist([])
    setSelectedId(null)
    setActiveTool('select')
  }, [persist])

  const deleteSelected = React.useCallback(() => {
    if (!selectedId) return
    removeDrawing(selectedId)
  }, [removeDrawing, selectedId])

  return {
    drawings,
    activeTool,
    setActiveTool,
    selectedId,
    setSelectedId,
    addDrawing,
    updateDrawing,
    removeDrawing,
    deleteSelected,
    clearDrawings,
    ready: true,
  }
}
