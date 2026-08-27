import React from 'react'

import { useWorkstationDrawings } from '../canvas/useWorkstationDrawings.js'
import { labelFromCrosshairTime } from '../canvas/sliceVisibleWindow.js'
import { barTimeToDate } from '../../charts/positioningTimelineAlign.js'

const WorkstationCanvasContext = React.createContext(null)

export function WorkstationCanvasProvider({ marketId, seriesForLabels = [], children }) {
  const [crosshairTime, setCrosshairTime] = React.useState(null)
  const [crosshairLabel, setCrosshairLabel] = React.useState(null)
  const [visibleTimeRange, setVisibleTimeRange] = React.useState(null)
  const [isUserZoomed, setIsUserZoomed] = React.useState(false)
  const drawings = useWorkstationDrawings(marketId)

  const clearCrosshair = React.useCallback(() => {
    setCrosshairTime(null)
    setCrosshairLabel(null)
  }, [])

  const setCrosshairFromTime = React.useCallback(
    (time, labelHint = null) => {
      if (!time) {
        clearCrosshair()
        return
      }
      setCrosshairTime(time)
      const label =
        labelHint ||
        labelFromCrosshairTime(seriesForLabels, time) ||
        barTimeToDate(time)
      setCrosshairLabel(label)
    },
    [clearCrosshair, seriesForLabels],
  )

  const setCrosshairFromCotRow = React.useCallback(
    (row, barTime = null) => {
      if (!row) {
        clearCrosshair()
        return
      }
      const label = row.label || row.date || null
      setCrosshairLabel(label)
      if (barTime != null) setCrosshairTime(barTime)
    },
    [clearCrosshair],
  )

  const resetVisibleRange = React.useCallback(() => {
    setVisibleTimeRange(null)
    setIsUserZoomed(false)
  }, [])

  const applyVisibleTimeRange = React.useCallback((range) => {
    if (!range?.from || !range?.to) return
    setVisibleTimeRange(range)
    setIsUserZoomed(true)
  }, [])

  const value = React.useMemo(
    () => ({
      marketId,
      crosshairTime,
      crosshairLabel,
      visibleTimeRange,
      isUserZoomed,
      setCrosshairTime: setCrosshairFromTime,
      setCrosshairFromCotRow,
      clearCrosshair,
      setVisibleTimeRange: applyVisibleTimeRange,
      resetVisibleRange,
      drawings,
    }),
    [
      marketId,
      crosshairTime,
      crosshairLabel,
      visibleTimeRange,
      isUserZoomed,
      setCrosshairFromTime,
      setCrosshairFromCotRow,
      clearCrosshair,
      applyVisibleTimeRange,
      resetVisibleRange,
      drawings,
    ],
  )

  return <WorkstationCanvasContext.Provider value={value}>{children}</WorkstationCanvasContext.Provider>
}

export function useWorkstationCanvas() {
  const ctx = React.useContext(WorkstationCanvasContext)
  if (!ctx) {
    throw new Error('useWorkstationCanvas must be used within WorkstationCanvasProvider')
  }
  return ctx
}

export function useWorkstationCanvasOptional() {
  return React.useContext(WorkstationCanvasContext)
}
