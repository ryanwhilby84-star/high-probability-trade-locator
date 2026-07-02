import React from 'react'

/**
 * Drawing layer scaffold — coordinates live in data space (week/date + panel value).
 * Renders nothing until drawing tools are enabled; subscribes to viewport geometry only.
 */
export function CotDrawingCoordinator({ subscribeGeometry, panelsRef, getViewportState }) {
  React.useEffect(() => {
    if (!subscribeGeometry) return undefined
    return subscribeGeometry(() => {
      // Future: repaint global vlines + panel hlines/rects from getViewportState()
      void panelsRef
      void getViewportState
    })
  }, [subscribeGeometry, panelsRef, getViewportState])

  return null
}
