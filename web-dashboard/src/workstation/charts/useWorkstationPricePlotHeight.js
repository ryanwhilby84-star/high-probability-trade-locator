import React from 'react'

import {
  WS_PRICE_PLOT_HEIGHT,
  WS_PRICE_PLOT_MIN,
  clampPanelHeight,
} from './workstationPanelSizing.js'

const PRICE_VIEWPORT_SHARE = 0.5
const PRICE_VIEWPORT_MAX = 720
const FULLSCREEN_CHROME_PX = 100
const EMBEDDED_CHROME_PX = 120

function computePricePlotHeight(isFullscreen) {
  if (typeof window === 'undefined') return WS_PRICE_PLOT_HEIGHT
  const chrome = isFullscreen ? FULLSCREEN_CHROME_PX : EMBEDDED_CHROME_PX
  const available = Math.max(window.innerHeight - chrome, 480)
  return clampPanelHeight(
    Math.round(available * PRICE_VIEWPORT_SHARE),
    WS_PRICE_PLOT_MIN,
    PRICE_VIEWPORT_MAX,
  )
}

/** Price hero panel — roughly half the visible workstation viewport. */
export function useWorkstationPricePlotHeight(isFullscreen = true) {
  const [height, setHeight] = React.useState(() => computePricePlotHeight(isFullscreen))

  React.useEffect(() => {
    const refresh = () => setHeight(computePricePlotHeight(isFullscreen))
    refresh()
    window.addEventListener('resize', refresh)
    return () => window.removeEventListener('resize', refresh)
  }, [isFullscreen])

  return height
}
