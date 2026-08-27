import React from 'react'

import { PANEL_IDS } from '../../charts/chartTheme.js'
import {
  clampPanelHeight,
  defaultCotPanelHeights,
  WS_COT_PLOT_MAX,
  WS_COT_PLOT_MIN,
} from './workstationPanelSizing.js'

const COT_PANEL_IDS = [PANEL_IDS.commercial, PANEL_IDS.institutional, PANEL_IDS.retail]

function storageKey(marketId) {
  return `hptl-ws-cot-heights:${marketId || 'unknown'}`
}

function loadHeights(marketId) {
  if (!marketId || typeof localStorage === 'undefined') return defaultCotPanelHeights()
  try {
    const raw = localStorage.getItem(storageKey(marketId))
    const parsed = raw ? JSON.parse(raw) : null
    if (!parsed || typeof parsed !== 'object') return defaultCotPanelHeights()
    return {
      commercial: clampPanelHeight(parsed.commercial, WS_COT_PLOT_MIN, WS_COT_PLOT_MAX),
      institutional: clampPanelHeight(parsed.institutional, WS_COT_PLOT_MIN, WS_COT_PLOT_MAX),
      retail: clampPanelHeight(parsed.retail, WS_COT_PLOT_MIN, WS_COT_PLOT_MAX),
    }
  } catch {
    return defaultCotPanelHeights()
  }
}

export function useCotPanelHeights(marketId) {
  const [heights, setHeights] = React.useState(() => loadHeights(marketId))

  React.useEffect(() => {
    setHeights(loadHeights(marketId))
  }, [marketId])

  const setPanelHeight = React.useCallback(
    (panelId, nextHeight) => {
      if (!COT_PANEL_IDS.includes(panelId)) return
      setHeights((prev) => {
        const key =
          panelId === PANEL_IDS.commercial
            ? 'commercial'
            : panelId === PANEL_IDS.institutional
              ? 'institutional'
              : 'retail'
        const merged = {
          ...prev,
          [key]: clampPanelHeight(nextHeight, WS_COT_PLOT_MIN, WS_COT_PLOT_MAX),
        }
        if (marketId && typeof localStorage !== 'undefined') {
          try {
            localStorage.setItem(storageKey(marketId), JSON.stringify(merged))
          } catch {
            /* ignore quota */
          }
        }
        return merged
      })
    },
    [marketId],
  )

  const heightForPanel = React.useCallback(
    (panelId) => {
      if (panelId === PANEL_IDS.commercial) return heights.commercial
      if (panelId === PANEL_IDS.institutional) return heights.institutional
      if (panelId === PANEL_IDS.retail) return heights.retail
      return WS_COT_PLOT_MIN
    },
    [heights],
  )

  return { heightForPanel, setPanelHeight }
}
