import React from 'react'

import { PANEL_IDS } from '../../charts/chartTheme.js'

const STORAGE_PREFIX = 'hptl-cot-ws-weights'

const DEFAULT_WITH_OHLC = {
  [PANEL_IDS.price]: 3.5,
  [PANEL_IDS.commercial]: 1,
  [PANEL_IDS.institutional]: 1,
  [PANEL_IDS.retail]: 1.05,
}

const DEFAULT_NO_OHLC = {
  [PANEL_IDS.price]: 0.28,
  [PANEL_IDS.commercial]: 1.14,
  [PANEL_IDS.institutional]: 1.14,
  [PANEL_IDS.retail]: 1.14,
}

const PANEL_ORDER = [PANEL_IDS.price, PANEL_IDS.commercial, PANEL_IDS.institutional, PANEL_IDS.retail]

function weightsEqual(a, b) {
  if (a === b) return true
  if (!a || !b) return false
  return PANEL_ORDER.every((id) => a[id] === b[id])
}

function storageKey(marketId, hasOhlc) {
  return `${STORAGE_PREFIX}:${marketId || 'unknown'}:${hasOhlc ? 'ohlc' : 'no-ohlc'}`
}

function clampWeight(value) {
  if (!Number.isFinite(value)) return 1
  return Math.min(4, Math.max(0.2, value))
}

function loadWeights(marketId, hasOhlc) {
  const defaults = hasOhlc ? DEFAULT_WITH_OHLC : DEFAULT_NO_OHLC
  if (!marketId || typeof localStorage === 'undefined') return { ...defaults }
  try {
    const raw = localStorage.getItem(storageKey(marketId, hasOhlc))
    const parsed = raw ? JSON.parse(raw) : null
    if (!parsed || typeof parsed !== 'object') return { ...defaults }
    const out = { ...defaults }
    for (const id of PANEL_ORDER) {
      if (Number.isFinite(parsed[id])) out[id] = clampWeight(parsed[id])
    }
    return out
  } catch {
    return { ...defaults }
  }
}

function persistWeights(marketId, hasOhlc, weights) {
  if (!marketId || typeof localStorage === 'undefined') return
  try {
    localStorage.setItem(storageKey(marketId, hasOhlc), JSON.stringify(weights))
  } catch {
    /* ignore */
  }
}

/** Flex weights for full-screen COT workstation panel sizing. */
export function useCotWorkstationLayout(marketId, hasOhlc, layoutSettled = true) {
  const [weights, setWeights] = React.useState(() => loadWeights(marketId, Boolean(hasOhlc)))

  React.useEffect(() => {
    if (!layoutSettled) return
    const next = loadWeights(marketId, Boolean(hasOhlc))
    setWeights((prev) => (weightsEqual(prev, next) ? prev : next))
  }, [marketId, hasOhlc, layoutSettled])

  const flexForPanel = React.useCallback((panelId) => weights[panelId] ?? 1, [weights])

  const resizeAdjacent = React.useCallback(
    (upperPanelId, lowerPanelId, deltaPx, containerHeight) => {
      if (!containerHeight || containerHeight <= 0) return
      const deltaWeight = (deltaPx / containerHeight) * 4
      setWeights((prev) => {
        const upper = prev[upperPanelId] ?? 1
        const lower = prev[lowerPanelId] ?? 1
        const nextUpper = clampWeight(upper + deltaWeight)
        const nextLower = clampWeight(lower - deltaWeight)
        const merged = {
          ...prev,
          [upperPanelId]: nextUpper,
          [lowerPanelId]: nextLower,
        }
        persistWeights(marketId, Boolean(hasOhlc), merged)
        return merged
      })
    },
    [marketId, hasOhlc],
  )

  return { flexForPanel, resizeAdjacent, panelOrder: PANEL_ORDER }
}
