/** Per-instrument chart drawings — localStorage persistence. */

export const DRAWING_TOOLS = {
  SELECT: 'select',
  BOX: 'box',
  HLINE: 'hline',
  VLINE: 'vline',
  TEXT: 'text',
}

/** @typedef {'box'|'hline'|'vline'|'text'} DrawingType */

/**
 * @typedef {object} ChartDrawing
 * @property {string} id
 * @property {DrawingType} type
 * @property {string} [panelId] — panel scope; vline omits (all date-synced panels)
 * @property {string} [date] — ISO date for vline / text anchor
 * @property {string} [dateStart]
 * @property {string} [dateEnd]
 * @property {number} [value]
 * @property {number} [valueTop]
 * @property {number} [valueBottom]
 * @property {string} [text]
 */

const STORAGE_PREFIX = 'hptl-ws-drawings:'

export function drawingStorageKey(instrumentId) {
  return `${STORAGE_PREFIX}${instrumentId || 'unknown'}`
}

/** @returns {ChartDrawing[]} */
export function loadDrawings(instrumentId) {
  if (!instrumentId || typeof localStorage === 'undefined') return []
  try {
    const raw = localStorage.getItem(drawingStorageKey(instrumentId))
    const parsed = raw ? JSON.parse(raw) : []
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

/** @param {ChartDrawing[]} drawings */
export function saveDrawings(instrumentId, drawings) {
  if (!instrumentId || typeof localStorage === 'undefined') return
  localStorage.setItem(drawingStorageKey(instrumentId), JSON.stringify(drawings))
}

export function createDrawingId() {
  return `d_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 7)}`
}

/** Drawings visible on a panel (vlines apply everywhere). */
export function drawingsForPanel(drawings, panelId) {
  return (drawings || []).filter((d) => {
    if (d.type === 'vline') return true
    return d.panelId === panelId
  })
}

export function nearestLabelFromX(labels, xScale, pointerX, offsetLeft = 0) {
  if (!labels?.length || !xScale) return null
  let best = labels[0]
  let bestDist = Infinity
  for (const label of labels) {
    const px = xScale(label)
    if (px == null || Number.isNaN(px)) continue
    const dist = Math.abs(px + offsetLeft - pointerX)
    if (dist < bestDist) {
      bestDist = dist
      best = label
    }
  }
  return best
}

export function valueFromY(yScale, pointerY, offsetTop = 0) {
  if (!yScale?.invert) return null
  try {
    const v = yScale.invert(pointerY - offsetTop)
    return Number.isFinite(v) ? v : null
  } catch {
    return null
  }
}
