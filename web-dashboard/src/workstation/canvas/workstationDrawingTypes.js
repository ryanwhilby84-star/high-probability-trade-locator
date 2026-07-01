import { CHART_WS } from '../../charts/chartTheme.js'

/**
 * Workstation drawing model — architecture for future annotation layer.
 * Visualization-only; does not affect COT, valuation, or price calculations.
 */

/** @typedef {'select'|'vline'|'hline'|'rect'|'delete'} WorkstationDrawingTool */

/**
 * @typedef {object} WorkstationDrawing
 * @property {string} id
 * @property {'vline'|'hline'|'rect'} type
 * @property {string} [panelId] — hline/rect scope; vline spans all panels
 * @property {string} [date] — ISO week for vline anchor
 * @property {string} [dateStart]
 * @property {string} [dateEnd]
 * @property {number} [value] — hline price/COT level
 * @property {number} [valueTop]
 * @property {number} [valueBottom]
 * @property {number} [time] — unix bar time (candle panel)
 * @property {string} [color] — vline stroke colour
 * @property {number} [width] — vline stroke width (px)
 * @property {string} [label] — future label text
 * @property {number} [createdAt]
 */

/** Default style for new global timeline vertical markers. */
export const DEFAULT_VLINE_STYLE = {
  color: CHART_WS.drawing,
  width: 1.5,
}

export const WORKSTATION_DRAWING_TOOLS = {
  SELECT: 'select',
  VLINE: 'vline',
  HLINE: 'hline',
  RECT: 'rect',
  DELETE: 'delete',
}

export const WORKSTATION_DRAWING_STORAGE_PREFIX = 'hptl-workstation-drawings:'

export function workstationDrawingStorageKey(marketId) {
  return `${WORKSTATION_DRAWING_STORAGE_PREFIX}${marketId || 'unknown'}`
}

export function createWorkstationDrawingId() {
  return `wsd_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 7)}`
}

/** Global timeline vertical markers (rendered once on the shared overlay). */
export function globalVlineDrawings(drawings) {
  return (drawings || []).filter((d) => d.type === 'vline')
}

/** Per-panel drawings — excludes global vlines (handled by shared timeline overlay). */
export function drawingsForWorkstationPanel(drawings, panelId) {
  return (drawings || []).filter((d) => d.type !== 'vline' && d.panelId === panelId)
}
