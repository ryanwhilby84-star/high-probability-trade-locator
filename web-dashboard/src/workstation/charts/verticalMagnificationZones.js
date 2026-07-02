import { PANEL_IDS } from '../../charts/chartTheme.js'

/** Price has its own vertical magnification; all COT panels share one factor. */
export function verticalMagnificationZone(panelId) {
  if (panelId === PANEL_IDS.price) return 'price'
  if (
    panelId === PANEL_IDS.commercial ||
    panelId === PANEL_IDS.institutional ||
    panelId === PANEL_IDS.retail
  ) {
    return 'cot'
  }
  return 'cot'
}
