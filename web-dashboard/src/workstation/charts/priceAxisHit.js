import { WS_PRICE_SCALE_WIDTH } from './workstationChartOptions.js'
import { verticalMagnificationZone } from './verticalMagnificationZones.js'

/** Pointer over a panel's right-hand value axis — returns panel id + magnification zone. */
export function hitPriceAxis(clientX, clientY, root) {
  if (!root) return null
  const bodies = root.querySelectorAll('.cot-ws-panel-body')
  for (const body of bodies) {
    const rect = body.getBoundingClientRect()
    if (clientY < rect.top || clientY > rect.bottom) continue
    const axisLeft = rect.right - WS_PRICE_SCALE_WIDTH
    if (clientX >= axisLeft && clientX <= rect.right) {
      const panelEl = body.closest('[data-panel]')
      const panelId = panelEl?.getAttribute('data-panel') ?? null
      return {
        body,
        rect,
        panelId,
        zone: verticalMagnificationZone(panelId),
      }
    }
  }
  return null
}
