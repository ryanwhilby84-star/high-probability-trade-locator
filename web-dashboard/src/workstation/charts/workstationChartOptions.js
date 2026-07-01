import { CrosshairMode } from 'lightweight-charts'

import { CHART_WS } from '../../charts/chartTheme.js'

export const WS_CHART_COLORS = {
  background: CHART_WS.canvas,
  text: CHART_WS.axis,
  grid: CHART_WS.grid,
  border: CHART_WS.border,
  up: '#22c55e',
  down: '#ef4444',
  wickUp: '#22c55e',
  wickDown: '#ef4444',
  crosshair: CHART_WS.crosshair,
  drawing: CHART_WS.drawing,
  drawingSelected: CHART_WS.drawingSelected,
}

/** Identical time-scale + layout options — every pane must match for pixel alignment. */
export function createWorkstationChartOptions({ width, height, showTimeAxis = false, interactionEnabled = true }) {
  return {
    width: Math.max(width, 1),
    height,
    layout: {
      background: { color: WS_CHART_COLORS.background },
      textColor: WS_CHART_COLORS.text,
      fontFamily: CHART_WS.fontFamily,
      fontSize: CHART_WS.axisFontSize,
    },
    grid: {
      vertLines: { color: WS_CHART_COLORS.grid },
      horzLines: { color: WS_CHART_COLORS.grid },
    },
    rightPriceScale: {
      borderColor: WS_CHART_COLORS.border,
      scaleMargins: { top: 0.08, bottom: 0.06 },
    },
    timeScale: {
      borderColor: WS_CHART_COLORS.border,
      visible: showTimeAxis,
      timeVisible: showTimeAxis,
      secondsVisible: false,
      rightOffset: 4,
      barSpacing: 7,
      minBarSpacing: 1.5,
      fixLeftEdge: false,
      fixRightEdge: false,
    },
    crosshair: {
      mode: CrosshairMode.Normal,
      vertLine: {
        color: WS_CHART_COLORS.crosshair,
        width: 1,
        style: 2,
        labelVisible: showTimeAxis,
        labelBackgroundColor: '#1e293b',
      },
      horzLine: {
        color: WS_CHART_COLORS.crosshair,
        width: 1,
        style: 2,
        labelBackgroundColor: '#1e293b',
      },
    },
    handleScroll: {
      mouseWheel: interactionEnabled,
      pressedMouseMove: interactionEnabled,
      horzTouchDrag: interactionEnabled,
      vertTouchDrag: false,
    },
    handleScale: {
      mouseWheel: interactionEnabled,
      pinch: interactionEnabled,
      axisPressedMouseMove: { time: false, price: true },
    },
    kineticScroll: { touch: interactionEnabled, mouse: interactionEnabled },
  }
}
