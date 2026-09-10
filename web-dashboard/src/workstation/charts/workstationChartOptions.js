import { CrosshairMode } from 'lightweight-charts'

import { CHART_WS, PANEL_IDS } from '../../charts/chartTheme.js'

export const WS_CHART_COLORS = {
  background: '#fbfcfa',
  text: '#4f5d58',
  grid: 'rgba(38, 58, 51, 0.065)',
  gridMajor: 'rgba(38, 58, 51, 0.10)',
  border: '#dfe6e2',
  up: '#17a36b',
  down: '#dc6464',
  upWick: '#17a36b',
  downWick: '#dc6464',
  crosshair: 'rgba(55, 73, 67, 0.34)',
  crosshairLabel: '#f5f8f7',
  drawing: CHART_WS.drawing,
  drawingSelected: CHART_WS.drawingSelected,
}

export const WS_PRICE_SCALE_WIDTH = 66
const EMPTY_PRICE_FORMATTER = () => ''

export function formatWorkstationAxisPrice(value) {
  const n = Number(value)
  if (!Number.isFinite(n)) return ''
  const abs = Math.abs(n)
  const sign = n < 0 ? '-' : ''
  if (abs >= 1_000_000) return `${sign}${(abs / 1_000_000).toFixed(1)}M`
  if (abs >= 10_000) return `${sign}${(abs / 1_000).toFixed(0)}k`
  if (abs >= 1_000) return `${sign}${(abs / 1_000).toFixed(1)}k`
  if (abs >= 100) return `${sign}${abs.toFixed(0)}`
  if (abs >= 10) return `${sign}${abs.toFixed(1)}`
  return `${sign}${abs.toFixed(2)}`
}

export function formatExactLivePrice(value, precision = null) {
  const n = Number(value)
  if (!Number.isFinite(n)) return ''
  const digits = precision != null && Number.isFinite(Number(precision))
    ? Math.max(0, Math.min(8, Number(precision)))
    : Math.abs(n) >= 1000 ? 3 : Math.abs(n) >= 100 ? 2 : Math.abs(n) >= 1 ? 3 : 4
  return n.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits, useGrouping: true })
}

/**
 * Research panes should use their own vertical range aggressively. The prior
 * 18–20% top/bottom margins made small COT series look like a flat line inside
 * a mostly empty box. Keep enough breathing room for labels without throwing
 * away a third of the panel.
 */
export function scaleMarginsForPanel(panelId, showTimeAxis = false) {
  if (panelId === PANEL_IDS.price) return { top: 0.06, bottom: 0.07 }
  if (showTimeAxis) return { top: 0.09, bottom: 0.11 }
  return { top: 0.08, bottom: 0.09 }
}

export function createWorkstationChartOptions({ width, height, showTimeAxis = false, panelId = null, interactionEnabled = true, passiveCamera = false, hidePriceScale = false, reservePriceScaleGutter = false, compact = false } = {}) {
  const horizontalNav = interactionEnabled && !passiveCamera
  const fontSize = compact ? 11 : CHART_WS.axisFontSize
  const gutterOnly = hidePriceScale && reservePriceScaleGutter

  // Every real pane gets its own date axis. This is deliberate: each COT panel
  // must be independently readable as a historical chart, not just as a strip
  // that relies on the bottom pane for date context.
  const independentTimeAxis = !gutterOnly

  return {
    width: Math.max(width, 1),
    height,
    layout: {
      background: { color: WS_CHART_COLORS.background },
      textColor: WS_CHART_COLORS.text,
      fontFamily: CHART_WS.fontFamily,
      fontSize,
    },
    grid: {
      vertLines: { color: WS_CHART_COLORS.grid, visible: !gutterOnly },
      horzLines: { color: WS_CHART_COLORS.grid, visible: !gutterOnly },
    },
    rightPriceScale: {
      visible: !hidePriceScale || gutterOnly,
      borderVisible: false,
      minimumWidth: hidePriceScale && !gutterOnly ? 0 : WS_PRICE_SCALE_WIDTH,
      scaleMargins: scaleMarginsForPanel(panelId, independentTimeAxis),
      entireTextOnly: true,
      ticksVisible: !gutterOnly,
      autoScale: true,
    },
    leftPriceScale: { visible: false },
    timeScale: {
      borderVisible: true,
      borderColor: WS_CHART_COLORS.border,
      visible: independentTimeAxis,
      timeVisible: false,
      secondsVisible: false,
      rightOffset: 1.5,
      barSpacing: compact ? 7.5 : 8.5,
      minBarSpacing: 0.8,
      fixLeftEdge: false,
      fixRightEdge: false,
    },
    crosshair: {
      mode: CrosshairMode.Magnet,
      vertLine: {
        color: WS_CHART_COLORS.crosshair,
        width: 1,
        style: 0,
        visible: true,
        labelVisible: independentTimeAxis,
        labelBackgroundColor: '#68746f',
      },
      horzLine: {
        visible: !hidePriceScale && !gutterOnly,
        color: WS_CHART_COLORS.crosshair,
        width: 1,
        style: 2,
        labelVisible: !hidePriceScale && !gutterOnly,
        labelBackgroundColor: '#68746f',
      },
    },
    handleScroll: {
      mouseWheel: false,
      pressedMouseMove: horizontalNav,
      horzTouchDrag: horizontalNav,
      vertTouchDrag: false,
    },
    handleScale: {
      mouseWheel: horizontalNav,
      pinch: horizontalNav,
      axisPressedMouseMove: { time: horizontalNav, price: false },
      axisDoubleClickReset: { time: false, price: false },
    },
    kineticScroll: { touch: horizontalNav, mouse: false },
    localization: gutterOnly
      ? { priceFormatter: EMPTY_PRICE_FORMATTER }
      : { priceFormatter: formatWorkstationAxisPrice },
  }
}

export function createCotWorkstationChartOptions(opts) {
  return createWorkstationChartOptions({ ...opts, compact: true })
}

export const EXCLUDE_FROM_AUTOSCALE = () => null
