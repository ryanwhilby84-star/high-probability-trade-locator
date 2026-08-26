import { CrosshairMode } from 'lightweight-charts'

import { CHART_WS, PANEL_IDS } from '../../charts/chartTheme.js'

export const WS_CHART_COLORS = {
  background: '#ffffff',
  text: '#53615d',
  grid: 'rgba(30, 55, 47, 0.055)',
  gridMajor: 'rgba(30, 55, 47, 0.085)',
  border: '#dfe6e3',
  up: '#12a56b',
  down: '#df5a5a',
  upWick: '#12a56b',
  downWick: '#df5a5a',
  crosshair: 'rgba(65, 84, 78, 0.34)',
  crosshairLabel: '#f5f8f7',
  drawing: CHART_WS.drawing,
  drawingSelected: CHART_WS.drawingSelected,
}

export const WS_PRICE_SCALE_WIDTH = 56
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

export function scaleMarginsForPanel(panelId, showTimeAxis = false) {
  if (panelId === PANEL_IDS.price) return { top: 0.1, bottom: 0.1 }
  if (showTimeAxis) return { top: 0.2, bottom: 0.2 }
  return { top: 0.18, bottom: 0.18 }
}

export function createWorkstationChartOptions({ width, height, showTimeAxis = false, panelId = null, interactionEnabled = true, passiveCamera = false, hidePriceScale = false, reservePriceScaleGutter = false, compact = false } = {}) {
  const horizontalNav = interactionEnabled && !passiveCamera
  const fontSize = compact ? 11 : CHART_WS.axisFontSize
  const gutterOnly = hidePriceScale && reservePriceScaleGutter

  return {
    width: Math.max(width, 1), height,
    layout: { background: { color: WS_CHART_COLORS.background }, textColor: WS_CHART_COLORS.text, fontFamily: CHART_WS.fontFamily, fontSize },
    grid: { vertLines: { color: WS_CHART_COLORS.grid, visible: true }, horzLines: { color: WS_CHART_COLORS.grid, visible: !gutterOnly } },
    rightPriceScale: { visible: !hidePriceScale || gutterOnly, borderVisible: false, minimumWidth: hidePriceScale && !gutterOnly ? 0 : WS_PRICE_SCALE_WIDTH, scaleMargins: { top: 0.12, bottom: showTimeAxis ? 0.14 : 0.1 }, entireTextOnly: true, ticksVisible: !gutterOnly },
    leftPriceScale: { visible: false },
    timeScale: { borderVisible: false, visible: showTimeAxis, timeVisible: showTimeAxis, secondsVisible: false, rightOffset: 3, barSpacing: compact ? 6 : 7, minBarSpacing: 0.35, fixLeftEdge: false, fixRightEdge: false },
    crosshair: {
      mode: CrosshairMode.Magnet,
      vertLine: { color: WS_CHART_COLORS.crosshair, width: 1, style: 0, visible: true, labelVisible: showTimeAxis, labelBackgroundColor: '#53615d' },
      horzLine: { visible: !hidePriceScale && !gutterOnly, color: WS_CHART_COLORS.crosshair, width: 1, style: 2, labelVisible: !hidePriceScale && !gutterOnly, labelBackgroundColor: '#53615d' },
    },
    handleScroll: { mouseWheel: false, pressedMouseMove: horizontalNav, horzTouchDrag: horizontalNav, vertTouchDrag: false },
    handleScale: { mouseWheel: horizontalNav, pinch: horizontalNav, axisPressedMouseMove: { time: horizontalNav, price: false }, axisDoubleClickReset: { time: false, price: false } },
    kineticScroll: { touch: horizontalNav, mouse: false },
    localization: gutterOnly ? { priceFormatter: EMPTY_PRICE_FORMATTER } : { priceFormatter: formatWorkstationAxisPrice },
  }
}

export function createCotWorkstationChartOptions(opts) { return createWorkstationChartOptions({ ...opts, compact: true }) }
export const EXCLUDE_FROM_AUTOSCALE = () => null
