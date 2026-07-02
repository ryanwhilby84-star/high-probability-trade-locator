import { CrosshairMode } from 'lightweight-charts'

import { CHART_WS } from '../../charts/chartTheme.js'

export const WS_CHART_COLORS = {
  background: CHART_WS.canvas,
  text: '#64748b',
  grid: 'rgba(148, 163, 184, 0.06)',
  gridMajor: 'rgba(148, 163, 184, 0.1)',
  border: CHART_WS.border,
  up: '#34d399',
  down: '#f87171',
  upWick: '#34d399',
  downWick: '#f87171',
  crosshair: 'rgba(148, 163, 184, 0.55)',
  crosshairLabel: '#0f172a',
  drawing: CHART_WS.drawing,
  drawingSelected: CHART_WS.drawingSelected,
}

/** Shared price-scale width — keeps plot areas pixel-aligned across panes. */
export const WS_PRICE_SCALE_WIDTH = 56

const EMPTY_PRICE_FORMATTER = () => ''

/** Compact axis labels so every pane keeps the same 56px price-scale gutter. */
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

/** Identical time-scale + layout options — every pane must match for pixel alignment. */
export function createWorkstationChartOptions({
  width,
  height,
  showTimeAxis = false,
  interactionEnabled = true,
  passiveCamera = false,
  hidePriceScale = false,
  reservePriceScaleGutter = false,
  compact = false,
} = {}) {
  const horizontalNav = interactionEnabled && !passiveCamera
  const fontSize = compact ? 9 : CHART_WS.axisFontSize
  const gutterOnly = hidePriceScale && reservePriceScaleGutter

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
      vertLines: { color: WS_CHART_COLORS.grid, visible: true },
      horzLines: { color: WS_CHART_COLORS.grid, visible: !gutterOnly },
    },
    rightPriceScale: {
      visible: !hidePriceScale || gutterOnly,
      borderVisible: false,
      minimumWidth: hidePriceScale && !gutterOnly ? 0 : WS_PRICE_SCALE_WIDTH,
      scaleMargins: { top: 0.12, bottom: showTimeAxis ? 0.14 : 0.1 },
      entireTextOnly: true,
      ticksVisible: !gutterOnly,
    },
    leftPriceScale: {
      visible: false,
    },
    timeScale: {
      borderVisible: false,
      visible: showTimeAxis,
      timeVisible: showTimeAxis,
      secondsVisible: false,
      rightOffset: 8,
      barSpacing: compact ? 6 : 7,
      minBarSpacing: 0.35,
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
        labelVisible: showTimeAxis,
        labelBackgroundColor: '#334155',
      },
      horzLine: {
        visible: !hidePriceScale && !gutterOnly,
        color: WS_CHART_COLORS.crosshair,
        width: 1,
        style: 2,
        labelVisible: !hidePriceScale && !gutterOnly,
        labelBackgroundColor: '#334155',
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
    kineticScroll: {
      touch: horizontalNav,
      mouse: false,
    },
    localization: gutterOnly
      ? { priceFormatter: EMPTY_PRICE_FORMATTER }
      : { priceFormatter: formatWorkstationAxisPrice },
  }
}

/** COT workstation — compact axes; passive panes use master camera for horizontal nav. */
export function createCotWorkstationChartOptions(opts) {
  return createWorkstationChartOptions({ ...opts, compact: true })
}
