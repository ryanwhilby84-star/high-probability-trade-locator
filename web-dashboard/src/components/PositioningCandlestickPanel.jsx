import React from 'react'

import { PANEL_IDS } from '../charts/chartTheme.js'
import { fmtPrice } from '../priceData.js'
import { WeeklyCandlestickChart } from '../workstation/charts/WeeklyCandlestickChart.jsx'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function fmtDelta(v) {
  if (!isNum(v)) return '—'
  const sign = v > 0 ? '+' : ''
  return `${sign}${v.toFixed(2)}`
}

function CandleBadge({ candle, weekLabel }) {
  if (!candle) {
    return (
      <aside className="pos-series-badge pos-series-badge--candle">
        <div className="pos-series-badge-label">OHLC</div>
        <div className="pos-series-badge-value">—</div>
      </aside>
    )
  }
  const chg = isNum(candle.open) && isNum(candle.close) ? candle.close - candle.open : null
  const deltaClass = chg > 0 ? ' pos-delta--up' : chg < 0 ? ' pos-delta--down' : ''
  return (
    <aside className="pos-series-badge pos-series-badge--candle">
      {weekLabel ? <div className="pos-series-badge-week">{weekLabel}</div> : null}
      <div className="pos-series-badge-label">Close</div>
      <div className="pos-series-badge-value">{fmtPrice(candle.close, 2)}</div>
      <div className="pos-series-badge-deltas">
        <span>O {fmtPrice(candle.open, 2)}</span>
        <span>H {fmtPrice(candle.high, 2)}</span>
        <span>L {fmtPrice(candle.low, 2)}</span>
        <span className={deltaClass}>Δ {fmtDelta(chg)}</span>
      </div>
    </aside>
  )
}

export function PositioningCandlestickPanel({
  visibleBars,
  height = 520,
  crosshairTime = null,
  onCrosshairMove,
  onClear,
  chartRef,
  panelWarning = null,
  weekLabel = null,
}) {
  const [hoverCandle, setHoverCandle] = React.useState(null)

  const handleCrosshair = React.useCallback(
    (payload) => {
      if (!payload?.time) {
        setHoverCandle(null)
        onClear?.()
        return
      }
      setHoverCandle(payload.candle || null)
      onCrosshairMove?.(payload)
    },
    [onCrosshairMove, onClear],
  )

  if (!visibleBars?.length) {
    return (
      <div className="pos-chart-panel pos-chart-panel--candles" data-panel={PANEL_IDS.price}>
        <div className="pos-chart-panel-head">
          <span className="pos-chart-panel-title">Weekly price</span>
        </div>
        <p className="pos-chart-panel-empty">{panelWarning || 'Weekly OHLC unavailable.'}</p>
      </div>
    )
  }

  const lastBar = visibleBars[visibleBars.length - 1]
  const displayCandle = hoverCandle || {
    open: lastBar.open,
    high: lastBar.high,
    low: lastBar.low,
    close: lastBar.close,
  }

  return (
    <div className="pos-chart-panel pos-chart-panel--candles" data-panel={PANEL_IDS.price}>
      <div className="pos-chart-panel-head">
        <span className="pos-chart-panel-title">Weekly price · OHLC</span>
      </div>
      {panelWarning ? <p className="pos-chart-panel-warn">{panelWarning}</p> : null}
      <div className="pos-chart-panel-body pos-chart-panel-body--canvas">
        <div className="pos-chart-panel-plot pos-chart-panel-plot--candles">
          <WeeklyCandlestickChart
            chartRef={chartRef}
            weeklyBars={visibleBars}
            height={height}
            onCrosshairMove={handleCrosshair}
            externalCrosshairTime={crosshairTime}
            lockTimeScale
            autoFit
            className="pos-candle-chart"
          />
        </div>
        <CandleBadge candle={displayCandle} weekLabel={weekLabel} />
      </div>
    </div>
  )
}
