import React from 'react'
import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceArea,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import { VerticalCrosshair, rangeMinMax } from '../charts/ChartPanel.jsx'
import { CHART_WS } from '../charts/chartTheme.js'
import { HPTL_LINE_TYPE } from '../charts/hptlLine.js'
import { fmtDelta, fmtValue, seriesMetrics } from '../cot/positioningChartMetrics.js'
import { WorkstationDrawingLayer } from '../workstation/canvas/WorkstationDrawingLayer.jsx'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function ExtremeZones({ extremes, color, domainMin, domainMax }) {
  if (!extremes || !isNum(extremes.high) || !isNum(extremes.low)) return null
  const zones = []
  if (isNum(domainMax) && extremes.high < domainMax) {
    zones.push(
      <ReferenceArea key="high" y1={extremes.high} y2={domainMax} fill={color} fillOpacity={0.05} strokeOpacity={0} />,
    )
  }
  if (isNum(domainMin) && extremes.low > domainMin) {
    zones.push(
      <ReferenceArea key="low" y1={domainMin} y2={extremes.low} fill={color} fillOpacity={0.05} strokeOpacity={0} />,
    )
  }
  return zones
}

function LatestDot({ cx, cy, color, isLast }) {
  if (!isLast || cx == null || cy == null) return null
  return (
    <g className="pos-latest-dot">
      <circle cx={cx} cy={cy} r={10} fill={color} fillOpacity={0.22} />
      <circle cx={cx} cy={cy} r={5} fill={color} stroke="#f8fafc" strokeWidth={1.5} />
    </g>
  )
}

function PositioningTooltip({ active, payload, label, dataKey, color, yFormatter }) {
  if (!active || !payload?.length) return null
  const row = payload[0]?.payload
  const v = row?.[dataKey]
  return (
    <div className="pos-chart-tooltip">
      <div className="pos-chart-tooltip-date">{label}</div>
      <div className="pos-chart-tooltip-val" style={{ color }}>
        {yFormatter(v)}
      </div>
      {isNum(row?.institutional_wow) && dataKey === 'institutional_net' ? (
        <div className="pos-chart-tooltip-sub">WoW {fmtDelta(row.institutional_wow)}</div>
      ) : null}
      {isNum(row?.commercial_wow) && dataKey === 'commercial_net' ? (
        <div className="pos-chart-tooltip-sub">WoW {fmtDelta(row.commercial_wow)}</div>
      ) : null}
      {isNum(row?.retail_wow) && dataKey === 'retail_net' ? (
        <div className="pos-chart-tooltip-sub">WoW {fmtDelta(row.retail_wow)}</div>
      ) : null}
    </div>
  )
}

function SeriesBadge({ shortLabel, metrics, color }) {
  const deltaClass = (v) => {
    if (!Number.isFinite(v)) return ''
    if (v > 0) return ' pos-delta--up'
    if (v < 0) return ' pos-delta--down'
    return ''
  }
  return (
    <aside className="pos-series-badge" style={{ '--series-color': color }}>
      <div className="pos-series-badge-label">{shortLabel}</div>
      <div className="pos-series-badge-value">{fmtValue(metrics.value)}</div>
      <div className="pos-series-badge-deltas">
        <span className={deltaClass(metrics.wow)}>WoW {fmtDelta(metrics.wow)}</span>
        <span className={deltaClass(metrics.w4)}>4W {fmtDelta(metrics.w4)}</span>
        <span className={deltaClass(metrics.w13)}>13W {fmtDelta(metrics.w13)}</span>
      </div>
    </aside>
  )
}

export function PositioningChartPanel({
  panelId,
  title,
  shortLabel,
  dataKey,
  wowKey,
  color,
  data,
  fullSeries,
  yFormatter,
  height = 200,
  showXAxis = false,
  connectNulls = true,
  extremes = null,
  showZeroLine = false,
  panelWarning = null,
  onPoint,
  onClear,
  syncId,
  activeLabel = null,
  badgeMetrics = null,
}) {
  const series = Array.isArray(data) ? data : []
  const hasData = series.some((d) => isNum(d[dataKey]))
  const { min, max } = rangeMinMax(series, dataKey)
  const pad = isNum(min) && isNum(max) ? (max - min) * 0.1 || 1 : 0
  const axisDomain = isNum(min) && isNum(max) ? [min - pad, max + pad] : ['auto', 'auto']
  const lastIndex = series.length - 1

  const computedBadge = React.useMemo(
    () => badgeMetrics || seriesMetrics(fullSeries || series, dataKey, wowKey),
    [badgeMetrics, fullSeries, series, dataKey, wowKey],
  )

  const tooltipContent = React.useCallback(
    (props) => (
      <PositioningTooltip {...props} dataKey={dataKey} color={color} yFormatter={yFormatter} />
    ),
    [dataKey, color, yFormatter],
  )

  return (
    <div className="pos-chart-panel pos-chart-panel--canvas" data-panel={panelId}>
      <div className="pos-chart-panel-head">
        <span className="pos-chart-panel-title">{title}</span>
      </div>
      {!hasData ? (
        <p className="pos-chart-panel-empty">{panelWarning || 'No data in selected range.'}</p>
      ) : (
        <div className="pos-chart-panel-body pos-chart-panel-body--canvas">
          <WorkstationDrawingLayer panelId={panelId}>
            <div className="pos-chart-panel-plot pos-chart-panel-plot--interactive">
              {panelWarning ? <p className="pos-chart-panel-warn">{panelWarning}</p> : null}
              <ResponsiveContainer width="100%" height={height}>
                <LineChart
                  data={series}
                  syncId={syncId}
                  margin={{ top: 10, right: 12, left: 0, bottom: showXAxis ? 22 : 8 }}
                  onMouseMove={(state) => {
                    const p = state?.activePayload?.[0]?.payload
                    if (p) onPoint?.(p)
                  }}
                  onMouseLeave={onClear}
                >
                  <CartesianGrid strokeDasharray="2 6" stroke={CHART_WS.grid} vertical={false} horizontal />
                  <XAxis
                    dataKey="label"
                    hide={!showXAxis}
                    tick={{ fontSize: 10, fill: CHART_WS.axisMuted, fontFamily: CHART_WS.fontFamily }}
                    interval="preserveStartEnd"
                    minTickGap={48}
                    axisLine={{ stroke: CHART_WS.border }}
                    tickLine={false}
                  />
                  <YAxis
                    tick={{ fontSize: 10, fill: CHART_WS.axisMuted, fontFamily: CHART_WS.fontFamily }}
                    width={72}
                    tickFormatter={yFormatter}
                    domain={axisDomain}
                    axisLine={false}
                    tickLine={false}
                  />
                  <Tooltip cursor={<VerticalCrosshair />} content={tooltipContent} isAnimationActive={false} />
                  {activeLabel ? (
                    <ReferenceLine
                      x={activeLabel}
                      stroke={CHART_WS.crosshair}
                      strokeWidth={1.5}
                      strokeDasharray="4 4"
                      ifOverflow="extendDomain"
                    />
                  ) : null}
                  {extremes ? (
                    <ExtremeZones
                      extremes={extremes}
                      color={color}
                      domainMin={isNum(min) ? min - pad : null}
                      domainMax={isNum(max) ? max + pad : null}
                    />
                  ) : null}
                  {showZeroLine ? <ReferenceLine y={0} stroke={CHART_WS.zero} strokeWidth={1} strokeDasharray="4 4" /> : null}
                  <Line
                    type={HPTL_LINE_TYPE}
                    dataKey={dataKey}
                    stroke={color}
                    strokeWidth={1.75}
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    dot={(props) => (
                      <LatestDot
                        cx={props.cx}
                        cy={props.cy}
                        color={color}
                        isLast={props.index === lastIndex}
                      />
                    )}
                    activeDot={{ r: 4, fill: color, stroke: '#f8fafc', strokeWidth: 1 }}
                    connectNulls={connectNulls}
                    isAnimationActive={false}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </WorkstationDrawingLayer>
          <SeriesBadge shortLabel={shortLabel} metrics={computedBadge} color={color} />
        </div>
      )}
    </div>
  )
}
