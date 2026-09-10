import React from 'react'
import {
  CartesianGrid,
  Customized,
  Line,
  LineChart,
  ReferenceArea,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import { HPTL_LINE_TYPE } from './hptlLine.js'
import { CHART_WS } from './chartTheme.js'
import {
  drawingsForPanel,
  nearestLabelFromX,
  valueFromY,
} from './chartDrawings.js'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function valuesFor(data, key) {
  return data.map((d) => d[key]).filter(isNum)
}

function rangeMinMax(data, key) {
  const vals = valuesFor(data, key)
  if (!vals.length) return { min: null, max: null }
  return { min: Math.min(...vals), max: Math.max(...vals) }
}

function quantile(sorted, q) {
  if (!sorted.length) return null
  if (sorted.length === 1) return sorted[0]
  const pos = (sorted.length - 1) * q
  const base = Math.floor(pos)
  const rest = pos - base
  const next = sorted[base + 1]
  return next == null ? sorted[base] : sorted[base] + rest * (next - sorted[base])
}

/**
 * Fit the visible history, but do not let one broken / stale COT print flatten
 * the other 99% of a panel. We only trim points when the full range is wildly
 * larger than the IQR-based core range. Normal market extremes remain visible.
 */
function qualityDomain(data, key, explicitDomain = null) {
  if (Array.isArray(explicitDomain) && explicitDomain.length === 2) {
    return { domain: explicitDomain, clipped: 0, min: explicitDomain[0], max: explicitDomain[1] }
  }

  const vals = valuesFor(data, key).sort((a, b) => a - b)
  if (!vals.length) return { domain: ['auto', 'auto'], clipped: 0, min: null, max: null }

  const rawMin = vals[0]
  const rawMax = vals[vals.length - 1]
  let fitMin = rawMin
  let fitMax = rawMax
  let clipped = 0

  if (vals.length >= 20) {
    const q1 = quantile(vals, 0.25)
    const q3 = quantile(vals, 0.75)
    const iqr = q3 - q1
    if (isNum(iqr) && iqr > 0) {
      const lowerFence = q1 - 4 * iqr
      const upperFence = q3 + 4 * iqr
      const core = vals.filter((v) => v >= lowerFence && v <= upperFence)
      if (core.length >= Math.max(12, Math.floor(vals.length * 0.9))) {
        const coreMin = core[0]
        const coreMax = core[core.length - 1]
        const rawSpan = rawMax - rawMin
        const coreSpan = coreMax - coreMin
        // Only intervene when an outlier is doing genuine visual damage.
        if (coreSpan > 0 && rawSpan > coreSpan * 2.75) {
          fitMin = coreMin
          fitMax = coreMax
          clipped = vals.length - core.length
        }
      }
    }
  }

  // The latest observation must always remain visible even when an old bad
  // print is excluded from auto-fit.
  const latest = [...data].reverse().find((d) => isNum(d[key]))?.[key]
  if (isNum(latest)) {
    fitMin = Math.min(fitMin, latest)
    fitMax = Math.max(fitMax, latest)
  }

  const span = fitMax - fitMin
  const magnitude = Math.max(Math.abs(fitMin), Math.abs(fitMax), 1)
  const pad = span > 0 ? span * 0.11 : magnitude * 0.08
  return {
    domain: [fitMin - pad, fitMax + pad],
    clipped,
    min: fitMin,
    max: fitMax,
  }
}

function VerticalCrosshair(props) {
  const { points, height, top = 0, bottom = 0 } = props
  if (!points?.length) return null
  const x = points[0].x
  return (
    <line
      x1={x}
      x2={x}
      y1={top}
      y2={height - bottom}
      stroke={CHART_WS.crosshair}
      strokeWidth={1}
      strokeDasharray="4 4"
    />
  )
}

function ExtremeZones({ extremes, color, domainMin, domainMax }) {
  if (!extremes || !isNum(extremes.high) || !isNum(extremes.low)) return null
  const zones = []
  if (isNum(domainMax) && extremes.high < domainMax) {
    zones.push(
      <ReferenceArea
        key="high"
        y1={extremes.high}
        y2={domainMax}
        fill={color}
        fillOpacity={0.055}
        strokeOpacity={0}
        ifOverflow="hidden"
      />,
    )
  }
  if (isNum(domainMin) && extremes.low > domainMin) {
    zones.push(
      <ReferenceArea
        key="low"
        y1={domainMin}
        y2={extremes.low}
        fill={color}
        fillOpacity={0.055}
        strokeOpacity={0}
        ifOverflow="hidden"
      />,
    )
  }
  return zones
}

function sortDatePair(a, b) {
  return a <= b ? [a, b] : [b, a]
}

function sortValuePair(a, b) {
  return a <= b ? [a, b] : [b, a]
}

function renderDrawingShape(d, ctx, { selectedId, selectMode = false, onSelect }) {
  const { xForLabel, yForValue, offset } = ctx
  const stroke = d.id === selectedId ? CHART_WS.drawingSelected : CHART_WS.drawing
  const sw = d.id === selectedId ? 2 : 1.25
  const pickProps = selectMode
    ? {
        style: { cursor: 'pointer' },
        onPointerDown: (e) => {
          e.stopPropagation()
          onSelect?.(d.id)
        },
      }
    : {}

  if (d.type === 'vline' && d.date) {
    const x = xForLabel(d.date)
    if (x == null) return null
    return (
      <g key={d.id}>
        {selectMode ? (
          <line x1={x} x2={x} y1={offset.top} y2={offset.top + ctx.innerHeight} stroke="transparent" strokeWidth={12} {...pickProps} />
        ) : null}
        <line x1={x} x2={x} y1={offset.top} y2={offset.top + ctx.innerHeight} stroke={stroke} strokeWidth={sw} strokeDasharray="6 4" style={{ pointerEvents: 'none' }} />
      </g>
    )
  }

  if (d.type === 'hline' && isNum(d.value)) {
    const y = yForValue(d.value)
    if (y == null) return null
    return (
      <g key={d.id}>
        {selectMode ? (
          <line x1={offset.left} x2={offset.left + ctx.innerWidth} y1={y} y2={y} stroke="transparent" strokeWidth={12} {...pickProps} />
        ) : null}
        <line x1={offset.left} x2={offset.left + ctx.innerWidth} y1={y} y2={y} stroke={stroke} strokeWidth={sw} style={{ pointerEvents: 'none' }} />
      </g>
    )
  }

  if (d.type === 'box' && d.dateStart && d.dateEnd && isNum(d.valueTop) && isNum(d.valueBottom)) {
    const [d0, d1] = sortDatePair(d.dateStart, d.dateEnd)
    const [v0, v1] = sortValuePair(d.valueTop, d.valueBottom)
    const x0 = xForLabel(d0)
    const x1 = xForLabel(d1)
    const y0 = yForValue(v1)
    const y1 = yForValue(v0)
    if ([x0, x1, y0, y1].some((v) => v == null)) return null
    const left = Math.min(x0, x1)
    const width = Math.abs(x1 - x0)
    const top = Math.min(y0, y1)
    const boxHeight = Math.abs(y1 - y0)
    return (
      <rect
        key={d.id}
        x={left}
        y={top}
        width={Math.max(width, 2)}
        height={Math.max(boxHeight, 2)}
        fill={stroke}
        fillOpacity={selectMode ? 0.12 : 0.08}
        stroke={stroke}
        strokeWidth={sw}
        {...pickProps}
      />
    )
  }

  if (d.type === 'text' && d.date && d.text) {
    const x = xForLabel(d.date)
    const y = isNum(d.value) ? yForValue(d.value) : offset.top + 14
    if (x == null || y == null) return null
    return (
      <text
        key={d.id}
        x={x + 4}
        y={y}
        fill={CHART_WS.drawingText || '#e2e8f0'}
        fontSize={11}
        fontWeight={600}
        {...pickProps}
      >
        {d.text}
      </text>
    )
  }

  return null
}

function useDrawingsCustomizedComponent(drawOptsRef) {
  return React.useMemo(() => {
    function ChartDrawingsCustomized(rechartsProps) {
      const opts = drawOptsRef.current
      const {
        panelId,
        drawings,
        draft,
        selectedId,
        labels,
        interactionRef,
        overlayActive,
        selectMode,
        onSelectDrawing,
        drawCursor,
        onDrawPointerDown,
        onDrawPointerMove,
        onDrawPointerUp,
      } = opts

      const { xAxisMap, yAxisMap, offset, width, height } = rechartsProps
      const xAxis = xAxisMap?.[Object.keys(xAxisMap || {})[0]]
      const yAxis = yAxisMap?.[Object.keys(yAxisMap || {})[0]]
      if (!xAxis?.scale || !yAxis?.scale) return null

      const xScale = xAxis.scale
      const yScale = yAxis.scale
      const bandwidth = typeof xScale.bandwidth === 'function' ? xScale.bandwidth() : 0

      const xForLabel = (label) => {
        const band = xScale(label)
        if (band == null || Number.isNaN(band)) return null
        return band + (bandwidth ? bandwidth / 2 : 0) + offset.left
      }
      const yForValue = (val) => {
        const y = yScale(val)
        return y == null || Number.isNaN(y) ? null : y + offset.top
      }

      const innerWidth = width - offset.left - offset.right
      const innerHeight = height - offset.top - offset.bottom

      if (interactionRef) {
        interactionRef.current = {
          xScale,
          yScale,
          offset,
          labels,
          innerWidth,
          innerHeight,
          xForLabel,
          yForValue,
          pointerToDataFromPlot: (plotX, plotY) => {
            const label = nearestLabelFromX(labels, xScale, plotX, 0)
            const value = valueFromY(yScale, plotY, 0)
            return { label, value }
          },
        }
      }

      const panelDrawings = drawingsForPanel(drawings, panelId)
      const shapes = panelDrawings.map((d) =>
        renderDrawingShape(
          d,
          { xForLabel, yForValue, offset, innerWidth, innerHeight },
          { selectedId, selectMode, onSelect: onSelectDrawing },
        ),
      )

      let draftShape = null
      if (draft && (draft.panelId === panelId || draft.type === 'vline')) {
        draftShape = renderDrawingShape(
          { ...draft, id: '__draft__' },
          { xForLabel, yForValue, offset, innerWidth, innerHeight },
          { selectedId: null },
        )
      }

      return (
        <g className="chart-ws-drawings">
          {shapes}
          {draftShape}
          <rect
            x={offset.left}
            y={offset.top}
            width={innerWidth}
            height={innerHeight}
            fill="transparent"
            style={{
              cursor: drawCursor,
              pointerEvents: overlayActive || draft != null ? 'all' : 'none',
            }}
            onPointerDown={(e) => onDrawPointerDown?.(e, panelId)}
            onPointerMove={(e) => onDrawPointerMove?.(e, panelId)}
            onPointerUp={(e) => onDrawPointerUp?.(e, panelId)}
            onPointerLeave={(e) => onDrawPointerUp?.(e, panelId)}
          />
        </g>
      )
    }

    ChartDrawingsCustomized.displayName = 'ChartDrawingsCustomized'
    return ChartDrawingsCustomized
  }, [drawOptsRef])
}

export function ChartPanel({
  panelId,
  title,
  subtitle = null,
  dataKey,
  color,
  data,
  yFormatter,
  height = 280,
  showXAxis = false,
  connectNulls = true,
  extremes = null,
  showExtremes = false,
  showZeroLine = false,
  panelWarning = null,
  onPoint,
  onClear,
  syncId,
  drawings = [],
  draft = null,
  selectedId = null,
  interactionRef,
  overlayActive = false,
  selectMode = false,
  onSelectDrawing,
  drawCursor = 'default',
  onDrawPointerDown,
  onDrawPointerMove,
  onDrawPointerUp,
  activeLabel = null,
  onDateClick = null,
  clickToExplain = false,
  yDomain = null,
}) {
  const hasData = data.some((d) => isNum(d[dataKey]))
  const raw = rangeMinMax(data, dataKey)
  const fit = qualityDomain(data, dataKey, yDomain)
  const labels = data.map((d) => d.label)
  const latestValue = [...data].reverse().find((d) => isNum(d[dataKey]))?.[dataKey]

  // Give every series enough vertical room to read its own historical story.
  // The price panel gets a little more; positioning panels never collapse below 285px.
  const effectiveHeight = panelId === 'price' ? Math.max(height, 330) : Math.max(height, 285)

  const drawOptsRef = React.useRef({})
  drawOptsRef.current = {
    panelId,
    drawings,
    draft,
    selectedId,
    labels,
    interactionRef,
    overlayActive,
    selectMode,
    onSelectDrawing,
    drawCursor,
    onDrawPointerDown,
    onDrawPointerMove,
    onDrawPointerUp,
  }
  const drawingsComponent = useDrawingsCustomizedComponent(drawOptsRef)

  return (
    <div className="chart-ws-panel chart-ws-panel--quality" data-panel={panelId}>
      <div className="chart-ws-panel-bar">
        <div className="chart-ws-panel-titles">
          <span className="chart-ws-panel-label">{title}</span>
          {subtitle ? <span className="chart-ws-panel-sub">{subtitle}</span> : null}
        </div>
        <div className="chart-ws-panel-value" style={{ color }}>
          {isNum(latestValue) ? yFormatter(latestValue) : '—'}
          {fit.clipped > 0 ? <span className="chart-ws-panel-fit">AUTO-FIT</span> : null}
        </div>
      </div>
      {!hasData ? (
        <p className="chart-ws-empty">{panelWarning || 'No data in selected range.'}</p>
      ) : (
        <>
          {panelWarning ? (
            <p className="chart-ws-panel-warn" role="alert">
              {panelWarning}
            </p>
          ) : null}
          <ResponsiveContainer width="100%" height={effectiveHeight}>
            <LineChart
              data={data}
              syncId={syncId}
              syncMethod="index"
              margin={{ top: 12, right: 6, left: 10, bottom: 20 }}
              onMouseMove={(state) => {
                const p = state?.activePayload?.[0]?.payload
                if (p) onPoint?.(p)
              }}
              onMouseLeave={onClear}
              onClick={(state) => {
                if (selectMode) onSelectDrawing?.(null)
                const p = state?.activePayload?.[0]?.payload
                if (p && clickToExplain && onDateClick) onDateClick(p)
              }}
            >
              <CartesianGrid
                strokeDasharray="2 4"
                stroke={CHART_WS.grid}
                vertical
                horizontal
              />
              <XAxis
                dataKey="label"
                hide={false}
                tick={{ fontSize: CHART_WS.axisFontSize, fill: CHART_WS.axis, fontFamily: CHART_WS.fontFamily }}
                interval="preserveStartEnd"
                minTickGap={58}
                axisLine={{ stroke: CHART_WS.border }}
                tickLine={false}
                height={30}
              />
              <YAxis
                orientation="right"
                tick={{ fontSize: CHART_WS.axisFontSize, fill: CHART_WS.axis, fontFamily: CHART_WS.fontFamily }}
                width={82}
                tickFormatter={yFormatter}
                domain={fit.domain}
                allowDataOverflow={fit.clipped > 0}
                axisLine={{ stroke: CHART_WS.border }}
                tickLine={false}
                tickCount={6}
              />
              <Tooltip cursor={<VerticalCrosshair />} content={() => null} />
              {activeLabel ? (
                <ReferenceLine
                  x={activeLabel}
                  stroke={CHART_WS.crosshair}
                  strokeWidth={1}
                  strokeDasharray="4 4"
                />
              ) : null}
              {showExtremes ? (
                <ExtremeZones
                  extremes={extremes}
                  color={color}
                  domainMin={isNum(fit.min) ? fit.domain[0] : raw.min}
                  domainMax={isNum(fit.max) ? fit.domain[1] : raw.max}
                />
              ) : null}
              {showZeroLine ? (
                <ReferenceLine y={0} stroke={CHART_WS.zero || 'rgba(148,163,184,.28)'} strokeWidth={1} ifOverflow="hidden" />
              ) : null}
              <Line
                type={HPTL_LINE_TYPE}
                dataKey={dataKey}
                stroke={color}
                dot={false}
                activeDot={{ r: 3.5, strokeWidth: 1.5, fill: color }}
                strokeWidth={2.25}
                strokeLinecap="round"
                strokeLinejoin="round"
                connectNulls={connectNulls}
                isAnimationActive={false}
              />
              <Customized component={drawingsComponent} />
            </LineChart>
          </ResponsiveContainer>
        </>
      )}
    </div>
  )
}

export { VerticalCrosshair, rangeMinMax, qualityDomain }
