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
  DRAWING_TOOLS,
  drawingsForPanel,
  nearestLabelFromX,
  valueFromY,
} from './chartDrawings.js'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function rangeMinMax(data, key) {
  const vals = data.map((d) => d[key]).filter(isNum)
  if (!vals.length) return { min: null, max: null }
  return { min: Math.min(...vals), max: Math.max(...vals) }
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
        fillOpacity={0.06}
        strokeOpacity={0}
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
        fillOpacity={0.06}
        strokeOpacity={0}
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
          <line
            x1={x}
            x2={x}
            y1={offset.top}
            y2={offset.top + ctx.innerHeight}
            stroke="transparent"
            strokeWidth={12}
            {...pickProps}
          />
        ) : null}
        <line
          x1={x}
          x2={x}
          y1={offset.top}
          y2={offset.top + ctx.innerHeight}
          stroke={stroke}
          strokeWidth={sw}
          strokeDasharray="6 4"
          style={{ pointerEvents: 'none' }}
        />
      </g>
    )
  }

  if (d.type === 'hline' && isNum(d.value)) {
    const y = yForValue(d.value)
    if (y == null) return null
    return (
      <g key={d.id}>
        {selectMode ? (
          <line
            x1={offset.left}
            x2={offset.left + ctx.innerWidth}
            y1={y}
            y2={y}
            stroke="transparent"
            strokeWidth={12}
            {...pickProps}
          />
        ) : null}
        <line
          x1={offset.left}
          x2={offset.left + ctx.innerWidth}
          y1={y}
          y2={y}
          stroke={stroke}
          strokeWidth={sw}
          style={{ pointerEvents: 'none' }}
        />
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
    const height = Math.abs(y1 - y0)
    return (
      <rect
        key={d.id}
        x={left}
        y={top}
        width={Math.max(width, 2)}
        height={Math.max(height, 2)}
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
        fill={CHART_WS.drawingText}
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
  const { min, max } = rangeMinMax(data, dataKey)
  const pad = isNum(min) && isNum(max) ? (max - min) * 0.05 || 1 : 0
  const axisDomain =
    Array.isArray(yDomain) && yDomain.length === 2
      ? yDomain
      : isNum(min) && isNum(max)
        ? [min - pad, max + pad]
        : ['auto', 'auto']
  const labels = data.map((d) => d.label)

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
    <div className="chart-ws-panel" data-panel={panelId}>
      <div className="chart-ws-panel-bar">
        <div className="chart-ws-panel-titles">
          <span className="chart-ws-panel-label">{title}</span>
          {subtitle ? <span className="chart-ws-panel-sub">{subtitle}</span> : null}
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
          <ResponsiveContainer width="100%" height={height}>
            <LineChart
              data={data}
              syncId={syncId}
              margin={{ top: 6, right: 14, left: 2, bottom: showXAxis ? 18 : 4 }}
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
                strokeDasharray="3 3"
                stroke={CHART_WS.grid}
                vertical
                horizontal
              />
              <XAxis
                dataKey="label"
                hide={!showXAxis}
                tick={{ fontSize: CHART_WS.axisFontSize, fill: CHART_WS.axis, fontFamily: CHART_WS.fontFamily }}
                interval="preserveStartEnd"
                minTickGap={36}
                axisLine={{ stroke: CHART_WS.border }}
                tickLine={{ stroke: CHART_WS.border }}
              />
              <YAxis
                tick={{ fontSize: CHART_WS.axisFontSize, fill: CHART_WS.axis, fontFamily: CHART_WS.fontFamily }}
                width={76}
                tickFormatter={yFormatter}
                domain={axisDomain}
                axisLine={{ stroke: CHART_WS.border }}
                tickLine={{ stroke: CHART_WS.border }}
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
                  domainMin={isNum(min) ? min - pad : null}
                  domainMax={isNum(max) ? max + pad : null}
                />
              ) : null}
              {showZeroLine ? (
                <ReferenceLine y={0} stroke={CHART_WS.zero} strokeWidth={1} />
              ) : null}
              <Line
                type={HPTL_LINE_TYPE}
                dataKey={dataKey}
                stroke={color}
                dot={false}
                strokeWidth={2}
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

export { VerticalCrosshair, rangeMinMax }
