import React from 'react'

import { CHART_WS } from '../../charts/chartTheme.js'
import { resolveTimelineTime } from '../canvas/workstationDrawingCoords.js'
import { snapRectTimes, snapTimeToTimeline } from '../canvas/workstationDrawingTimeline.js'
import {
  drawingsForWorkstationPanel,
  WORKSTATION_DRAWING_TOOLS,
} from '../canvas/workstationDrawingTypes.js'
import {
  bindChartViewport,
  chartTimeToX,
  pointerToPanelData,
  seriesValueToY,
} from './drawingViewport.js'

const SVG_NS = 'http://www.w3.org/2000/svg'

function isNum(v) {
  return typeof v === 'number' && Number.isFinite(v)
}

/**
 * Per-panel hline / rect overlay — imperative repaint on viewport change only.
 * Data coords: dateStart/dateEnd + valueTop/valueBottom, or value for hline.
 */
export function WorkstationLwcDrawingOverlay({
  chart,
  primarySeries,
  panelId,
  timelineRows = [],
  drawings = [],
  selectedId = null,
  activeTool = WORKSTATION_DRAWING_TOOLS.SELECT,
  dateToTime,
  onSelectDrawing,
  onDrawingCommit,
}) {
  const svgRef = React.useRef(null)
  const nodeMapRef = React.useRef(new Map())
  const draftRef = React.useRef(null)
  const dragRef = React.useRef(null)
  const metaRef = React.useRef({
    panelId,
    timelineRows,
    drawings,
    selectedId,
    activeTool,
    dateToTime,
    onSelectDrawing,
    onDrawingCommit,
  })

  metaRef.current = {
    panelId,
    timelineRows,
    drawings,
    selectedId,
    activeTool,
    dateToTime,
    onSelectDrawing,
    onDrawingCommit,
  }

  const repaintRef = React.useRef(() => {})

  const repaint = React.useCallback(() => {
    const svg = svgRef.current
    const meta = metaRef.current
    if (!svg || !chart || !primarySeries) return

    const panelDrawings = drawingsForWorkstationPanel(meta.drawings, meta.panelId)
    const selectMode = meta.activeTool === WORKSTATION_DRAWING_TOOLS.SELECT
    const ids = new Set(panelDrawings.map((d) => d.id))

    for (const [id, nodes] of nodeMapRef.current) {
      if (!ids.has(id)) {
        nodes.group.remove()
        nodeMapRef.current.delete(id)
      }
    }

      for (const d of panelDrawings) {
        let nodes = nodeMapRef.current.get(d.id)
        if (!nodes) {
          nodes = createDrawingNodes(svg, d, selectMode, meta.onSelectDrawing)
          nodeMapRef.current.set(d.id, nodes)
        }
        if (nodes.hit && d.type === 'hline') {
          nodes.hit.style.pointerEvents = selectMode ? 'stroke' : 'none'
        }
        paintDrawing(nodes, d, chart, primarySeries, meta, false)
      }

    if (draftRef.current) {
      let draftNodes = nodeMapRef.current.get('__draft__')
      if (!draftNodes) {
        draftNodes = createDrawingNodes(svg, draftRef.current, false, null)
        nodeMapRef.current.set('__draft__', draftNodes)
      }
      paintDrawing(draftNodes, draftRef.current, chart, primarySeries, meta, true)
    } else {
      const draftNodes = nodeMapRef.current.get('__draft__')
      if (draftNodes) {
        draftNodes.group.remove()
        nodeMapRef.current.delete('__draft__')
      }
    }
  }, [chart, primarySeries])

  repaintRef.current = repaint

  React.useEffect(() => {
    if (!chart || !primarySeries) return undefined
    return bindChartViewport(chart, primarySeries, () => repaintRef.current())
  }, [chart, primarySeries])

  React.useEffect(() => {
    repaintRef.current()
  }, [drawings, selectedId, activeTool, timelineRows, dateToTime, panelId, chart, primarySeries, repaint])

  React.useEffect(
    () => () => {
      for (const nodes of nodeMapRef.current.values()) nodes.group.remove()
      nodeMapRef.current.clear()
    },
    [],
  )

  const drawToolActive =
    activeTool === WORKSTATION_DRAWING_TOOLS.HLINE || activeTool === WORKSTATION_DRAWING_TOOLS.RECT

  const snapHit = (hit) => {
    if (!hit) return null
    const time = snapTimeToTimeline(metaRef.current.timelineRows, hit.time)
    return { ...hit, time }
  }

  const onPointerDown = (e) => {
    if (e.button !== 0 || !drawToolActive) return
    e.preventDefault()
    e.stopPropagation()
    try {
      e.currentTarget.setPointerCapture(e.pointerId)
    } catch {
      /* ignore */
    }
    const hit = snapHit(pointerToPanelData(chart, primarySeries, e.clientX, e.clientY))
    if (!hit) return
    const meta = metaRef.current

    if (activeTool === WORKSTATION_DRAWING_TOOLS.HLINE) {
      if (hit.value == null || !Number.isFinite(hit.value)) return
      meta.onDrawingCommit?.({ type: 'hline', panelId: meta.panelId, value: hit.value })
      return
    }

    if (activeTool === WORKSTATION_DRAWING_TOOLS.RECT) {
      if (hit.time == null) return
      const snapped = snapRectTimes(meta.timelineRows, hit.time, hit.time)
      const start = {
        type: 'rect',
        panelId: meta.panelId,
        timeStart: snapped.timeStart,
        timeEnd: snapped.timeEnd,
        valueTop: hit.value,
        valueBottom: hit.value,
      }
      dragRef.current = start
      draftRef.current = start
      repaintRef.current()
    }
  }

  const onPointerMove = (e) => {
    const drag = dragRef.current
    if (!drag || drag.type !== 'rect') return
    e.preventDefault()
    const hit = pointerToPanelData(chart, primarySeries, e.clientX, e.clientY)
    if (!hit) return
    const snapped = snapRectTimes(metaRef.current.timelineRows, drag.timeStart, hit.time)
    const next = {
      ...drag,
      timeEnd: snapped.timeEnd,
      valueBottom: hit.value,
    }
    dragRef.current = next
    draftRef.current = next
    repaintRef.current()
  }

  const onPointerUp = (e) => {
    const drag = dragRef.current
    if (!drag) return
    e.preventDefault()
    dragRef.current = null
    draftRef.current = null
    repaintRef.current()
    metaRef.current.onDrawingCommit?.(drag)
    try {
      e.currentTarget.releasePointerCapture(e.pointerId)
    } catch {
      /* ignore */
    }
  }

  if (!chart) return null

  return (
    <svg
      ref={svgRef}
      className="ws-drawing-overlay"
      aria-hidden={drawToolActive ? undefined : true}
      onPointerDown={onPointerDown}
      onPointerMove={onPointerMove}
      onPointerUp={onPointerUp}
      onPointerCancel={onPointerUp}
      style={{
        pointerEvents: drawToolActive ? 'auto' : 'none',
        cursor: drawToolActive ? 'crosshair' : 'default',
      }}
    />
  )
}

function paintDrawing(nodes, d, chart, primarySeries, meta, isDraft) {
  const selected = !isDraft && d.id === meta.selectedId
  if (d.type === 'hline') {
    const y = seriesValueToY(primarySeries, d.value)
    if (y == null) {
      nodes.line.setAttribute('visibility', 'hidden')
      if (nodes.hit) nodes.hit.setAttribute('visibility', 'hidden')
      return
    }
    nodes.line.setAttribute('visibility', 'visible')
    nodes.line.setAttribute('y1', String(y))
    nodes.line.setAttribute('y2', String(y))
    nodes.line.setAttribute('stroke', selected || isDraft ? CHART_WS.drawingSelected : CHART_WS.drawing)
    nodes.line.setAttribute('stroke-width', String(selected || isDraft ? 2 : 1.5))
    if (nodes.hit) {
      nodes.hit.setAttribute('visibility', 'visible')
      nodes.hit.setAttribute('y1', String(y))
      nodes.hit.setAttribute('y2', String(y))
    }
    return
  }
  if (d.type === 'rect') {
    const t0 = resolveTimelineTime(d, 'dateStart', meta.dateToTime) ?? d.timeStart
    const t1 = resolveTimelineTime(d, 'dateEnd', meta.dateToTime) ?? d.timeEnd
    const x1 = chartTimeToX(chart, t0)
    const x2 = chartTimeToX(chart, t1)
    const y1 = seriesValueToY(primarySeries, d.valueTop)
    const y2 = seriesValueToY(primarySeries, d.valueBottom)
    if ([x1, x2, y1, y2].some((v) => v == null)) {
      nodes.rect.setAttribute('visibility', 'hidden')
      return
    }
    const left = Math.min(x1, x2)
    const top = Math.min(y1, y2)
    nodes.rect.setAttribute('visibility', 'visible')
    nodes.rect.setAttribute('x', String(left))
    nodes.rect.setAttribute('y', String(top))
    nodes.rect.setAttribute('width', String(Math.abs(x2 - x1)))
    nodes.rect.setAttribute('height', String(Math.abs(y2 - y1)))
    nodes.rect.setAttribute('stroke', selected || isDraft ? CHART_WS.drawingSelected : CHART_WS.drawing)
    nodes.rect.setAttribute('stroke-width', String(selected || isDraft ? 2 : 1.5))
  }
}

function createDrawingNodes(svg, d, selectMode, onSelectDrawing) {
  const group = document.createElementNS(SVG_NS, 'g')
  group.setAttribute('data-drawing-id', d.id || '__draft__')

  if (d.type === 'hline') {
    const hit = document.createElementNS(SVG_NS, 'line')
    hit.setAttribute('x1', '0')
    hit.setAttribute('x2', '100%')
    hit.setAttribute('stroke', 'transparent')
    hit.setAttribute('stroke-width', '12')
    hit.style.cursor = 'pointer'
    hit.style.pointerEvents = selectMode && d.id ? 'stroke' : 'none'
    if (selectMode && d.id) {
      hit.addEventListener('pointerdown', (e) => {
        e.stopPropagation()
        onSelectDrawing?.(d.id)
      })
    }

    const line = document.createElementNS(SVG_NS, 'line')
    line.setAttribute('x1', '0')
    line.setAttribute('x2', '100%')
    line.setAttribute('stroke-dasharray', '6 4')
    line.setAttribute('vector-effect', 'non-scaling-stroke')
    line.style.pointerEvents = 'none'

    group.appendChild(hit)
    group.appendChild(line)
    svg.appendChild(group)
    return { group, line, hit, rect: null }
  }

  const rect = document.createElementNS(SVG_NS, 'rect')
  rect.setAttribute('fill', CHART_WS.drawing)
  rect.setAttribute('fill-opacity', '0.08')
  rect.setAttribute('vector-effect', 'non-scaling-stroke')
  rect.style.pointerEvents = 'none'
  group.appendChild(rect)
  svg.appendChild(group)
  return { group, line: null, hit: null, rect }
}
