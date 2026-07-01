import React from 'react'

import { CHART_WS, PANEL_IDS } from '../../charts/chartTheme.js'
import { resolveTimelineTime } from '../canvas/workstationDrawingCoords.js'
import { snapTimeToTimeline } from '../canvas/workstationDrawingTimeline.js'
import {
  DEFAULT_VLINE_STYLE,
  globalVlineDrawings,
  WORKSTATION_DRAWING_TOOLS,
} from '../canvas/workstationDrawingTypes.js'
import {
  bindChartViewport,
  chartTimeToX,
  chartXToTime,
  measureGlobalOverlayLayout,
} from './drawingViewport.js'

const SVG_NS = 'http://www.w3.org/2000/svg'

function setLinePosition(lineEl, hitEl, x) {
  if (x == null || !Number.isFinite(x)) {
    lineEl.setAttribute('visibility', 'hidden')
    if (hitEl) hitEl.setAttribute('visibility', 'hidden')
    return
  }
  lineEl.setAttribute('visibility', 'visible')
  lineEl.setAttribute('x1', String(x))
  lineEl.setAttribute('x2', String(x))
  if (hitEl) {
    hitEl.setAttribute('visibility', 'visible')
    hitEl.setAttribute('x1', String(x))
    hitEl.setAttribute('x2', String(x))
  }
}

function applyStroke(lineEl, selected, color, width) {
  lineEl.setAttribute('stroke', selected ? CHART_WS.drawingSelected : color || DEFAULT_VLINE_STYLE.color)
  lineEl.setAttribute(
    'stroke-width',
    String(selected ? Math.max(width || 1.5, 2) : width || DEFAULT_VLINE_STYLE.width),
  )
}

function createVlineNodes(svg, id, onDragStart) {
  const group = document.createElementNS(SVG_NS, 'g')
  group.setAttribute('data-ws-vline', 'global')
  group.setAttribute('data-drawing-id', id)

  const hit = document.createElementNS(SVG_NS, 'line')
  hit.setAttribute('y1', '0')
  hit.setAttribute('y2', '100%')
  hit.setAttribute('stroke', 'transparent')
  hit.setAttribute('stroke-width', '14')
  hit.style.cursor = 'ew-resize'
  hit.style.pointerEvents = 'stroke'
  hit.addEventListener('pointerdown', (e) => onDragStart(id, e))

  const line = document.createElementNS(SVG_NS, 'line')
  line.setAttribute('y1', '0')
  line.setAttribute('y2', '100%')
  line.setAttribute('vector-effect', 'non-scaling-stroke')
  line.style.pointerEvents = 'none'

  group.appendChild(hit)
  group.appendChild(line)
  svg.appendChild(group)

  return { group, hit, line }
}

/**
 * Global vertical timeline markers — imperative SVG layer.
 * Data coords: drawing.date → resolved to bar time at paint time only.
 */
export function GlobalTimelineVlineOverlay({
  stackRef,
  getPaneChart,
  subscribeGeometry,
  timelineRows = [],
  drawings = [],
  selectedId = null,
  activeTool = WORKSTATION_DRAWING_TOOLS.SELECT,
  dateToTime,
  onSelectDrawing,
  onDrawingCommit,
  onDrawingUpdate,
}) {
  const svgRef = React.useRef(null)
  const nodeMapRef = React.useRef(new Map())
  const dragRef = React.useRef(null)
  const dragPreviewDateRef = React.useRef(null)
  const metaRef = React.useRef({})
  const repaintRef = React.useRef(() => {})
  const startDragRef = React.useRef(() => {})

  metaRef.current = {
    timelineRows,
    drawings,
    selectedId,
    activeTool,
    dateToTime,
    onSelectDrawing,
    onDrawingCommit,
    onDrawingUpdate,
    getPaneChart,
    stackRef,
  }

  const snapDate = React.useCallback((clientX) => {
    const chart = metaRef.current.getPaneChart?.(PANEL_IDS.price)
    if (!chart) return null
    const time = chartXToTime(chart, clientX)
    if (time == null) return null
    const snapped = snapTimeToTimeline(metaRef.current.timelineRows, time)
    if (snapped == null) return null
    const row = metaRef.current.timelineRows.find((r) => r.time === snapped)
    return row?.label || row?.date || null
  }, [])

  startDragRef.current = (drawingId, e) => {
    if (metaRef.current.activeTool !== WORKSTATION_DRAWING_TOOLS.SELECT) return
    e.preventDefault()
    e.stopPropagation()
    metaRef.current.onSelectDrawing?.(drawingId)
    dragRef.current = { id: drawingId }

    const onMove = (ev) => {
      if (!dragRef.current) return
      ev.preventDefault()
      const date = snapDate(ev.clientX)
      if (!date) return
      dragPreviewDateRef.current = date
      repaintRef.current()
    }

    const onUp = (ev) => {
      document.removeEventListener('pointermove', onMove)
      document.removeEventListener('pointerup', onUp)
      document.removeEventListener('pointercancel', onUp)
      const drag = dragRef.current
      dragRef.current = null
      dragPreviewDateRef.current = null
      if (!drag) return
      const date = snapDate(ev.clientX)
      if (date) metaRef.current.onDrawingUpdate?.(drag.id, { date })
      repaintRef.current()
    }

    document.addEventListener('pointermove', onMove)
    document.addEventListener('pointerup', onUp)
    document.addEventListener('pointercancel', onUp)
  }

  React.useEffect(() => {
    const svg = svgRef.current
    const stackEl = stackRef.current
    if (!svg || !stackEl) return undefined

    let boundChart = null
    let unbindChart = () => {}

    const repaint = () => {
      const chart = metaRef.current.getPaneChart?.(PANEL_IDS.price) ?? null
      const stack = metaRef.current.stackRef?.current
      if (!chart || !stack) return

      if (chart !== boundChart) {
        unbindChart()
        boundChart = chart
        unbindChart = bindChartViewport(chart, null, repaint)
      }

      const layout = measureGlobalOverlayLayout(chart, stack)
      if (!layout) return
      svg.style.left = `${layout.left}px`
      svg.style.width = `${layout.width}px`
      svg.style.height = `${layout.height}px`

      const meta = metaRef.current
      const vlines = globalVlineDrawings(meta.drawings)
      const selectMode = meta.activeTool === WORKSTATION_DRAWING_TOOLS.SELECT
      const ids = new Set(vlines.map((d) => d.id))

      for (const [id, nodes] of nodeMapRef.current) {
        if (!ids.has(id)) {
          nodes.group.remove()
          nodeMapRef.current.delete(id)
        }
      }

      for (const d of vlines) {
        let nodes = nodeMapRef.current.get(d.id)
        if (!nodes) {
          nodes = createVlineNodes(svg, d.id, (id, e) => startDragRef.current(id, e))
          nodeMapRef.current.set(d.id, nodes)
        }
        nodes.hit.style.display = selectMode ? '' : 'none'

        const previewDate = dragRef.current?.id === d.id ? dragPreviewDateRef.current : null
        const time =
          previewDate && meta.dateToTime
            ? meta.dateToTime(previewDate)
            : resolveTimelineTime(d, 'date', meta.dateToTime)
        const x = chartTimeToX(chart, time)
        const selected = d.id === meta.selectedId
        applyStroke(nodes.line, selected, d.color, d.width)
        setLinePosition(nodes.line, nodes.hit, x)
      }
    }

    repaintRef.current = repaint

    const unbindStack = subscribeGeometry?.(repaint) ?? (() => {})
    const ro = new ResizeObserver(repaint)
    ro.observe(stackEl)

    repaint()

    return () => {
      unbindChart()
      unbindStack()
      ro.disconnect()
      for (const nodes of nodeMapRef.current.values()) nodes.group.remove()
      nodeMapRef.current.clear()
      boundChart = null
    }
  }, [stackRef, subscribeGeometry, snapDate])

  React.useEffect(() => {
    repaintRef.current()
  }, [drawings, selectedId, activeTool, timelineRows, dateToTime])

  const onPointerDown = (e) => {
    if (e.button !== 0 || metaRef.current.activeTool !== WORKSTATION_DRAWING_TOOLS.VLINE) return
    e.preventDefault()
    e.stopPropagation()
    const date = snapDate(e.clientX)
    if (!date) return
    metaRef.current.onDrawingCommit?.({ type: 'vline', date })
  }

  const vlineToolActive = activeTool === WORKSTATION_DRAWING_TOOLS.VLINE

  return (
    <svg
      ref={svgRef}
      className="ws-global-timeline-overlay"
      aria-hidden={vlineToolActive ? undefined : true}
      style={{
        pointerEvents: vlineToolActive ? 'auto' : 'none',
        cursor: vlineToolActive ? 'crosshair' : 'default',
      }}
      onPointerDown={onPointerDown}
    />
  )
}
