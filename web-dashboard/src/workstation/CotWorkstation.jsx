import React from 'react'

import { findCotRowAsOf } from '../charts/positioningTimelineAlign.js'
import { CHART_WS, PANEL_IDS } from '../charts/chartTheme.js'
import { buildCotWorkstation } from '../cot/buildCotWorkstation.js'
import {
  POSITIONING_DEFAULT_RANGE_ID,
  POSITIONING_RANGE_PRESETS,
  rangePresetById,
} from '../cot/positioningChartMetrics.js'
import { useCot3ySeries, resolveCot3yBlock } from '../hooks/useCot3ySeries.js'
import { COT_3Y_PATH } from '../data/cot3ySeriesStore.js'
import { useWorkstationOhlc } from './hooks/useWorkstationOhlc.js'
import {
  buildPositioningWorkstationSeries,
  rowsToWeeklyBars,
  sliceWorkstationRows,
} from './data/buildPositioningWorkstationSeries.js'
import { labelFromTimelineTime, rowsToLinePoints } from './charts/buildWorkstationTimelineData.js'
import { useLinkedChartTimeline } from './charts/useLinkedChartTimeline.js'
import { SimpleChartPane } from './charts/SimpleChartPane.jsx'

import '../charts/positioningChart.css'
import './cotWorkstation.css'

const PANEL_HEIGHT = 220

function PanelShell({ title, children }) {
  return (
    <div className="cot-ws-panel">
      <div className="cot-ws-panel-head">
        <span className="cot-ws-panel-title">{title}</span>
      </div>
      <div className="cot-ws-panel-body" style={{ height: PANEL_HEIGHT }}>
        {children}
      </div>
    </div>
  )
}

export function CotWorkstation({ marketId }) {
  const { doc, loading, errored } = useCot3ySeries()
  const { exportBlock } = useWorkstationOhlc(marketId)
  const [rangeId, setRangeId] = React.useState(POSITIONING_DEFAULT_RANGE_ID)
  const [crosshairTime, setCrosshairTime] = React.useState(null)
  const [crosshairLabel, setCrosshairLabel] = React.useState(null)

  const { block } = React.useMemo(() => resolveCot3yBlock(doc, marketId), [doc, marketId])

  const model = React.useMemo(() => {
    if (!block) return null
    try {
      return buildCotWorkstation(block)
    } catch (err) {
      console.error('[cot-workstation] buildCotWorkstation failed', marketId, err)
      return { available: false, error: String(err?.message || err) }
    }
  }, [block, marketId])

  const binding = React.useMemo(() => {
    if (!model?.available) return null
    return buildPositioningWorkstationSeries(model, null, exportBlock)
  }, [model, exportBlock])

  const preset = rangePresetById(rangeId)
  const fullSeries = model?.series ?? []

  const visibleRows = React.useMemo(
    () => sliceWorkstationRows(binding?.rows ?? [], preset.weeks),
    [binding?.rows, preset.weeks],
  )

  const visibleBars = React.useMemo(() => rowsToWeeklyBars(visibleRows), [visibleRows])

  const { registerPane, fitAll, setExternalCrosshair } = useLinkedChartTimeline()
  const fitKey = `${marketId}:${rangeId}:${visibleRows.length}`

  React.useEffect(() => {
    fitAll(fitKey)
  }, [fitKey, fitAll])

  React.useEffect(() => {
    setExternalCrosshair(crosshairTime)
  }, [crosshairTime, setExternalCrosshair])

  const onCrosshairMove = React.useCallback(
    (payload) => {
      if (!payload?.time) return
      const label = labelFromTimelineTime(visibleRows, payload.time)
      setCrosshairTime(payload.time)
      setCrosshairLabel(label)
    },
    [visibleRows],
  )

  const onCrosshairClear = React.useCallback(() => {
    setCrosshairTime(null)
    setCrosshairLabel(null)
  }, [])

  const activeRow = React.useMemo(() => {
    if (!fullSeries.length) return null
    if (!crosshairLabel) return fullSeries[fullSeries.length - 1]
    return findCotRowAsOf(fullSeries, crosshairLabel) || fullSeries[fullSeries.length - 1]
  }, [crosshairLabel, fullSeries])

  const paneProps = {
    timelineRows: visibleRows,
    registerPane,
    onCrosshairMove,
    onCrosshairClear,
    externalCrosshairTime: crosshairTime,
  }

  if (loading && !doc) {
    return (
      <p className="cot-ws-status" role="status">
        Loading COT series from <code>{COT_3Y_PATH}</code>…
      </p>
    )
  }

  if (!model?.available || !visibleRows.length) {
    return (
      <div className="cot-ws-status cot-ws-status--error">
        <p>
          No COT workstation data for <strong>{marketId}</strong>
          {errored ? ' (fetch error — check network tab)' : ''}.
        </p>
        {model?.error ? <p className="cot-ws-status-detail">{model.error}</p> : null}
        {!block && doc ? (
          <p className="cot-ws-status-detail">Market not found in {COT_3Y_PATH}.</p>
        ) : null}
      </div>
    )
  }

  const instTitle = model.institutionalGroup || 'Non-Commercial'
  const retailTitle = model.retailGroup || 'Non-Reportable'

  return (
    <div className="cot-workstation positioning-chart-stack positioning-chart-stack--cot3y">
      <header className="cot-ws-toolbar">
        <div className="cot-ws-toolbar-left">
          <span className="cot-ws-history">{model.historyLabel}</span>
          <span className="cot-ws-weeks">{visibleRows.length} weeks visible</span>
        </div>
        <div className="cot-ws-toolbar-center">
          <span className="cot-ws-crosshair-label">
            {crosshairLabel || activeRow?.date || '—'}
          </span>
        </div>
        <div className="cot-ws-range-toggles" role="group" aria-label="Chart range">
          {POSITIONING_RANGE_PRESETS.map((p) => (
            <button
              key={p.id}
              type="button"
              className={`cot-ws-range-btn${p.id === rangeId ? ' active' : ''}`}
              onClick={() => setRangeId(p.id)}
            >
              {p.label}
            </button>
          ))}
        </div>
      </header>

      <PanelShell title="Weekly OHLC">
        <SimpleChartPane
          {...paneProps}
          panelId={PANEL_IDS.price}
          mode="candle"
          candleBars={visibleBars}
        />
      </PanelShell>

      <PanelShell title="Commercial net">
        <SimpleChartPane
          {...paneProps}
          panelId={PANEL_IDS.commercial}
          mode="line"
          lineColor={CHART_WS.commercial}
          linePoints={rowsToLinePoints(visibleRows, 'commercial_net')}
          zeroLine
        />
      </PanelShell>

      <PanelShell title={`${instTitle} net`}>
        <SimpleChartPane
          {...paneProps}
          panelId={PANEL_IDS.institutional}
          mode="line"
          lineColor={CHART_WS.institutional}
          linePoints={rowsToLinePoints(visibleRows, 'institutional_net')}
          zeroLine
        />
      </PanelShell>

      <PanelShell title={`${retailTitle} net`}>
        <SimpleChartPane
          {...paneProps}
          panelId={PANEL_IDS.retail}
          mode="line"
          showTimeAxis
          lineColor={CHART_WS.retail}
          linePoints={rowsToLinePoints(visibleRows, 'retail_net')}
          zeroLine
        />
      </PanelShell>
    </div>
  )
}
