import React from 'react'

import { buildCotWorkstation } from '../cot/buildCotWorkstation.js'
import { useCot3ySeries, resolveCot3yBlock } from '../hooks/useCot3ySeries.js'
import { buildPositioningWorkstationSeries } from './data/buildPositioningWorkstationSeries.js'
import { buildWeeklyViewModel } from './data/buildWeeklyViewModel.js'
import { buildBasicLookback } from './data/buildBasicLookback.js'
import { resolveWeeklyInspectorBlock } from './data/expandWeeklyInspector.js'
import { useWorkstationOhlc } from './hooks/useWorkstationOhlc.js'

import './basicLookback.css'

function fmtPct(value, digits = 1) {
  if (value == null || !Number.isFinite(Number(value))) return '—'
  const n = Number(value)
  return `${n > 0 ? '+' : ''}${n.toFixed(digits)}%`
}

function fmtRate(value) {
  if (value == null || !Number.isFinite(Number(value))) return '—'
  return `${Math.round(Number(value))}%`
}

function useBasicSelectedWeekLookback(week) {
  const marketId = week?.instrument || ''
  const selectedDate = week?.date || null
  const { doc } = useCot3ySeries()
  const { exportBlock } = useWorkstationOhlc(marketId)
  const [inspectorBlock, setInspectorBlock] = React.useState(null)

  React.useEffect(() => {
    if (!marketId) {
      setInspectorBlock(null)
      return undefined
    }

    let cancelled = false
    fetch('/data/cot_weekly_inspector_latest.json', { cache: 'no-store' })
      .then((r) => (r.ok ? r.json() : null))
      .then((payload) => {
        if (cancelled || !payload) return
        setInspectorBlock(resolveWeeklyInspectorBlock(payload, marketId))
      })
      .catch(() => {
        if (!cancelled) setInspectorBlock(null)
      })

    return () => {
      cancelled = true
    }
  }, [marketId])

  return React.useMemo(() => {
    if (!marketId || !selectedDate || !doc || !inspectorBlock) return null

    const { block } = resolveCot3yBlock(doc, marketId)
    if (!block) return null

    let model = null
    try {
      model = buildCotWorkstation(block)
    } catch {
      return null
    }
    if (!model?.available) return null

    const binding = buildPositioningWorkstationSeries(model, null, exportBlock, {
      preserveFullCotHistory: true,
    })
    const timelineRows = binding?.rows || []
    if (!timelineRows.length) return null

    const weeklyModel = buildWeeklyViewModel({
      timelineRows,
      researchBlock: { weekly_inspector: inspectorBlock },
      instrument: marketId,
      loadedLatestDate: null,
      staleView: false,
    })

    return buildBasicLookback({
      weeklyView: weeklyModel.weeklyView,
      dates: weeklyModel.dates,
      selectedDate,
    })
  }, [marketId, selectedDate, doc, exportBlock, inspectorBlock])
}

export function BasicLookbackPanel({ week }) {
  const lookback = useBasicSelectedWeekLookback(week)

  if (!lookback) {
    return (
      <section className="cot-lookback cot-lookback--loading" aria-label="Historical lookback">
        <div className="cot-lookback-kicker">LOOKBACK · BASIC V1</div>
        <p>Preparing historical cohort…</p>
      </section>
    )
  }

  if (!lookback.available) {
    return (
      <section className="cot-lookback cot-lookback--empty" aria-label="Historical lookback">
        <div className="cot-lookback-kicker">LOOKBACK · BASIC V1</div>
        <p>{lookback.reason || 'Lookback unavailable for this week.'}</p>
      </section>
    )
  }

  return (
    <section className="cot-lookback" aria-label="Historical lookback">
      <div className="cot-lookback-head">
        <div>
          <div className="cot-lookback-kicker">LOOKBACK · BASIC V1</div>
          <div className="cot-lookback-rule">{lookback.cohortLabel}</div>
        </div>
        <div className="cot-lookback-count">
          <strong>{lookback.priorMatchCount}</strong>
          <span>prior matching weeks</span>
        </div>
      </div>

      <div className="cot-lookback-table" role="table" aria-label="Forward price outcomes">
        <div className="cot-lookback-row cot-lookback-row--head" role="row">
          <span>Horizon</span>
          <span>Median</span>
          <span>Higher</span>
          <span>N</span>
        </div>
        {lookback.horizons.map((horizon) => {
          const outcome = lookback.outcomes?.[horizon] || {}
          return (
            <div className="cot-lookback-row" role="row" key={horizon}>
              <strong>{horizon}W</strong>
              <span className={
                outcome.medianReturnPct > 0
                  ? 'is-positive'
                  : outcome.medianReturnPct < 0
                    ? 'is-negative'
                    : ''
              }>
                {fmtPct(outcome.medianReturnPct)}
              </span>
              <span>{fmtRate(outcome.positiveRatePct)}</span>
              <span>{outcome.sampleCount ?? 0}</span>
            </div>
          )
        })}
      </div>

      <p className="cot-lookback-note">
        Point-in-time only · no future data beyond {week?.date || 'the selected week'} is used.
      </p>
    </section>
  )
}
