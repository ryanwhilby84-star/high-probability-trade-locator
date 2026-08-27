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

function fmtRatio(value) {
  if (value == null || !Number.isFinite(Number(value))) return '—'
  return `${Number(value).toFixed(1)}x`
}

function evidenceRead(lookback, evidence) {
  const hit = Number(evidence?.hitRatePct)
  const expectancy = Number(evidence?.expectancyPct)
  const rr = Number(evidence?.rewardRiskMedian)
  const n = Number(evidence?.sampleCount)
  const direction = lookback?.expectedDirection === 'down' ? 'lower' : 'higher'

  if (![hit, expectancy, n].every(Number.isFinite)) return { label: 'NO READ', text: 'Not enough completed history to form a useful directional read.' }
  if (n < 6) return { label: 'TOO EARLY', text: `Only ${n} independent episodes are available. The current setup can still be extreme, but the historical sample is too small to trust.` }
  if (hit >= 60 && expectancy > 0 && (!Number.isFinite(rr) || rr >= 1)) return { label: 'SUPPORTIVE', text: `Comparable historical setups have favoured a move ${direction}; hit rate, expectancy and payoff profile agree.` }
  if (hit >= 55 && expectancy > 0) return { label: 'LEAN', text: `Comparable history leans ${direction}, but the edge is not strong enough to use on its own.` }
  if (hit <= 45 || expectancy < 0) return { label: 'WEAK / CONTRARY', text: `Even though the current positioning can be extreme, comparable historical setups have not produced a dependable move ${direction}.` }
  return { label: 'MIXED', text: 'The current positioning setup is extreme, but the historical price outcomes are mixed.' }
}

function useBasicSelectedWeekLookback(week) {
  const marketId = week?.instrument || ''
  const selectedDate = week?.date || null
  const { doc } = useCot3ySeries()
  const { exportBlock } = useWorkstationOhlc(marketId)
  const [inspectorBlock, setInspectorBlock] = React.useState(null)

  React.useEffect(() => {
    if (!marketId) { setInspectorBlock(null); return undefined }
    let cancelled = false
    fetch('/data/cot_weekly_inspector_latest.json', { cache: 'no-store' })
      .then((r) => (r.ok ? r.json() : null))
      .then((payload) => { if (!cancelled && payload) setInspectorBlock(resolveWeeklyInspectorBlock(payload, marketId)) })
      .catch(() => { if (!cancelled) setInspectorBlock(null) })
    return () => { cancelled = true }
  }, [marketId])

  return React.useMemo(() => {
    if (!marketId || !selectedDate || !doc || !inspectorBlock) return null
    const { block } = resolveCot3yBlock(doc, marketId)
    if (!block) return null

    let model = null
    try { model = buildCotWorkstation(block) } catch { return null }
    if (!model?.available) return null

    const binding = buildPositioningWorkstationSeries(model, null, exportBlock, { preserveFullCotHistory: true })
    const timelineRows = binding?.rows || []
    if (!timelineRows.length) return null

    const weeklyModel = buildWeeklyViewModel({
      timelineRows,
      researchBlock: { weekly_inspector: inspectorBlock },
      instrument: marketId,
      loadedLatestDate: null,
      staleView: false,
    })

    return buildBasicLookback({ weeklyView: weeklyModel.weeklyView, dates: weeklyModel.dates, selectedDate })
  }, [marketId, selectedDate, doc, exportBlock, inspectorBlock])
}

export function BasicLookbackPanel({ week }) {
  const lookback = useBasicSelectedWeekLookback(week)

  if (!lookback) {
    return <section className="cot-lookback cot-lookback--loading" aria-label="Historical lookback"><div className="cot-lookback-kicker">LOOKBACK · EVIDENCE V3</div><p>Preparing historical episodes…</p></section>
  }

  if (!lookback.available) {
    return <section className="cot-lookback cot-lookback--empty" aria-label="Historical lookback"><div className="cot-lookback-kicker">LOOKBACK · EVIDENCE V3</div><p>{lookback.reason || 'Lookback unavailable for this week.'}</p></section>
  }

  const evidence = lookback.primaryEvidence || {}
  const confidence = lookback.sampleConfidence || {}
  const setup = lookback.currentSetupState || {}
  const read = evidenceRead(lookback, evidence)
  const timing = evidence.medianWeeksTo5 != null
    ? `A 5% favourable move, when reached, took a median ${evidence.medianWeeksTo5} weeks.`
    : 'A 5% favourable move was not reached often enough to give useful timing.'

  return (
    <section className="cot-lookback" aria-label="Historical lookback">
      <div className="cot-lookback-head">
        <div>
          <div className="cot-lookback-kicker">LOOKBACK · EVIDENCE V3</div>
          <div className="cot-lookback-rule">{lookback.cohortLabel}</div>
          <div className="cot-lookback-direction">Expected COT direction: <strong>{lookback.expectedDirection === 'down' ? 'LOWER' : 'HIGHER'}</strong></div>
        </div>
        <div className="cot-lookback-count"><strong>{lookback.priorEpisodeCount}</strong><span>independent episodes</span></div>
      </div>

      <div className={`cot-lookback-setup is-${setup.tone || 'normal'}`}>
        <strong>{setup.grade || 'CURRENT SETUP'}</strong>
        <span>{setup.label || 'Current positioning state'}</span>
      </div>

      <div className="cot-lookback-evidence-grid">
        <div><span>{lookback.primaryHorizon}W hit rate</span><strong>{fmtRate(evidence.hitRatePct)}</strong></div>
        <div><span>Expectancy</span><strong className={evidence.expectancyPct > 0 ? 'is-positive' : evidence.expectancyPct < 0 ? 'is-negative' : ''}>{fmtPct(evidence.expectancyPct)}</strong></div>
        <div><span>Median MFE</span><strong className="is-positive">{fmtPct(evidence.medianMfePct)}</strong></div>
        <div><span>Median MAE</span><strong className="is-negative">{fmtPct(evidence.medianMaePct)}</strong></div>
        <div><span>Winner / loser</span><strong>{fmtRatio(evidence.rewardRiskMedian)}</strong></div>
        <div><span>+5% reached</span><strong>{fmtRate(evidence.hit5RatePct)}</strong></div>
      </div>

      <div className={`cot-lookback-confidence is-${confidence.tone || 'low'}`}>
        <strong>SAMPLE · {confidence.grade || '—'}</strong>
        <span>{confidence.label || 'Sample quality unavailable'}</span>
      </div>

      <div className="cot-lookback-read">
        <div className="cot-lookback-kicker">HISTORICAL EDGE · {lookback.primaryHorizon}W</div>
        <strong>{read.label}</strong>
        <p>{read.text}</p>
        <p>{timing} Typical favourable excursion was {fmtPct(evidence.medianMfePct)} versus {fmtPct(evidence.medianMaePct)} adverse.</p>
      </div>

      <div className="cot-lookback-table" role="table" aria-label="Forward price outcomes">
        <div className="cot-lookback-row cot-lookback-row--head" role="row"><span>Horizon</span><span>Hit</span><span>Median</span><span>MFE / MAE</span><span>N</span></div>
        {lookback.horizons.map((horizon) => {
          const outcome = lookback.outcomes?.[horizon] || {}
          return (
            <div className="cot-lookback-row" role="row" key={horizon}>
              <strong>{horizon}W</strong>
              <span>{fmtRate(outcome.hitRatePct)}</span>
              <span className={outcome.medianReturnPct > 0 ? 'is-positive' : outcome.medianReturnPct < 0 ? 'is-negative' : ''}>{fmtPct(outcome.medianReturnPct)}</span>
              <span>{fmtPct(outcome.medianMfePct)} / {fmtPct(outcome.medianMaePct)}</span>
              <span>{outcome.sampleCount ?? 0}</span>
            </div>
          )
        })}
      </div>

      <p className="cot-lookback-note">Current setup strength and historical evidence are separate. Consecutive matching weeks count as one episode · seasonality excluded · point-in-time only through {week?.date || 'the selected week'}.</p>
    </section>
  )
}
