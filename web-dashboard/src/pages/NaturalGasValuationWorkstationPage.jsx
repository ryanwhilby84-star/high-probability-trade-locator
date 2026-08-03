import React from 'react'

import { PANEL_IDS } from '../charts/chartTheme.js'
import {
  POSITIONING_DEFAULT_RANGE_ID,
  POSITIONING_RANGE_PRESETS,
  rangePresetById,
} from '../cot/positioningChartMetrics.js'
import { fetchPublicJson } from '../utils/fetchPublicJson.js'
import { useLivePrice } from '../prices/usePriceStores.js'
import {
  navigateToCotWorkstation,
  navigateToInstrument,
  navigateToNaturalGasValuationLive,
  navigateToScanner,
} from '../routing.js'
import { useLinkedChartTimeline } from '../workstation/charts/useLinkedChartTimeline.js'
import { WorkstationChartPane } from '../workstation/charts/WorkstationChartPane.jsx'
import {
  BOTTOM_SERIES,
  DEFAULT_SCALE_MODE,
  DEVIATION_BAND_LINES,
  FOCUS_SCALE_LIMIT,
  SIGN_CONVENTION,
  ZONE_LABELS,
  applyFocusScale,
  assertLinkedVisibleRanges,
  buildBucketStripCells,
  buildDeviationPoints,
  buildFairPoints,
  buildPricePoints,
  buildSharedTimeline,
  deriveBucketEvents,
  findAdjacentIndex,
  formatReportDate,
  inspectorForWeek,
  isoToBarTime,
  returnTone,
} from './naturalGasValuationWorkstationModel.js'
import {
  INTERACTION_MODE,
  alignPointsToTimeline,
  assertSharedVisibleRange,
  buildLiveValuationState,
  extractPhysicalFairValueTip,
  formatClock,
  historicalSeriesFingerprint,
  resolveCurrentPriceSource,
  resolveInteractionMode,
} from './naturalGasValuationWorkstationLive.js'
import './naturalGasValuationWorkstation.css'
import '../workstation/cotWorkstationPage.css'
import '../workstation/cotWorkstation.css'

const MARKET = 'Natural Gas / NG'
const HISTORY_URL = '/data/ng_valuation_workstation_latest.json'
const VALUATION_URL = '/data/natural_gas_valuation_latest.json'

function fmt(v, digits = 3) {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  return Number(v).toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })
}

function fmtSigned(v, digits = 2) {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  const n = Number(v)
  return `${n > 0 ? '+' : ''}${fmt(n, digits)}`
}

function fmtPct(v, digits = 1) {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  return `${fmt(100 * Number(v), digits)}%`
}

/** Memoized strip — ignores hover flicker. */
const BucketStrip = React.memo(function BucketStrip({ cells, selectedTime, onSelect }) {
  return (
    <div className="ngvw-bucket-strip" data-testid="ngvw-bucket-strip" role="list">
      {cells.map((cell) => (
        <button
          key={cell.index}
          type="button"
          role="listitem"
          title={`${cell.week} · ${cell.bucket || 'unavailable'}`}
          className={`ngvw-strip-cell${selectedTime != null && cell.time === selectedTime ? ' is-selected' : ''}`}
          style={{ background: cell.color }}
          onClick={() => cell.time != null && onSelect(cell.time)}
        />
      ))}
    </div>
  )
})

export function NaturalGasValuationWorkstationPage() {
  const [historyDoc, setHistoryDoc] = React.useState(null)
  const [valuationDoc, setValuationDoc] = React.useState(null)
  const [error, setError] = React.useState(null)
  const [lockedTime, setLockedTime] = React.useState(null)
  const [hoverTime, setHoverTime] = React.useState(null)
  const lockedTimeRef = React.useRef(null)
  lockedTimeRef.current = lockedTime
  const hoverRafRef = React.useRef(null)
  const pendingHoverRef = React.useRef(null)
  const rangeInitRef = React.useRef(false)

  const [rangeId, setRangeId] = React.useState(POSITIONING_DEFAULT_RANGE_ID)
  const [bottomSeries, setBottomSeries] = React.useState(BOTTOM_SERIES.deviation)
  const [modelMode, setModelMode] = React.useState('walkforward')
  const [scaleMode, setScaleMode] = React.useState(DEFAULT_SCALE_MODE)
  const [pricesLatestSnapshot, setPricesLatestSnapshot] = React.useState(null)
  const sharedVisibleRangeRef = React.useRef(null)
  const lockedTimeStableRef = React.useRef(null)

  const liveHook = useLivePrice(MARKET)

  React.useEffect(() => {
    let cancelled = false
    const loadCore = () =>
      Promise.all([
        fetchPublicJson(HISTORY_URL),
        fetchPublicJson(VALUATION_URL).catch(() => null),
        fetchPublicJson('/data/prices_latest.json').catch(() => null),
      ]).then(([hist, val, prices]) => {
        if (cancelled) return
        setHistoryDoc(hist)
        setValuationDoc(val)
        const snap = prices?.instruments?.[MARKET]?.price || null
        setPricesLatestSnapshot(snap)
        setError(null)
      })

    loadCore().catch((err) => {
      if (!cancelled) setError(err?.message || String(err))
    })

    // Reactive snapshot path when WebSocket is down — never requires weekly rebuild.
    const pollId = window.setInterval(() => {
      Promise.all([
        fetchPublicJson(VALUATION_URL).catch(() => null),
        fetchPublicJson('/data/prices_latest.json').catch(() => null),
      ]).then(([val, prices]) => {
        if (cancelled) return
        if (val) setValuationDoc(val)
        const snap = prices?.instruments?.[MARKET]?.price || null
        if (snap) setPricesLatestSnapshot(snap)
      })
    }, 15_000)

    return () => {
      cancelled = true
      window.clearInterval(pollId)
      if (hoverRafRef.current != null) cancelAnimationFrame(hoverRafRef.current)
    }
  }, [])

  const weeks = historyDoc?.weeks || []
  const timelineRows = React.useMemo(() => buildSharedTimeline(weeks).timelineRows, [weeks])
  const weekByTime = React.useMemo(() => {
    const m = new Map()
    weeks.forEach((w, index) => {
      const t = isoToBarTime(w.model_week)
      if (t != null) m.set(t, { week: w, index })
    })
    return m
  }, [weeks])

  const fingerprint = React.useMemo(
    () => historicalSeriesFingerprint(weeks, modelMode),
    [weeks, modelMode],
  )

  const physicalTip = React.useMemo(
    () => extractPhysicalFairValueTip(valuationDoc, weeks, modelMode),
    [valuationDoc, weeks, modelMode],
  )

  const priceSource = React.useMemo(
    () =>
      resolveCurrentPriceSource({
        connected: Boolean(liveHook?.connected),
        streamPrice: liveHook?.streamPrice || null,
        quote: liveHook?.quote || null,
        status: liveHook?.status || null,
        freshness: liveHook?.freshness || null,
        valuationPriceFreshness: physicalTip.price_freshness,
        pricesLatestSnapshot,
      }),
    [
      liveHook?.connected,
      liveHook?.streamPrice,
      liveHook?.quote,
      liveHook?.status,
      liveHook?.freshness,
      physicalTip.price_freshness,
      pricesLatestSnapshot,
    ],
  )

  const liveState = React.useMemo(
    () =>
      buildLiveValuationState({
        physicalTip,
        priceSource,
        historicalSeriesFingerprint: fingerprint,
        researchVerdict: historyDoc?.verdict?.verdict || physicalTip.model_verdict,
      }),
    [physicalTip, priceSource, fingerprint, historyDoc?.verdict?.verdict],
  )

  const interactionMode = resolveInteractionMode({ lockedTime, hoverTime })
  // Preserve lock across live quote ticks (price updates must not clear selection).
  lockedTimeStableRef.current = lockedTime

  const cooldown =
    Number(historyDoc?.event_study_walkforward?.cooldown_weeks) > 0
      ? Number(historyDoc.event_study_walkforward.cooldown_weeks)
      : 4
  const events = React.useMemo(
    () => deriveBucketEvents(weeks, modelMode, cooldown),
    [weeks, modelMode, cooldown],
  )
  const allEventIndexes = React.useMemo(() => events.map((e) => e.index), [events])

  const pricePoints = React.useMemo(
    () => alignPointsToTimeline(timelineRows, buildPricePoints(weeks)),
    [timelineRows, weeks],
  )
  const rawDeviation = React.useMemo(
    () => alignPointsToTimeline(timelineRows, buildDeviationPoints(weeks, modelMode)),
    [timelineRows, weeks, modelMode],
  )
  const fairPoints = React.useMemo(
    () => alignPointsToTimeline(timelineRows, buildFairPoints(weeks, modelMode)),
    [timelineRows, weeks, modelMode],
  )
  const scaled = React.useMemo(
    () => applyFocusScale(rawDeviation, scaleMode, FOCUS_SCALE_LIMIT),
    [rawDeviation, scaleMode],
  )
  const valuationPoints =
    bottomSeries === BOTTOM_SERIES.fair ? fairPoints : scaled.displayPoints
  const stripCells = React.useMemo(
    () => buildBucketStripCells(weeks, modelMode),
    [weeks, modelMode],
  )

  const timelineRowsRef = React.useRef(timelineRows)
  timelineRowsRef.current = timelineRows

  const hoverTimeRef = React.useRef(null)
  const scheduleHover = React.useCallback((t) => {
    if (lockedTimeRef.current != null) return
    // No-op when unchanged — avoids inspector / prop churn on every mousemove pixel.
    if (t === hoverTimeRef.current) return
    pendingHoverRef.current = t
    if (hoverRafRef.current != null) return
    hoverRafRef.current = requestAnimationFrame(() => {
      hoverRafRef.current = null
      const next = pendingHoverRef.current
      if (next === hoverTimeRef.current) return
      hoverTimeRef.current = next
      setHoverTime(next)
    })
  }, [])

  const linked = useLinkedChartTimeline({
    timelineRowsRef,
    onCrosshairTime: (t) => scheduleHover(t),
    onCrosshairClear: () => {
      if (lockedTimeRef.current != null) return
      pendingHoverRef.current = null
      if (hoverTimeRef.current == null) return
      hoverTimeRef.current = null
      setHoverTime(null)
    },
  })

  const applyRange = React.useCallback(
    (id) => {
      const preset = rangePresetById(id)
      setRangeId(preset.id === 'all' ? 'all' : preset.id)
      const n = timelineRows.length
      if (!n) return
      if (!preset.weeks) linked.fitAllRows(n, { force: true })
      else linked.showWindow(n, preset.weeks, { force: true })
    },
    [linked, timelineRows.length],
  )

  React.useEffect(() => {
    if (!timelineRows.length || rangeInitRef.current) return
    rangeInitRef.current = true
    applyRange(POSITIONING_DEFAULT_RANGE_ID)
  }, [timelineRows.length, applyRange])

  React.useEffect(() => {
    return linked.subscribeGeometry(() => {
      const state = linked.getViewportState()
      const price = state?.panes?.get?.(PANEL_IDS.price)
      const valuation = state?.panes?.get?.('valuation')
      if (!price?.chart || !valuation?.chart) return
      try {
        const priceRange = price.chart.timeScale().getVisibleLogicalRange()
        const valuationRange = valuation.chart.timeScale().getVisibleLogicalRange()
        const selectedWeek =
          (lockedTime != null ? weekByTime.get(lockedTime) : null)?.week?.model_week || null
        const hard = assertSharedVisibleRange(priceRange, valuationRange)
        const soft = assertLinkedVisibleRanges(priceRange, valuationRange, selectedWeek)
        sharedVisibleRangeRef.current = priceRange
        window.__NGVW_SYNC__ = {
          ...soft,
          ...hard,
          sharedVisibleRange: priceRange,
          sharedSelectedWeek: selectedWeek,
          sharedCrosshairDate: hoverTime || lockedTime,
        }
        if (!hard.ok && import.meta.env?.DEV) {
          console.error('[NGVW] sharedVisibleRange invariant broken', hard)
        }
      } catch {
        /* ignore */
      }
    })
  }, [linked, lockedTime, hoverTime, weekByTime])

  const returnToLive = React.useCallback(() => {
    setLockedTime(null)
    setHoverTime(null)
    hoverTimeRef.current = null
    pendingHoverRef.current = null
    linked.setExternalCrosshair(null)
    applyRange(POSITIONING_DEFAULT_RANGE_ID)
  }, [linked, applyRange])

  React.useEffect(() => {
    const onKey = (e) => {
      if (e.key === 'Escape') returnToLive()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [returnToLive])

  const lockWeekAtTime = React.useCallback(
    (time) => {
      if (time == null) return
      setLockedTime(time)
      setHoverTime(null)
      hoverTimeRef.current = null
      pendingHoverRef.current = null
      linked.setExternalCrosshair(time)
    },
    [linked],
  )

  const selectIndex = React.useCallback(
    (index) => {
      if (index == null || index < 0 || index >= weeks.length) return
      const time = isoToBarTime(weeks[index].model_week)
      if (time != null) lockWeekAtTime(time)
    },
    [weeks, lockWeekAtTime],
  )

  const historyHit =
    interactionMode === INTERACTION_MODE.LOCKED_HISTORY
      ? weekByTime.get(lockedTime)
      : interactionMode === INTERACTION_MODE.HOVER_PREVIEW
        ? weekByTime.get(hoverTime)
        : null

  const inspector = React.useMemo(() => {
    if (!historyHit) return null
    return inspectorForWeek(
      historyHit.week,
      modelMode,
      weeks,
      historyHit.index,
      historyDoc?.bucket_outcomes_walkforward,
    )
  }, [historyHit, modelMode, weeks, historyDoc])

  // Only pin crosshair via React when LOCKED. Hover uses the native linked
  // crosshair path only — re-applying externalCrosshair on every hover tick was
  // a sync feedback loop that flashed both panes.
  const externalCrosshair =
    interactionMode === INTERACTION_MODE.LOCKED_HISTORY ? lockedTime : null

  const currentIndex = historyHit?.index ?? weeks.length - 1
  const paneDate =
    interactionMode === INTERACTION_MODE.LIVE
      ? 'LIVE'
      : inspector?.report_date_label || '—'

  const isDeviation = bottomSeries === BOTTOM_SERIES.deviation
  const coverage = historyDoc?.coverage || {}
  const livePriceForChart =
    interactionMode === INTERACTION_MODE.LIVE && liveState.market_price != null
      ? liveState.market_price
      : null
  const liveDevMarker =
    interactionMode === INTERACTION_MODE.LIVE &&
    isDeviation &&
    liveState.live_deviation_pct != null
      ? liveState.live_deviation_pct
      : null

  const liveTone = !liveState.deviation_trusted
    ? 'stale'
    : liveState.strength?.includes('support')
      ? 'under'
      : liveState.strength?.includes('contradiction')
        ? 'over'
        : 'near'

  return (
    <div
      className={`cot-ws-page ngvw-ws-page mode-${interactionMode}`}
      data-testid="ngvw-page"
      data-interaction-mode={interactionMode}
      data-scale-mode={scaleMode}
      data-price-status={liveState.price_status}
    >
      <header className="cot-ws-page-topbar">
        <div className="cot-ws-page-topbar-left">
          <button type="button" className="cot-ws-page-btn" onClick={navigateToScanner}>
            Scanner
          </button>
          <button
            type="button"
            className="cot-ws-page-btn"
            onClick={() => navigateToInstrument(MARKET)}
          >
            ← {MARKET}
          </button>
          <button type="button" className="cot-ws-page-btn" onClick={navigateToNaturalGasValuationLive}>
            Live tip card
          </button>
        </div>
        <div className="cot-ws-page-topbar-center">
          <span className="cot-ws-page-title">{MARKET}</span>
          <span className="cot-ws-page-subtitle">Valuation Workstation</span>
        </div>
        <div className="cot-ws-page-topbar-right">
          <span
            className={`ngvw-status-badge ngvw-status-badge--${String(liveState.price_label || 'STALE').toLowerCase().replace(/\s+/g, '-')}`}
            data-testid="ngvw-price-badge"
          >
            {liveState.price_label}
          </span>
          <button
            type="button"
            className="cot-ws-page-btn"
            data-testid="ngvw-return-live"
            onClick={returnToLive}
          >
            Return to Live
          </button>
          <button
            type="button"
            className="cot-ws-page-btn"
            onClick={() => navigateToCotWorkstation(MARKET)}
          >
            COT workstation
          </button>
        </div>
      </header>

      <main className="cot-ws-page-body ngvw-ws-body">
        {error ? (
          <div className="cot-ws-status cot-ws-status--error">Failed to load: {error}</div>
        ) : null}
        {!historyDoc && !error ? (
          <div className="cot-ws-status" role="status">
            Loading historical valuation…
          </div>
        ) : null}

        {historyDoc ? (
          <div className="ngvw-workstation">
            <header className="ngvw-toolbar">
              <div className="ngvw-toolbar-meta">
                <strong>
                  {coverage.first_week} → {coverage.last_week}
                </strong>
                <span>
                  Mode: {interactionMode.replace(/_/g, ' ')} · walk-forward n=
                  {coverage.n_walkforward_fair_values}
                </span>
              </div>
              <div className="cot-ws-range-toggles" role="group" aria-label="Chart range">
                <button type="button" className="cot-ws-range-btn" onClick={() => applyRange('all')}>
                  All
                </button>
                <button
                  type="button"
                  className="cot-ws-range-btn"
                  onClick={() => applyRange(POSITIONING_DEFAULT_RANGE_ID)}
                >
                  Home
                </button>
                {POSITIONING_RANGE_PRESETS.filter((p) => p.id !== 'all').map((p) => (
                  <button
                    key={p.id}
                    type="button"
                    className={`cot-ws-range-btn${rangeId === p.id ? ' active' : ''}`}
                    onClick={() => applyRange(p.id)}
                  >
                    {p.label}
                  </button>
                ))}
              </div>
            </header>

            <div className="ngvw-controls">
              <div className="ngvw-btn-group">
                <button
                  type="button"
                  className={bottomSeries === BOTTOM_SERIES.fair ? 'is-active' : ''}
                  onClick={() => setBottomSeries(BOTTOM_SERIES.fair)}
                >
                  Fair Value
                </button>
                <button
                  type="button"
                  className={bottomSeries === BOTTOM_SERIES.deviation ? 'is-active' : ''}
                  onClick={() => setBottomSeries(BOTTOM_SERIES.deviation)}
                >
                  Valuation Deviation
                </button>
              </div>
              <div className="ngvw-btn-group">
                <button
                  type="button"
                  className={modelMode === 'walkforward' ? 'is-active' : ''}
                  onClick={() => setModelMode('walkforward')}
                >
                  Walk-Forward
                </button>
                <button
                  type="button"
                  className={modelMode === 'frozen' ? 'is-active' : ''}
                  onClick={() => setModelMode('frozen')}
                >
                  Frozen Diagnostic
                </button>
              </div>
              <div className="ngvw-btn-group" data-testid="ngvw-scale-toggle">
                <button
                  type="button"
                  className={scaleMode === 'focus' ? 'is-active' : ''}
                  onClick={() => setScaleMode('focus')}
                >
                  Focus scale
                </button>
                <button
                  type="button"
                  className={scaleMode === 'full' ? 'is-active' : ''}
                  onClick={() => setScaleMode('full')}
                >
                  Full scale
                </button>
              </div>
              <div className="ngvw-nav-controls">
                <button
                  type="button"
                  className="ngvw-nav-btn"
                  onClick={() => selectIndex(Math.max(0, currentIndex - 1))}
                >
                  ← Week
                </button>
                <button
                  type="button"
                  className="ngvw-nav-btn"
                  onClick={() => selectIndex(Math.min(weeks.length - 1, currentIndex + 1))}
                >
                  Week →
                </button>
                <button
                  type="button"
                  className="ngvw-nav-btn"
                  onClick={() => selectIndex(findAdjacentIndex(allEventIndexes, currentIndex, -1))}
                >
                  ← Event
                </button>
                <button
                  type="button"
                  className="ngvw-nav-btn"
                  onClick={() => selectIndex(findAdjacentIndex(allEventIndexes, currentIndex, 1))}
                >
                  Event →
                </button>
                {interactionMode !== INTERACTION_MODE.LIVE ? (
                  <button
                    type="button"
                    className="ngvw-nav-btn ngvw-nav-btn--unlock"
                    onClick={returnToLive}
                  >
                    Return to Live
                  </button>
                ) : null}
              </div>
            </div>

            <p className="ngvw-sign-legend" data-testid="ngvw-sign-legend">
              {SIGN_CONVENTION}
            </p>

            <section className="ngvw-section-pane ngvw-section-pane--price">
              <div className="ngvw-pane-title">
                <h2>Weekly Natural Gas Price</h2>
                <span>{paneDate}</span>
              </div>
              <div className="ngvw-plot ngvw-plot--price">
                <WorkstationChartPane
                  key="ngvw-price-pane"
                  panelId={PANEL_IDS.price}
                  mode="line"
                  showTimeAxis={false}
                  lineColor="#f1f5f9"
                  lineWidth={2.25}
                  linePoints={pricePoints}
                  timelineRows={timelineRows}
                  registerPane={linked.registerPane}
                  externalCrosshairTime={externalCrosshair}
                  selectedTime={lockedTime}
                  onChartClick={({ time }) => lockWeekAtTime(time)}
                  hideFloatingLabels
                  livePrice={livePriceForChart}
                />
              </div>
            </section>

            {/* Always-visible LIVE current-state card — never replaced by hover */}
            <section
              className={`ngvw-live-card ngvw-live-card--${liveTone}`}
              data-testid="ngvw-live-card"
            >
              <div className="ngvw-live-card-head">
                <h2>CURRENT NATURAL GAS VALUATION</h2>
                <span className={`ngvw-status-badge ngvw-status-badge--${String(liveState.price_label || 'STALE').toLowerCase().replace(/\s+/g, '-')}`}>
                  {liveState.price_label}
                </span>
              </div>
              <div className="ngvw-hero-metrics">
                <div>
                  <span>Market price</span>
                  <strong data-testid="ngvw-market-price">${fmt(liveState.market_price)}</strong>
                </div>
                <div>
                  <span>Physical fair value</span>
                  <strong data-testid="ngvw-fair-value">${fmt(liveState.physical_fair_value)}</strong>
                </div>
                <div>
                  <span>Valuation deviation</span>
                  <strong className="ngvw-hero-dev" data-testid="ngvw-live-deviation">
                    {liveState.live_deviation_pct_display != null
                      ? `${fmtSigned(liveState.live_deviation_pct_display)}%`
                      : '—'}
                    {!liveState.deviation_trusted && liveState.live_deviation_pct_display != null
                      ? ' · indicative'
                      : ''}
                  </strong>
                </div>
              </div>
              <div className="ngvw-hero-state">
                <strong data-testid="ngvw-state-headline">{liveState.state_headline || '—'}</strong>
                <p>{liveState.interpretation}</p>
              </div>
              <dl className="ngvw-live-meta">
                <div>
                  <dt>Price source</dt>
                  <dd>{liveState.price_source}</dd>
                </div>
                <div>
                  <dt>Price status</dt>
                  <dd data-testid="ngvw-price-status">{liveState.price_status}</dd>
                </div>
                <div>
                  <dt>Current comparison</dt>
                  <dd data-testid="ngvw-comparison-status">{liveState.comparison_status}</dd>
                </div>
                <div>
                  <dt>Model verdict</dt>
                  <dd data-testid="ngvw-model-verdict">{liveState.model_verdict || '—'}</dd>
                </div>
                <div>
                  <dt>Price updated</dt>
                  <dd>{formatClock(liveState.price_updated)}</dd>
                </div>
                <div>
                  <dt>Model as of</dt>
                  <dd>{liveState.model_as_of || '—'}</dd>
                </div>
                <div>
                  <dt>Storage as of</dt>
                  <dd>{liveState.storage_as_of || '—'}</dd>
                </div>
                <div>
                  <dt>Production as of</dt>
                  <dd>{liveState.production_as_of || '—'}</dd>
                </div>
              </dl>
              {interactionMode !== INTERACTION_MODE.LIVE ? (
                <p className="ngvw-live-lock-note">
                  Historical {interactionMode === INTERACTION_MODE.LOCKED_HISTORY ? 'lock' : 'preview'}{' '}
                  active — live quotes still update this card; inspector below shows history.
                </p>
              ) : (
                <p className="ngvw-live-lock-note ngvw-live-lock-note--spacer" aria-hidden="true">
                  &nbsp;
                </p>
              )}
              {import.meta.env?.DEV ? (
                <pre className="ngvw-live-diag" data-testid="ngvw-live-diag">
                  {JSON.stringify(
                    {
                      live_price_received: liveState.market_price,
                      live_price_timestamp: liveState.price_updated,
                      fair_value_used: liveState.physical_fair_value,
                      deviation_calculated: liveState.live_deviation_pct,
                      price_source: liveState.price_source,
                      freshness_status: liveState.price_status,
                      comparison_status: liveState.comparison_status,
                      model_verdict: liveState.model_verdict,
                      last_state_update: liveState.last_state_update,
                      shared_visible_range: sharedVisibleRangeRef.current,
                      locked_time: lockedTimeStableRef.current,
                    },
                    null,
                    2,
                  )}
                </pre>
              ) : null}
            </section>

            <section className="ngvw-section-pane ngvw-section-pane--valuation">
              <div className="ngvw-pane-title">
                <h2>
                  {isDeviation ? 'Valuation Deviation' : 'Model Fair Value'}
                  <em>
                    {modelMode === 'frozen' ? ' · Frozen diagnostic' : ' · Walk-forward history'}
                    {isDeviation && scaleMode === 'focus' ? ` · Focus ±${FOCUS_SCALE_LIMIT}%` : ''}
                  </em>
                </h2>
                <span>{paneDate}</span>
              </div>
              <div
                className={`ngvw-plot ngvw-plot--valuation${isDeviation ? ' has-zones' : ''}`}
                data-testid="ngvw-valuation-plot"
                data-floating-tabs="off"
              >
                {isDeviation && scaleMode === 'focus' ? (
                  <div className="ngvw-zone-layer" data-testid="ngvw-zone-layer" aria-hidden="true">
                    {ZONE_LABELS.map((z) => (
                      <div
                        key={z.id}
                        className={`ngvw-zone ngvw-zone--${z.id}`}
                        style={{ top: z.top, height: z.height }}
                      >
                        <span>{z.text}</span>
                      </div>
                    ))}
                  </div>
                ) : null}
                <WorkstationChartPane
                  key="ngvw-valuation-pane"
                  panelId="valuation"
                  mode="line"
                  showTimeAxis={false}
                  lineColor={modelMode === 'frozen' ? '#fbbf24' : '#38bdf8'}
                  lineWidth={3}
                  linePoints={valuationPoints}
                  timelineRows={timelineRows}
                  registerPane={linked.registerPane}
                  externalCrosshairTime={externalCrosshair}
                  selectedTime={lockedTime}
                  onChartClick={({ time }) => lockWeekAtTime(time)}
                  hideFloatingLabels
                  syncFollower
                  transparentBackground={isDeviation && scaleMode === 'focus'}
                  zeroLine={isDeviation}
                  symmetricZero={isDeviation && scaleMode === 'full'}
                  fixedPriceRange={
                    isDeviation && scaleMode === 'focus'
                      ? { min: -FOCUS_SCALE_LIMIT, max: FOCUS_SCALE_LIMIT }
                      : null
                  }
                  priceLines={isDeviation ? DEVIATION_BAND_LINES : null}
                  overflowMarkers={isDeviation ? scaled.overflowMarkers : null}
                  liveDeviationMarker={liveDevMarker}
                />
              </div>

              <BucketStrip
                cells={stripCells}
                selectedTime={lockedTime}
                onSelect={lockWeekAtTime}
              />
              <div className="ngvw-strip-legend">
                <span className="c-mu">Material under</span>
                <span className="c-u">Under</span>
                <span className="c-n">Near fair</span>
                <span className="c-o">Over</span>
                <span className="c-mo">Material over</span>
              </div>
            </section>

            <section className="ngvw-analysis" data-testid="ngvw-historical-inspector">
              <div className="ngvw-analysis-head">
                <h2>
                  {interactionMode === INTERACTION_MODE.LIVE
                    ? 'Historical inspection'
                    : interactionMode === INTERACTION_MODE.LOCKED_HISTORY
                      ? 'Locked historical week'
                      : 'Hover preview (historical)'}
                </h2>
                {interactionMode !== INTERACTION_MODE.LIVE ? (
                  <button
                    type="button"
                    className="ngvw-nav-btn ngvw-nav-btn--unlock"
                    data-testid="ngvw-unlock"
                    onClick={returnToLive}
                  >
                    Return to Live (Esc)
                  </button>
                ) : (
                  <span className="ngvw-hint">Hover to preview · click to lock history</span>
                )}
              </div>

              {inspector ? (
                <div className="ngvw-cards" data-testid="ngvw-selected-week-card">
                  <article className="ngvw-card">
                    <h3>Valuation</h3>
                    <dl>
                      <div><dt>Selected date</dt><dd>{inspector.report_date_label}</dd></div>
                      <div><dt>Market price</dt><dd>${fmt(inspector.market_price)}</dd></div>
                      <div><dt>Fair value</dt><dd>${fmt(inspector.fair_value)}</dd></div>
                      <div>
                        <dt>Valuation deviation</dt>
                        <dd className="is-large">{fmtSigned(inspector.deviation_pct)}%</dd>
                      </div>
                      <div><dt>State</dt><dd>{inspector.state_headline}</dd></div>
                      <div>
                        <dt>Strength</dt>
                        <dd>{String(inspector.interpretation_strength || '—').replace(/_/g, ' ')}</dd>
                      </div>
                    </dl>
                    <p className="ngvw-card-note">{inspector.interpretation}</p>
                  </article>

                  <article className="ngvw-card">
                    <h3>Why the model said this</h3>
                    <dl>
                      <div>
                        <dt>Storage surplus/deficit</dt>
                        <dd>{fmt(inspector.storage_surplus_bcf, 1)} Bcf</dd>
                      </div>
                      <div>
                        <dt>Storage contribution (log)</dt>
                        <dd>{fmtSigned(inspector.storage_log_contribution, 4)}</dd>
                      </div>
                      <div>
                        <dt>Production YoY</dt>
                        <dd>{fmtSigned(inspector.production_yoy_pct)}%</dd>
                      </div>
                      <div>
                        <dt>Production contribution (log)</dt>
                        <dd>{fmtSigned(inspector.production_log_contribution, 4)}</dd>
                      </div>
                      <div>
                        <dt>Intercept / baseline</dt>
                        <dd>{fmt(inspector.intercept, 4)}</dd>
                      </div>
                      <div>
                        <dt>Contribution reconciliation (log P)</dt>
                        <dd>{fmt(inspector.log_price_recon, 4)}</dd>
                      </div>
                    </dl>
                  </article>

                  <article className="ngvw-card">
                    <h3>What happened afterwards</h3>
                    <div className="ngvw-returns">
                      {[1, 2, 4, 8, 12].map((h) => {
                        const v = inspector.forward_returns?.[h]
                        return (
                          <div key={h} className={`tone-${returnTone(v)}`}>
                            <span>{h}-week return</span>
                            <strong>{fmtSigned(v)}%</strong>
                          </div>
                        )
                      })}
                      <div className={`tone-${returnTone(inspector.mfe)}`}>
                        <span>MFE (12w)</span>
                        <strong>{fmtSigned(inspector.mfe)}%</strong>
                      </div>
                      <div className={`tone-${returnTone(inspector.mae)}`}>
                        <span>MAE (12w)</span>
                        <strong>{fmtSigned(inspector.mae)}%</strong>
                      </div>
                    </div>
                  </article>

                  <article className="ngvw-card">
                    <h3>Historical context</h3>
                    <dl>
                      <div>
                        <dt>Percentile of valuation deviation</dt>
                        <dd>{fmt(inspector.deviation_percentile, 1)}</dd>
                      </div>
                      <div>
                        <dt>Weeks in bucket</dt>
                        <dd>{inspector.weeks_in_bucket ?? '—'}</dd>
                      </div>
                      <div>
                        <dt>Bucket median duration</dt>
                        <dd>
                          {inspector.bucket_median_duration != null
                            ? `${fmt(inspector.bucket_median_duration, 1)} weeks`
                            : '—'}
                        </dd>
                      </div>
                      <div>
                        <dt>Bucket 4w mean return</dt>
                        <dd>{fmtSigned(inspector.bucket_mean_forward_4w)}%</dd>
                      </div>
                      <div>
                        <dt>Bucket 4w hit rate</dt>
                        <dd>{fmtPct(inspector.bucket_hit_rate_4w)}</dd>
                      </div>
                      <div>
                        <dt>Research verdict</dt>
                        <dd>{historyDoc?.verdict?.verdict || '—'}</dd>
                      </div>
                    </dl>
                  </article>
                </div>
              ) : (
                <p className="ngvw-empty">
                  Live mode — current valuation is in the card above. Hover the charts to preview a
                  historical week without changing the live state.
                </p>
              )}
            </section>
          </div>
        ) : null}
      </main>
    </div>
  )
}

export default NaturalGasValuationWorkstationPage
