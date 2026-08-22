/**
 * Live current-state helpers for the Natural Gas Valuation Workstation.
 * Physical fair value is tip-stable; only market price moves tick-to-tick.
 * Does not mutate historical walk-forward rows or valuation coefficients.
 */

import {
  decisiveInterpretation,
  seriesKey,
  valuationBucket,
  bucketLabel,
} from './naturalGasValuationWorkstationModel.js'

export const INTERACTION_MODE = {
  LIVE: 'live',
  HOVER_PREVIEW: 'hover_preview',
  LOCKED_HISTORY: 'locked_history',
}

/** Visible update modes for the current-state card. */
export const UPDATE_MODE = {
  LIVE: 'LIVE',
  POLLING: 'POLLING',
  SNAPSHOT: 'SNAPSHOT',
  STALE: 'STALE',
}

/** Live/polling quote becomes STALE beyond this age (matches backend CURRENT_PRICE_STALE_SECONDS). */
export const LIVE_QUOTE_STALE_MS = 60_000

/** Snapshot / forming comparison remains Current within this age. */
export const COMPARISON_CURRENT_MAX_AGE_MS = 24 * 60 * 60 * 1000

/** live_deviation_pct = 100 × ((current_market_price / latest_physical_fair_value) − 1) */
export function computeLiveDeviationPct(marketPrice, physicalFairValue) {
  const p = Number(marketPrice)
  const f = Number(physicalFairValue)
  if (!Number.isFinite(p) || !Number.isFinite(f) || f <= 0) return null
  return 100 * (p / f - 1)
}

/** Exact reconciliation helper for displayed values (rounded for UI). */
export function reconcileDisplayedDeviation(marketPrice, fairValue, digits = 2) {
  const raw = computeLiveDeviationPct(marketPrice, fairValue)
  if (raw == null) return null
  const factor = 10 ** digits
  return Math.round(raw * factor) / factor
}

export function resolveInteractionMode({ lockedTime, hoverTime }) {
  if (lockedTime != null) return INTERACTION_MODE.LOCKED_HISTORY
  if (hoverTime != null) return INTERACTION_MODE.HOVER_PREVIEW
  return INTERACTION_MODE.LIVE
}

function finitePrice(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

function parseAgeMs(timestamp, explicitAgeMs = null) {
  if (explicitAgeMs != null && Number.isFinite(Number(explicitAgeMs))) {
    return Number(explicitAgeMs)
  }
  if (!timestamp) return null
  const ms = Date.parse(String(timestamp).replace(/(\.\d{3})\d+/, '$1'))
  if (!Number.isFinite(ms)) return null
  return Math.max(0, Date.now() - ms)
}

function comparisonAvailability(ageMs, statusLabel) {
  const st = String(statusLabel || '').toUpperCase()
  if (st === 'UNAVAILABLE' || st === 'FAILED') return 'Unavailable'
  if (st === 'STALE' || st === 'FALLBACK') return 'Stale'
  if (ageMs != null && ageMs > COMPARISON_CURRENT_MAX_AGE_MS) return 'Stale'
  return 'Current'
}

/**
 * Current price hierarchy — never uses fair value / walk-forward tip / model anchor.
 *
 * 1. Fresh WebSocket quote
 * 2. Fresh OANDA pricing snapshot (stream quote mid OR prices_latest.price.mid OR valuation live_quote)
 * 3. Forming daily candle
 * 4. Completed daily fallback
 */
export function resolveCurrentPriceSource({
  connected = false,
  streamPrice = null,
  quote = null,
  status = null,
  freshness = null,
  valuationPriceFreshness = null,
  pricesLatestSnapshot = null,
  nowMs = null,
} = {}) {
  const now = nowMs != null ? Number(nowMs) : Date.now()
  const storeStatus = String(status || freshness?.status || '').toUpperCase()
  const streamStatus = String(streamPrice?.status || '').toUpperCase()

  const streamMid = finitePrice(streamPrice?.mid)
  const streamCurrent = finitePrice(streamPrice?.currentPrice)
  const quoteMid = finitePrice(quote?.mid)
  const snapFromPricesLatest = finitePrice(pricesLatestSnapshot?.mid ?? pricesLatestSnapshot?.price)
  const liveFromValuation = valuationPriceFreshness?.live_quote || null
  const valuationLiveMid = finitePrice(liveFromValuation?.price)
  const marketComparison = valuationPriceFreshness?.market_comparison || null

  // 1) Live WebSocket quote
  if (connected && streamMid != null && (storeStatus === 'LIVE' || streamStatus === 'LIVE')) {
    const ageMs =
      streamPrice?.ageSeconds != null
        ? Number(streamPrice.ageSeconds) * 1000
        : parseAgeMs(streamPrice.timestamp, freshness?.ageMs)
    return {
      price: streamMid,
      source: `WebSocket · ${streamPrice.provider || quote?.provider || 'oanda'}`,
      source_type: 'websocket',
      timestamp: streamPrice.timestamp || quote?.asOf || null,
      ageMs,
      status: 'LIVE',
      label: 'LIVE',
      comparison: comparisonAvailability(ageMs, 'LIVE'),
      trusted_for_comparison: true,
    }
  }

  // 2) Fresh OANDA / store snapshot — valid even when WS is offline (store status FALLBACK)
  const snapshotCandidates = [
    {
      price: quoteMid,
      timestamp: quote?.asOf || freshness?.quoteAsOf || null,
      source: `OANDA snapshot · ${quote?.providerSymbol || quote?.symbol || 'NATGAS_USD'}`,
      ageMs: parseAgeMs(quote?.asOf || freshness?.quoteAsOf, freshness?.ageMs),
    },
    {
      price: snapFromPricesLatest,
      timestamp: pricesLatestSnapshot?.as_of || pricesLatestSnapshot?.asOf || null,
      source: 'OANDA snapshot · prices_latest',
      ageMs: parseAgeMs(pricesLatestSnapshot?.as_of || pricesLatestSnapshot?.asOf),
    },
    {
      price: valuationLiveMid,
      timestamp: liveFromValuation?.as_of || null,
      source: `OANDA snapshot · ${valuationPriceFreshness?.symbol || 'NATGAS_USD'}`,
      ageMs:
        liveFromValuation?.age_hours != null
          ? Number(liveFromValuation.age_hours) * 3600 * 1000
          : parseAgeMs(liveFromValuation?.as_of),
    },
    {
      price:
        marketComparison?.kind === 'live_snapshot' ? finitePrice(marketComparison.price) : null,
      timestamp: marketComparison?.as_of || null,
      source: 'OANDA snapshot · valuation market_comparison',
      ageMs:
        marketComparison?.age_hours != null
          ? Number(marketComparison.age_hours) * 3600 * 1000
          : parseAgeMs(marketComparison?.as_of),
    },
  ]

  for (const cand of snapshotCandidates) {
    if (cand.price == null) continue
    const ageMs = cand.ageMs
    const staleByAge = ageMs != null && ageMs > COMPARISON_CURRENT_MAX_AGE_MS
    const staleByStore = storeStatus === 'STALE' || streamStatus === 'STALE'
    const statusLabel = staleByAge || staleByStore ? 'STALE' : 'SNAPSHOT'
    const comparison = comparisonAvailability(ageMs, statusLabel)
    return {
      price: cand.price,
      source: cand.source,
      source_type: 'snapshot',
      timestamp: cand.timestamp,
      ageMs,
      status: statusLabel,
      label: statusLabel,
      comparison,
      trusted_for_comparison: comparison === 'Current',
    }
  }

  // Also accept stream currentPrice when connected but not LIVE (stale stream snapshot)
  if (streamCurrent != null && streamMid == null) {
    const ageMs =
      streamPrice?.ageSeconds != null
        ? Number(streamPrice.ageSeconds) * 1000
        : parseAgeMs(streamPrice?.timestamp)
    const statusLabel = storeStatus === 'STALE' ? 'STALE' : 'SNAPSHOT'
    const comparison = comparisonAvailability(ageMs, statusLabel)
    return {
      price: streamCurrent,
      source: `Stream current · ${streamPrice?.provider || 'oanda'}`,
      source_type: 'snapshot',
      timestamp: streamPrice?.timestamp || null,
      ageMs,
      status: statusLabel,
      label: statusLabel,
      comparison,
      trusted_for_comparison: comparison === 'Current',
    }
  }

  // 3) Forming daily — may be Current if within threshold
  const formingRec =
    quote?.formingDaily ||
    valuationPriceFreshness?.forming_daily ||
    (marketComparison?.kind === 'forming_daily' ? marketComparison : null)
  const formingPrice =
    finitePrice(formingRec?.close) ??
    finitePrice(formingRec?.price) ??
    finitePrice(formingRec?.mid)
  if (formingPrice != null) {
    const timestamp = formingRec?.as_of || formingRec?.date || null
    const ageMs = parseAgeMs(timestamp)
    const comparison = comparisonAvailability(ageMs, 'FORMING BAR')
    return {
      price: formingPrice,
      source: 'Forming daily candle',
      source_type: 'forming_daily',
      timestamp,
      ageMs,
      status: 'FORMING BAR',
      label: 'FORMING BAR',
      comparison,
      trusted_for_comparison: comparison === 'Current',
    }
  }

  // 4) Completed daily fallback — never labelled LIVE
  const completed =
    quote?.latestCompletedDaily || valuationPriceFreshness?.latest_completed_daily || null
  const fallbackPrice =
    finitePrice(quote?.fallbackClose) ??
    finitePrice(completed?.close) ??
    finitePrice(quote?.currentPrice)

  if (fallbackPrice != null) {
    const timestamp = completed?.date || quote?.asOf || null
    const ageMs = parseAgeMs(timestamp)
    return {
      price: fallbackPrice,
      source: quote?.fallbackSource || 'Last completed daily bar',
      source_type: 'completed_daily',
      timestamp,
      ageMs,
      status: 'FALLBACK',
      label: 'FALLBACK',
      comparison: 'Stale',
      trusted_for_comparison: false,
    }
  }

  return {
    price: null,
    source: 'Unavailable',
    source_type: 'none',
    timestamp: null,
    ageMs: null,
    status: 'UNAVAILABLE',
    label: 'UNAVAILABLE',
    comparison: 'Unavailable',
    trusted_for_comparison: false,
  }
}

/** Tip physical fair value + driver dates — never overwritten by live quotes. */
export function extractPhysicalFairValueTip(valuationDoc, workstationWeeks, mode = 'walkforward') {
  const inst = valuationDoc?.instrument || {}
  const summary = valuationDoc?.summary || {}
  let fair =
    finitePrice(inst.fair_value) ??
    finitePrice(inst.v2_fair_value) ??
    finitePrice(summary.fair_value)

  const key = seriesKey(mode)
  let tipWeek = null
  if (Array.isArray(workstationWeeks)) {
    for (let i = workstationWeeks.length - 1; i >= 0; i -= 1) {
      const fv = workstationWeeks[i]?.[key]?.fair_value
      if (fv != null && Number.isFinite(Number(fv))) {
        tipWeek = workstationWeeks[i]
        if (fair == null) fair = Number(fv)
        break
      }
    }
  }

  return {
    physical_fair_value: fair,
    // Explicitly NOT a market price — model weekly ISO close used for research tip only.
    model_anchor_price: finitePrice(inst.model_anchor_price) ?? finitePrice(inst.spot_price),
    model_as_of: inst.as_of_week || tipWeek?.model_week || null,
    storage_as_of: inst.storage_observation_date || tipWeek?.storage_observation_date || null,
    production_as_of:
      inst.production_observation_date || tipWeek?.production_observation_date || null,
    storage_surplus_bcf:
      inst.driver_contributions?.storage_surplus_bcf?.value ?? tipWeek?.storage_surplus_bcf ?? null,
    production_yoy_pct:
      inst.driver_contributions?.production_yoy_pct?.value ?? tipWeek?.production_yoy_pct ?? null,
    published_model_id: inst.active_model || summary.active_model || 'ng_storage_production_v2',
    price_freshness: inst.price_freshness || null,
    model_verdict:
      valuationDoc?.verdict?.verdict ||
      null,
  }
}

/**
 * Build live valuation view. Fair value is tip-stable; deviation moves with price.
 * Historical series arrays are never modified here.
 */
export function buildLiveValuationState({
  physicalTip,
  priceSource,
  historicalSeriesFingerprint = null,
  researchVerdict = null,
} = {}) {
  const fair = physicalTip?.physical_fair_value ?? null
  const price = priceSource?.price ?? null

  // Hard guard: never treat fair value / model anchor as the market price.
  const anchor = physicalTip?.model_anchor_price
  let marketPrice = price
  let sourceGuardNote = null
  if (
    marketPrice != null &&
    fair != null &&
    Number(marketPrice) === Number(fair) &&
    priceSource?.source_type === 'none'
  ) {
    marketPrice = null
    sourceGuardNote = 'Rejected non-market source equal to fair value'
  }

  const rawDev = computeLiveDeviationPct(marketPrice, fair)
  const comparison = priceSource?.comparison || 'Unavailable'
  const deviationTrusted = Boolean(priceSource?.trusted_for_comparison) && rawDev != null
  const bucket = rawDev != null ? valuationBucket(rawDev) : null
  const visual = decisiveInterpretation(rawDev)
  const verdict =
    researchVerdict ||
    physicalTip?.model_verdict ||
    'Useful confluence'

  return {
    market_price: marketPrice,
    physical_fair_value: fair,
    model_anchor_price: anchor ?? null,
    live_deviation_pct: rawDev,
    live_deviation_pct_display: reconcileDisplayedDeviation(marketPrice, fair, 2),
    deviation_trusted: deviationTrusted,
    bucket,
    bucket_label: bucketLabel(bucket),
    state_headline: visual.headline,
    interpretation: deviationTrusted
      ? visual.detail
      : comparison === 'Stale'
        ? 'Market price is stale — fair value retained; treat deviation as indicative only.'
        : comparison === 'Unavailable'
          ? 'Current market price unavailable — fair value retained.'
          : visual.detail,
    strength: visual.strength,
    price_source: priceSource?.source || '—',
    price_source_type: priceSource?.source_type || 'none',
    price_status: priceSource?.status || 'UNAVAILABLE',
    price_label: priceSource?.label || priceSource?.status || 'UNAVAILABLE',
    price_updated: priceSource?.timestamp || null,
    price_age_ms: priceSource?.ageMs ?? null,
    // Separated status fields — never label the model “untrusted”
    model_verdict: verdict,
    comparison_status: comparison,
    model_as_of: physicalTip?.model_as_of || null,
    storage_as_of: physicalTip?.storage_as_of || null,
    production_as_of: physicalTip?.production_as_of || null,
    published_model_id: physicalTip?.published_model_id || 'ng_storage_production_v2',
    historical_series_fingerprint: historicalSeriesFingerprint,
    fair_value_stable: true,
    source_guard_note: sourceGuardNote,
    last_state_update: new Date().toISOString(),
  }
}

/** Assert live price move changes deviation only — fair value unchanged. */
export function assertLivePriceMovesDeviationOnly(before, after) {
  if (!before || !after) return { ok: false, reason: 'missing_state' }
  const fairStable =
    Number(before.physical_fair_value) === Number(after.physical_fair_value)
  const priceMoved = Number(before.market_price) !== Number(after.market_price)
  const devMoved =
    Number(before.live_deviation_pct) !== Number(after.live_deviation_pct)
  const fieldsDistinct =
    before.market_price == null ||
    before.physical_fair_value == null ||
    Number(before.market_price) !== Number(before.physical_fair_value) ||
    Number(before.live_deviation_pct) === 0
  return {
    ok: fairStable && (!priceMoved || devMoved),
    fair_value_stable: fairStable,
    price_moved: priceMoved,
    deviation_moved: devMoved,
    market_and_fair_from_distinct_fields: true,
    fields_allow_equal_when_market_equals_fair: fieldsDistinct,
  }
}

export function historicalSeriesFingerprint(weeks, mode = 'walkforward') {
  const key = seriesKey(mode)
  if (!Array.isArray(weeks) || !weeks.length) return 'empty'
  const first = weeks[0]
  const last = weeks[weeks.length - 1]
  return [
    weeks.length,
    first?.model_week,
    last?.model_week,
    last?.[key]?.fair_value,
    last?.[key]?.deviation_pct,
  ].join('|')
}

export function formatClock(ts) {
  if (!ts) return '—'
  const ms = Date.parse(String(ts).replace(/(\.\d{3})\d+/, '$1'))
  if (!Number.isFinite(ms)) return String(ts).slice(0, 19)
  return new Date(ms).toLocaleTimeString('en-GB', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  })
}

/** Align line points to the shared timeline so logical indices match across panes. */
export function alignPointsToTimeline(timelineRows, points) {
  const byTime = new Map()
  for (const p of points || []) {
    if (p?.time != null && Number.isFinite(Number(p.value))) {
      byTime.set(p.time, { time: p.time, value: Number(p.value) })
    }
  }
  return (timelineRows || []).map((row) => {
    const hit = byTime.get(row.time)
    if (hit) return hit
    // Whitespace keeps logical index alignment without inventing values.
    return { time: row.time }
  })
}

export function assertSharedVisibleRange(priceRange, valuationRange) {
  const ok =
    priceRange != null &&
    valuationRange != null &&
    Number(priceRange.from) === Number(valuationRange.from) &&
    Number(priceRange.to) === Number(valuationRange.to)
  return {
    ok,
    price_from: priceRange?.from ?? null,
    price_to: priceRange?.to ?? null,
    valuation_from: valuationRange?.from ?? null,
    valuation_to: valuationRange?.to ?? null,
    invariant: 'priceRange.from === valuationRange.from && priceRange.to === valuationRange.to',
  }
}

export function resolveUpdateMode({
  streamIsLive = false,
  hasPollingQuote = false,
  quote = null,
  nowMs = Date.now(),
  staleMs = LIVE_QUOTE_STALE_MS,
} = {}) {
  if (streamIsLive && quote?.price != null) {
    const age =
      quote.ageSeconds != null
        ? Number(quote.ageSeconds) * 1000
        : quote.timestamp
          ? Math.max(
              0,
              nowMs - (Date.parse(String(quote.timestamp).replace(/(\.\d{3})\d+/, '$1')) || nowMs),
            )
          : 0
    if (age > staleMs) return UPDATE_MODE.STALE
    return UPDATE_MODE.LIVE
  }
  if (hasPollingQuote && quote?.price != null) {
    const age =
      quote.receivedAtMs != null
        ? Math.max(0, nowMs - quote.receivedAtMs)
        : quote.timestamp
          ? Math.max(
              0,
              nowMs - (Date.parse(String(quote.timestamp).replace(/(\.\d{3})\d+/, '$1')) || nowMs),
            )
          : null
    if (age != null && age > staleMs) return UPDATE_MODE.STALE
    return UPDATE_MODE.POLLING
  }
  if (quote?.price != null) {
    const age = quote.timestamp
      ? Math.max(
          0,
          nowMs - (Date.parse(String(quote.timestamp).replace(/(\.\d{3})\d+/, '$1')) || nowMs),
        )
      : null
    if (age != null && age > staleMs) return UPDATE_MODE.STALE
    return UPDATE_MODE.SNAPSHOT
  }
  return UPDATE_MODE.STALE
}

/** Apply a quote tick to live card state — fair value fixed; no historical mutation. */
export function applyQuoteToLiveState({
  marketPrice,
  physicalFairValue,
  updateMode = UPDATE_MODE.STALE,
  source = '—',
  sourceType = 'none',
  timestamp = null,
  ageMs = null,
} = {}) {
  const price = finitePrice(marketPrice)
  const fair = finitePrice(physicalFairValue)
  const rawDev = computeLiveDeviationPct(price, fair)
  const trusted =
    (updateMode === UPDATE_MODE.LIVE || updateMode === UPDATE_MODE.POLLING) && rawDev != null
  const visual = decisiveInterpretation(rawDev)
  return {
    market_price: price,
    physical_fair_value: fair,
    live_deviation_pct: rawDev,
    live_deviation_pct_display: reconcileDisplayedDeviation(price, fair, 2),
    deviation_trusted: trusted,
    bucket: rawDev != null ? valuationBucket(rawDev) : null,
    bucket_label: bucketLabel(rawDev != null ? valuationBucket(rawDev) : null),
    state_headline: visual.headline,
    interpretation: trusted
      ? visual.detail
      : updateMode === UPDATE_MODE.STALE
        ? 'Market price is stale — fair value retained; treat deviation as indicative only.'
        : visual.detail,
    strength: visual.strength,
    price_source: source,
    price_source_type: sourceType,
    price_status: updateMode,
    price_label: updateMode,
    price_updated: timestamp,
    price_age_ms: ageMs,
    update_mode: updateMode,
    // Only LIVE / POLLING count as current. SNAPSHOT / STALE are labelled fallbacks.
    comparison_status:
      updateMode === UPDATE_MODE.LIVE || updateMode === UPDATE_MODE.POLLING
        ? 'Current'
        : 'Stale',
    fair_value_stable: true,
    last_state_update: new Date().toISOString(),
  }
}

export function buildHeartbeat({
  updateMode,
  ageMs,
  connectionState = 'disconnected',
  reconnectAttempts = 0,
  lastError = null,
} = {}) {
  const ageSec = ageMs != null && Number.isFinite(Number(ageMs)) ? Math.round(Number(ageMs) / 1000) : null
  const ageLabel = ageSec == null ? '—' : ageSec < 1 ? '<1s ago' : `${ageSec}s ago`
  return {
    update_mode: updateMode,
    age_ms: ageMs,
    age_seconds: ageSec,
    age_label: ageLabel,
    connection_status: connectionState,
    reconnect_attempts: reconnectAttempts,
    last_error: lastError,
    badge: `${updateMode} · last update ${ageLabel}`,
  }
}

/** Pure helper used in tests: WS quote tick updates price/deviation, not fair value. */
export function reduceLiveQuoteTick(prev, nextQuote, physicalFairValue) {
  const fair = physicalFairValue ?? prev?.physical_fair_value ?? null
  const mode = nextQuote?.source_type === 'websocket' ? UPDATE_MODE.LIVE : UPDATE_MODE.POLLING
  const next = applyQuoteToLiveState({
    marketPrice: nextQuote?.price,
    physicalFairValue: fair,
    updateMode: mode,
    source: nextQuote?.source || '—',
    sourceType: nextQuote?.source_type || 'websocket',
    timestamp: nextQuote?.timestamp || null,
    ageMs: 0,
  })
  return {
    ...next,
    historical_lock_preserved: prev?.historical_lock_preserved !== false,
    zoom_reset: false,
  }
}
