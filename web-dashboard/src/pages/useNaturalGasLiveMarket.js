/**
 * Reactive Natural Gas market-price subscription for the Valuation Workstation.
 *
 * Priority while the page is open:
 *  1. WebSocket LIVE quotes from Current Price Service (:8787)
 *  2. POLLING `/api/ng-live-price` (fresh OANDA REST) every 30s when WS is down
 *  3. One-shot SNAPSHOT / STALE last-known (never treated as live)
 *
 * Does not rebuild valuation or mutate historical series.
 */

import React from 'react'

import { useLivePrice } from '../prices/usePriceStores.js'
import { CurrentPriceStreamStore } from '../prices/stores/CurrentPriceStreamStore.js'
import {
  LIVE_QUOTE_STALE_MS,
  UPDATE_MODE,
  applyQuoteToLiveState,
  buildHeartbeat,
  computeLiveDeviationPct,
  resolveUpdateMode,
} from './naturalGasValuationWorkstationLive.js'

const MARKET = 'Natural Gas / NG'
const POLL_MS = 30_000
const HEARTBEAT_MS = 1_000
const WS_RESUME_MS = 45_000
const NG_LIVE_PRICE_URL = '/api/ng-live-price'
const MAX_PROOF_LOG = 8

async function fetchNgPollingQuote() {
  const res = await fetch(`${NG_LIVE_PRICE_URL}?t=${Date.now()}`, { cache: 'no-store' })
  if (!res.ok) throw new Error(`ng-live-price HTTP ${res.status}`)
  const body = await res.json()
  if (!body?.ok || body.mid == null || !Number.isFinite(Number(body.mid))) {
    throw new Error(body?.error || 'invalid ng-live-price payload')
  }
  return {
    price: Number(body.mid),
    bid: body.bid != null ? Number(body.bid) : null,
    ask: body.ask != null ? Number(body.ask) : null,
    timestamp: body.as_of || body.fetched_at || new Date().toISOString(),
    source: 'OANDA REST poll · NATGAS_USD',
    source_type: 'polling',
    provider_symbol: body.provider_symbol || 'NATGAS_USD',
    receivedAtMs: Date.now(),
  }
}

function streamToLiveQuote(streamPrice, connected) {
  if (!streamPrice) return null
  const mid =
    streamPrice.mid != null && Number.isFinite(Number(streamPrice.mid))
      ? Number(streamPrice.mid)
      : null
  if (mid == null) return null
  const status = String(streamPrice.status || '').toUpperCase()
  if (!(connected && status === 'LIVE') && status !== 'STALE' && status !== 'LIVE') {
    // Allow STALE stream quotes to keep last-known until polling replaces them.
    if (mid == null) return null
  }
  return {
    price: mid,
    bid: streamPrice.bid ?? null,
    ask: streamPrice.ask ?? null,
    timestamp: streamPrice.timestamp || null,
    source: `WebSocket · ${streamPrice.provider || 'oanda'} · ${streamPrice.providerSymbol || 'NATGAS_USD'}`,
    source_type: connected && status === 'LIVE' ? 'websocket' : 'stream_stale',
    provider_symbol: streamPrice.providerSymbol || 'NATGAS_USD',
    status,
    ageSeconds: streamPrice.ageSeconds ?? null,
    receivedAtMs: Date.now(),
  }
}

/**
 * @param {object} opts
 * @param {number|null} opts.physicalFairValue tip-stable fair value
 * @param {boolean} opts.lockedHistory when true, inspector lock must survive quote ticks
 */
export function useNaturalGasLiveMarket({
  physicalFairValue = null,
  lockedHistory = false,
  initialSnapshot = null,
} = {}) {
  const liveHook = useLivePrice(MARKET)
  const [pollingQuote, setPollingQuote] = React.useState(null)
  const [pollError, setPollError] = React.useState(null)
  const [nowMs, setNowMs] = React.useState(() => Date.now())
  const [proofLog, setProofLog] = React.useState([])
  const lastPriceRef = React.useRef(null)
  const lockedHistoryRef = React.useRef(lockedHistory)
  lockedHistoryRef.current = lockedHistory
  const fairRef = React.useRef(physicalFairValue)
  fairRef.current = physicalFairValue

  const snapshotQuote = React.useMemo(() => {
    const mid =
      initialSnapshot?.mid != null && Number.isFinite(Number(initialSnapshot.mid))
        ? Number(initialSnapshot.mid)
        : initialSnapshot?.price != null && Number.isFinite(Number(initialSnapshot.price))
          ? Number(initialSnapshot.price)
          : null
    if (mid == null) return null
    return {
      price: mid,
      timestamp: initialSnapshot.as_of || initialSnapshot.asOf || initialSnapshot.timestamp || null,
      source: initialSnapshot.source || 'OANDA snapshot · static tip (not live)',
      source_type: 'snapshot',
      receivedAtMs: null,
    }
  }, [initialSnapshot])

  // Heartbeat clock — updates age display without resetting charts.
  React.useEffect(() => {
    const id = window.setInterval(() => setNowMs(Date.now()), HEARTBEAT_MS)
    return () => window.clearInterval(id)
  }, [])

  const connected = Boolean(liveHook?.connected)
  const streamQuote = React.useMemo(
    () => streamToLiveQuote(liveHook?.streamPrice, connected),
    [liveHook?.streamPrice, connected],
  )
  const streamIsLive =
    connected &&
    streamQuote != null &&
    String(liveHook?.streamPrice?.status || liveHook?.status || '').toUpperCase() === 'LIVE'

  // Polling fallback — fresh OANDA REST, not static JSON.
  React.useEffect(() => {
    let cancelled = false
    let inFlight = false

    const poll = async () => {
      if (cancelled || inFlight) return
      // Prefer LIVE stream; still poll when stream is not LIVE.
      if (streamIsLive) return
      inFlight = true
      try {
        const q = await fetchNgPollingQuote()
        if (cancelled) return
        setPollingQuote(q)
        setPollError(null)
      } catch (err) {
        if (!cancelled) setPollError(String(err?.message || err))
      } finally {
        inFlight = false
      }
    }

    // Immediate poll when not live so the card is not stuck on static JSON.
    poll()
    const id = window.setInterval(poll, POLL_MS)
    return () => {
      cancelled = true
      window.clearInterval(id)
    }
  }, [streamIsLive])

  // Attempt WS resume while polling so LIVE restores automatically.
  React.useEffect(() => {
    if (streamIsLive) return undefined
    const id = window.setInterval(() => {
      try {
        CurrentPriceStreamStore.reconnect()
      } catch {
        /* ignore */
      }
    }, WS_RESUME_MS)
    return () => window.clearInterval(id)
  }, [streamIsLive])

  const activeQuote = streamIsLive
    ? streamQuote
    : pollingQuote || streamQuote || snapshotQuote

  const updateMode = resolveUpdateMode({
    streamIsLive,
    hasPollingQuote: pollingQuote != null && !streamIsLive,
    quote: activeQuote,
    nowMs,
    staleMs: LIVE_QUOTE_STALE_MS,
  })

  // Prefer local receive time for poll freshness; provider as_of can lag wall-clock.
  const ageMs =
    activeQuote?.receivedAtMs != null
      ? Math.max(0, nowMs - activeQuote.receivedAtMs)
      : activeQuote?.timestamp != null
        ? Math.max(
            0,
            nowMs -
              (Date.parse(String(activeQuote.timestamp).replace(/(\.\d{3})\d+/, '$1')) || nowMs),
          )
        : null

  const liveState = React.useMemo(
    () =>
      applyQuoteToLiveState({
        marketPrice: activeQuote?.price ?? null,
        physicalFairValue,
        updateMode,
        source: activeQuote?.source || 'Unavailable',
        sourceType: activeQuote?.source_type || 'none',
        timestamp: activeQuote?.timestamp || null,
        ageMs,
      }),
    [activeQuote, physicalFairValue, updateMode, ageMs],
  )

  // Proof log: record successive price changes while the page stays open.
  React.useEffect(() => {
    const price = activeQuote?.price
    if (price == null || !Number.isFinite(Number(price))) return
    if (lastPriceRef.current != null && Number(lastPriceRef.current) === Number(price)) return
    // Historical lock must not be overwritten — we only log card updates.
    if (lockedHistoryRef.current) {
      /* inspector stays locked; card still updates */
    }
    lastPriceRef.current = price
    const fair = fairRef.current
    const entry = {
      timestamp: activeQuote.timestamp || new Date(nowMs).toISOString(),
      price: Number(price),
      fair_value: fair,
      deviation: computeLiveDeviationPct(price, fair),
      source_mode: updateMode,
    }
    setProofLog((prev) => [...prev.slice(-(MAX_PROOF_LOG - 1)), entry])
    if (typeof window !== 'undefined') {
      window.__NGVW_LIVE_PROOF__ = {
        updates: [...(window.__NGVW_LIVE_PROOF__?.updates || []), entry].slice(-MAX_PROOF_LOG),
        locked_history: lockedHistoryRef.current,
      }
    }
  }, [activeQuote?.price, activeQuote?.timestamp, updateMode, nowMs])

  const heartbeat = buildHeartbeat({
    updateMode,
    ageMs,
    connectionState: liveHook?.connectionState || CurrentPriceStreamStore.getConnectionState(),
    reconnectAttempts: CurrentPriceStreamStore.getReconnectAttempts?.() ?? 0,
    lastError: liveHook?.refreshError || CurrentPriceStreamStore.getLastError?.() || pollError,
  })

  return {
    market: MARKET,
    activeQuote,
    updateMode,
    liveState,
    heartbeat,
    proofLog,
    streamIsLive,
    connected,
    pollError,
    reconnectAttempts: CurrentPriceStreamStore.getReconnectAttempts?.() ?? 0,
    // Stable zoom contract: quote ticks never expose a range-reset API.
    quoteUpdatesDoNotResetZoom: true,
  }
}

export default useNaturalGasLiveMarket
