/**
 * CurrentPriceStreamStore — single shared client for the Phase 2 Current Price Service.
 *
 * Connects to the FastAPI WebSocket (ws://localhost:8787/ws/prices via Vite proxy
 * at /ws/prices in development). One connection for the whole dashboard.
 *
 * Exposes:
 * - connectionState: connected | reconnecting | disconnected
 * - prices[internal_key]: CurrentPrice payload from the backend
 * - weeklyCandles[internal_key]: active weekly candle
 * - stream meta (generated_at, stream health)
 *
 * Does NOT open a WebSocket per chart. Does NOT fetch live_quotes_latest.json.
 */

const DEFAULT_HTTP_PRICES = '/api/prices'
const DEFAULT_HTTP_WEEKLY = '/api/weekly-candles'
const DEFAULT_WS_PATH = '/ws/prices'

const BACKOFF_BASE_MS = 500
const BACKOFF_MAX_MS = 15_000
const SNAPSHOT_FETCH_TIMEOUT_MS = 8_000

const _listeners = new Set()
let _prices = Object.create(null)
let _weeklyCandles = Object.create(null)
let _streamMeta = null
let _generatedAt = null
let _connectionState = 'disconnected' // connected | reconnecting | disconnected
let _lastError = null
let _ws = null
let _reconnectTimer = null
let _backoffMs = BACKOFF_BASE_MS
let _subscriberCount = 0
let _intentionalClose = false
let _snapshotCache = null
let _snapshotCacheKey = ''
let _started = false

function emit() {
  _snapshotCache = null
  _snapshotCacheKey = ''
  for (const fn of _listeners) {
    try {
      fn()
    } catch {
      // Ignore listener errors so one bad subscriber cannot kill the store.
    }
  }
}

function resolveWsUrl() {
  const explicit = (import.meta.env?.VITE_CURRENT_PRICE_WS || '').trim()
  if (explicit) return explicit

  if (typeof window === 'undefined') return DEFAULT_WS_PATH

  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${proto}//${window.location.host}${DEFAULT_WS_PATH}`
}

function resolveHttpUrl(path) {
  const base = (import.meta.env?.VITE_CURRENT_PRICE_URL || '').trim().replace(/\/$/, '')
  return base ? `${base}${path}` : path
}

function parseAgeSeconds(asOf) {
  if (!asOf) return null
  const ms = Date.parse(String(asOf).replace(/(\.\d{3})\d+/, '$1'))
  if (!Number.isFinite(ms)) return null
  return Math.max(0, (Date.now() - ms) / 1000)
}

function normalizePrice(raw) {
  if (!raw || typeof raw !== 'object') return null
  const mid = Number(raw.mid ?? raw.current_price)
  const bid = Number(raw.bid)
  const ask = Number(raw.ask)
  return {
    internalKey: raw.internal_key ?? null,
    displayName: raw.display_name ?? null,
    provider: raw.provider ?? null,
    providerSymbol: raw.provider_symbol ?? null,
    assetType: raw.asset_type ?? null,
    currency: raw.currency ?? null,
    pricePrecision:
      raw.price_precision != null && Number.isFinite(Number(raw.price_precision))
        ? Number(raw.price_precision)
        : null,
    timestamp: raw.timestamp ?? null,
    bid: Number.isFinite(bid) ? bid : null,
    ask: Number.isFinite(ask) ? ask : null,
    mid: Number.isFinite(mid) ? mid : null,
    currentPrice:
      raw.current_price != null && Number.isFinite(Number(raw.current_price))
        ? Number(raw.current_price)
        : Number.isFinite(mid)
          ? mid
          : null,
    status: String(raw.status || 'UNAVAILABLE').toUpperCase(),
    ageSeconds:
      raw.age_seconds != null && Number.isFinite(Number(raw.age_seconds))
        ? Number(raw.age_seconds)
        : parseAgeSeconds(raw.timestamp),
    tradeable: raw.tradeable !== false,
    fallbackClose:
      raw.fallback_close != null && Number.isFinite(Number(raw.fallback_close))
        ? Number(raw.fallback_close)
        : null,
    fallbackSource: raw.fallback_source ?? null,
    note: raw.note ?? null,
  }
}

function normalizeWeeklyCandle(raw) {
  if (!raw || typeof raw !== 'object') return null
  const open = Number(raw.open)
  const high = Number(raw.high)
  const low = Number(raw.low)
  const close = Number(raw.close)
  if (![open, high, low, close].every(Number.isFinite)) return null
  const date = String(raw.date || '').slice(0, 10)
  if (!date) return null
  const time = Math.floor(Date.parse(`${date}T12:00:00Z`) / 1000)
  if (!Number.isFinite(time)) return null
  return {
    date,
    time,
    open,
    high,
    low,
    close,
    source: raw.source ?? null,
    updatedAt: raw.updated_at ?? null,
    live: raw.live === true,
  }
}

function applySnapshot(payload) {
  if (!payload || typeof payload !== 'object') return

  if (payload.prices && typeof payload.prices === 'object') {
    const next = Object.create(null)
    for (const [key, row] of Object.entries(payload.prices)) {
      const n = normalizePrice(row)
      if (n) next[key] = n
    }
    if (Object.keys(next).length > 0) {
      // Full snapshot replaces; partial frames merge so brief gaps keep last quote.
      _prices = payload.type === 'snapshot' ? next : { ..._prices, ...next }
    }
  }

  if (payload.weekly_candles && typeof payload.weekly_candles === 'object') {
    const next = Object.create(null)
    for (const [key, row] of Object.entries(payload.weekly_candles)) {
      const n = normalizeWeeklyCandle(row)
      if (n) next[key] = n
    }
    if (Object.keys(next).length > 0) {
      _weeklyCandles =
        payload.type === 'snapshot' ? next : { ..._weeklyCandles, ...next }
    }
  }

  if (payload.stream) _streamMeta = payload.stream
  if (payload.generated_at) _generatedAt = payload.generated_at
  emit()
}

async function fetchInitialSnapshot() {
  const ctrl = typeof AbortController !== 'undefined' ? new AbortController() : null
  const timer =
    ctrl &&
    window.setTimeout(() => {
      try {
        ctrl.abort()
      } catch {
        /* ignore */
      }
    }, SNAPSHOT_FETCH_TIMEOUT_MS)

  try {
    const [pricesResp, weeklyResp] = await Promise.all([
      fetch(resolveHttpUrl(DEFAULT_HTTP_PRICES), {
        cache: 'no-store',
        signal: ctrl?.signal,
      }),
      fetch(resolveHttpUrl(DEFAULT_HTTP_WEEKLY), {
        cache: 'no-store',
        signal: ctrl?.signal,
      }),
    ])

    const pricesDoc = pricesResp.ok ? await pricesResp.json() : null
    const weeklyDoc = weeklyResp.ok ? await weeklyResp.json() : null

    applySnapshot({
      type: 'snapshot',
      generated_at: pricesDoc?.generated_at ?? weeklyDoc?.generated_at ?? null,
      prices: pricesDoc?.prices ?? {},
      weekly_candles: weeklyDoc?.weekly_candles ?? {},
      stream: { any_connected: pricesResp.ok },
    })
    _lastError = null
    return true
  } catch (err) {
    _lastError = String(err?.message || err)
    emit()
    return false
  } finally {
    if (timer) window.clearTimeout(timer)
  }
}

function clearReconnectTimer() {
  if (_reconnectTimer != null) {
    window.clearTimeout(_reconnectTimer)
    _reconnectTimer = null
  }
}

function scheduleReconnect() {
  if (_intentionalClose || _subscriberCount <= 0) return
  clearReconnectTimer()
  _connectionState = 'reconnecting'
  emit()
  const delay = _backoffMs
  _backoffMs = Math.min(_backoffMs * 2, BACKOFF_MAX_MS)
  _reconnectTimer = window.setTimeout(() => {
    _reconnectTimer = null
    openSocket()
  }, delay)
}

function openSocket() {
  if (typeof window === 'undefined') return
  if (_ws && (_ws.readyState === WebSocket.OPEN || _ws.readyState === WebSocket.CONNECTING)) {
    return
  }

  _intentionalClose = false
  const url = resolveWsUrl()

  try {
    _ws = new WebSocket(url)
  } catch (err) {
    _lastError = String(err?.message || err)
    scheduleReconnect()
    return
  }

  _ws.onopen = () => {
    _connectionState = 'connected'
    _backoffMs = BACKOFF_BASE_MS
    _lastError = null
    emit()
  }

  _ws.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data)
      applySnapshot(payload)
    } catch {
      // Ignore malformed frames without crashing.
    }
  }

  _ws.onerror = () => {
    _lastError = 'WebSocket error'
  }

  _ws.onclose = () => {
    _ws = null
    if (_intentionalClose || _subscriberCount <= 0) {
      _connectionState = 'disconnected'
      emit()
      return
    }
    scheduleReconnect()
  }
}

function start() {
  if (_started) return
  _started = true
  fetchInitialSnapshot().finally(() => {
    if (_subscriberCount > 0 && !_intentionalClose) openSocket()
  })
}

function stop() {
  _intentionalClose = true
  clearReconnectTimer()
  if (_ws) {
    try {
      _ws.close()
    } catch {
      /* ignore */
    }
    _ws = null
  }
  _connectionState = 'disconnected'
  _started = false
  emit()
}

function displayStatus(internalKey) {
  if (_connectionState === 'reconnecting') return 'RECONNECTING'
  if (_connectionState === 'disconnected') return 'BACKEND OFFLINE'

  const price = _prices[internalKey]
  if (!price) return 'UNAVAILABLE'

  const backendStatus = String(price.status || 'UNAVAILABLE').toUpperCase()

  // LIVE only when WS is connected AND backend reports LIVE.
  if (backendStatus === 'LIVE') {
    if (_connectionState !== 'connected') return 'RECONNECTING'
    return 'LIVE'
  }

  return backendStatus
}

export const CurrentPriceStreamStore = {
  STORE_NAME: 'CurrentPriceStreamStore',

  subscribe(listener) {
    _listeners.add(listener)
    _subscriberCount += 1

    if (_subscriberCount === 1) {
      start()
    }

    return () => {
      _listeners.delete(listener)
      _subscriberCount = Math.max(0, _subscriberCount - 1)
      if (_subscriberCount === 0) {
        stop()
      }
    }
  },

  getSnapshot() {
    const key = [
      _connectionState,
      _generatedAt ?? '',
      Object.keys(_prices).length,
      Object.keys(_weeklyCandles).length,
      _lastError ?? '',
      _streamMeta?.connected ?? '',
      _streamMeta?.last_message_age_seconds ?? '',
    ].join('|')

    if (_snapshotCache && _snapshotCacheKey === key) return _snapshotCache

    _snapshotCacheKey = key
    _snapshotCache = {
      connectionState: _connectionState,
      connected: _connectionState === 'connected',
      reconnecting: _connectionState === 'reconnecting',
      disconnected: _connectionState === 'disconnected',
      prices: _prices,
      weeklyCandles: _weeklyCandles,
      streamMeta: _streamMeta,
      generatedAt: _generatedAt,
      lastError: _lastError,
      subscriberCount: _subscriberCount,
    }
    return _snapshotCache
  },

  getPrice(internalKey) {
    if (!internalKey) return null
    return _prices[internalKey] ?? null
  },

  getWeeklyCandle(internalKey) {
    if (!internalKey) return null
    return _weeklyCandles[internalKey] ?? null
  },

  getDisplayStatus(internalKey) {
    return displayStatus(internalKey)
  },

  getConnectionState() {
    return _connectionState
  },

  /** Test / recovery helper — force a reconnect without dropping cached quotes. */
  reconnect() {
    if (_ws) {
      try {
        _ws.close()
      } catch {
        /* ignore */
      }
      _ws = null
    }
    _backoffMs = BACKOFF_BASE_MS
    if (_subscriberCount > 0) openSocket()
  },

  clearCache() {
    _prices = Object.create(null)
    _weeklyCandles = Object.create(null)
    _streamMeta = null
    _generatedAt = null
    emit()
  },
}

export default CurrentPriceStreamStore
