/**
 * Offline fallback to canonical prices_latest.json when Current Price Service is down.
 * Always disclosed as FALLBACK — never presented as LIVE.
 */

let _doc = null
let _loadedAtMs = null
let _loadPromise = null
const MAX_CACHE_MS = 60_000

export async function loadPricesLatestFallback(force = false) {
  const now = Date.now()
  if (!force && _doc && _loadedAtMs && now - _loadedAtMs < MAX_CACHE_MS) {
    return _doc
  }
  if (_loadPromise && !force) return _loadPromise
  _loadPromise = fetch(`/data/prices_latest.json?v=${now}`, { cache: 'no-store' })
    .then(async (res) => {
      if (!res.ok) throw new Error(`prices_latest HTTP ${res.status}`)
      _doc = await res.json()
      _loadedAtMs = Date.now()
      return _doc
    })
    .catch(() => {
      _doc = _doc || { instruments: {} }
      return _doc
    })
    .finally(() => {
      _loadPromise = null
    })
  return _loadPromise
}

export function getPricesLatestFallbackQuote(marketId) {
  const rec = (_doc?.instruments || {})[marketId]
  if (!rec) return null
  const snap = rec.price || {}
  const mid = snap.mid != null && Number.isFinite(Number(snap.mid)) ? Number(snap.mid) : null
  const daily = Array.isArray(rec.daily) ? rec.daily : []
  const lastDaily = daily.length ? daily[daily.length - 1] : null
  const forming = rec.forming_daily || null
  const close =
    mid ??
    (forming?.close != null ? Number(forming.close) : null) ??
    (lastDaily?.close != null ? Number(lastDaily.close) : null)
  if (close == null || !Number.isFinite(close)) return null
  const scale = rec.price_scale || {}
  return {
    instrumentId: marketId,
    symbol: scale.symbol || null,
    bid: snap.bid != null ? Number(snap.bid) : null,
    ask: snap.ask != null ? Number(snap.ask) : null,
    mid: mid,
    source: scale.source && scale.symbol ? `${scale.source}:${scale.symbol}` : 'prices_latest',
    asOf: snap.as_of || forming?.date || lastDaily?.date || null,
    fetchOk: true,
    fetchError: null,
    pricePrecision: null,
    status: 'FALLBACK',
    ageSeconds: null,
    provider: scale.source || 'price_store',
    providerSymbol: scale.symbol || null,
    currentPrice: close,
    fallbackClose: close,
    fallbackSource: mid != null ? 'prices_latest.price' : forming ? 'prices_latest.forming_daily' : 'prices_latest.daily',
    latestCompletedDaily: lastDaily
      ? { date: lastDaily.date, close: lastDaily.close }
      : null,
    formingDaily: forming,
  }
}
