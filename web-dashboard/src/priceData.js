/**
 * Canonical price store — source-agnostic (OANDA / Alpha Vantage hidden).
 * Shape per instrument: price, daily, weekly, history, range_52w
 */

const PRICES_URL = '/data/prices_latest.json'

let _cache = null
let _loadPromise = null

export async function loadPriceStore() {
  if (_cache) return _cache
  if (_loadPromise) return _loadPromise
  _loadPromise = fetch(PRICES_URL)
    .then((r) => {
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      return r.json()
    })
    .then((doc) => {
      _cache = doc && typeof doc === 'object' ? doc : { instruments: {} }
      return _cache
    })
    .catch(() => {
      _cache = { instruments: {}, summary: {} }
      return _cache
    })
  return _loadPromise
}

export function clearPriceStoreCache() {
  _cache = null
  _loadPromise = null
}

/** @returns {{ price, daily, weekly, history, range_52w, error } | null} */
export function getInstrumentPrices(store, instrumentId) {
  if (!store?.instruments) return null
  const rec = store.instruments[instrumentId]
  if (!rec) return null
  return {
    price: rec.price ?? null,
    daily: Array.isArray(rec.daily) ? rec.daily : [],
    weekly: Array.isArray(rec.weekly) ? rec.weekly : [],
    history: rec.history ?? null,
    range_52w: rec.range_52w ?? rec.history?.range_52w ?? null,
    error: rec.error ?? null,
  }
}

export function fmtPrice(v, digits = 4) {
  if (v === null || v === undefined || v === '') return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  if (Math.abs(n) >= 1000) return n.toFixed(2)
  if (Math.abs(n) >= 10) return n.toFixed(3)
  return n.toFixed(digits)
}
