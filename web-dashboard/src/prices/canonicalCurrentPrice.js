/**
 * Canonical current-price helpers for every UI surface.
 *
 * CURRENT PRICE = LivePriceStore / CurrentPriceStreamStore only.
 * Valuation model spot_price is never treated as live market price.
 */

import { LivePriceStore } from './stores/LivePriceStore.js'
import { useLivePrice } from './usePriceStores.js'

export function formatCanonicalPrice(value, digits = 2) {
  if (value == null || !Number.isFinite(Number(value))) return '—'
  return Number(value).toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })
}

/**
 * Resolve the display price for an instrument from the canonical store.
 * Prefer mid when LIVE/STALE; otherwise currentPrice (may be FALLBACK close).
 */
export function resolveCanonicalDisplayPrice(quote, status) {
  if (!quote) {
    return { price: null, status: status || 'UNAVAILABLE', label: 'Unavailable' }
  }
  const st = String(status || quote.status || 'UNAVAILABLE').toUpperCase()
  const mid = quote.mid != null && Number.isFinite(Number(quote.mid)) ? Number(quote.mid) : null
  const fallback =
    quote.fallbackClose != null && Number.isFinite(Number(quote.fallbackClose))
      ? Number(quote.fallbackClose)
      : null
  const current =
    quote.currentPrice != null && Number.isFinite(Number(quote.currentPrice))
      ? Number(quote.currentPrice)
      : null

  if (st === 'LIVE' || st === 'STALE') {
    return {
      price: mid ?? current,
      status: st,
      label: st === 'LIVE' ? 'Live' : 'Stale',
      bid: quote.bid,
      ask: quote.ask,
      asOf: quote.asOf,
      provider: quote.provider,
      providerSymbol: quote.providerSymbol,
    }
  }
  if (st === 'FALLBACK' || st === 'RECONNECTING' || st === 'BACKEND OFFLINE') {
    return {
      price: current ?? fallback ?? mid,
      status: st,
      label:
        st === 'FALLBACK'
          ? 'Fallback close'
          : st === 'RECONNECTING'
            ? 'Reconnecting'
            : 'Backend offline',
      bid: quote.bid,
      ask: quote.ask,
      asOf: quote.asOf,
      provider: quote.provider,
      providerSymbol: quote.providerSymbol,
      note: quote.fallbackSource || null,
    }
  }
  return {
    price: null,
    status: 'UNAVAILABLE',
    label: 'Unavailable',
    bid: null,
    ask: null,
    asOf: null,
  }
}

/** React hook: canonical current price for one market. */
export function useCanonicalCurrentPrice(marketId) {
  const live = useLivePrice(marketId)
  const quote = live?.quote ?? LivePriceStore.getQuote(marketId)
  const status = live?.status ?? LivePriceStore.getStatus(marketId)
  const display = resolveCanonicalDisplayPrice(quote, status)
  return {
    ...display,
    quote,
    freshness: live?.freshness,
    connectionState: live?.connectionState,
    loaded: live?.loaded,
  }
}
