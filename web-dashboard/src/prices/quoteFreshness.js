export const QUOTE_STALE_MS = 60_000

export function quoteAgeMs(timestamp, now = Date.now()) {
  if (!timestamp) return null
  const ms = Date.parse(String(timestamp))
  if (!Number.isFinite(ms) || ms > now + 5_000) return null
  return Math.max(0, now - ms)
}

export function quoteDisplayStatus(price, connectionState, now = Date.now()) {
  if (connectionState === 'reconnecting') return 'RECONNECTING'
  if (connectionState !== 'connected') return 'BACKEND OFFLINE'
  if (!price) return 'UNAVAILABLE'
  const status = String(price.status || 'UNAVAILABLE').toUpperCase()
  if (status !== 'LIVE') return status
  const age = quoteAgeMs(price.timestamp, now)
  return age == null || age > QUOTE_STALE_MS ? 'STALE' : 'LIVE'
}

export function nullableNumber(value) {
  if (value == null || value === '') return null
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}
