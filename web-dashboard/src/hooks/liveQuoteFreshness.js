/** Live quote age thresholds — quote is stale when older than this. */
export const LIVE_QUOTE_STALE_MS = 60_000

/** Poll interval for live_quotes_latest.json (cache-busted). */
export const LIVE_QUOTE_POLL_MS = 45_000

export function parseIsoMs(iso) {
  if (iso == null || iso === '') return null
  const ms = Date.parse(String(iso))
  return Number.isFinite(ms) ? ms : null
}

export function formatAgeMs(ageMs) {
  if (ageMs == null || !Number.isFinite(ageMs)) return '—'
  if (ageMs < 1000) return `${Math.round(ageMs)}ms`
  if (ageMs < 60_000) return `${Math.round(ageMs / 1000)}s`
  return `${Math.round(ageMs / 60_000)}m`
}

/**
 * Freshness from OANDA quote timestamp (preferred) or export generated_at.
 */
export function getLiveQuoteFreshness(quote, doc) {
  const quoteAsOfMs = parseIsoMs(quote?.live_price_as_of)
  const docGeneratedAtMs = parseIsoMs(doc?.generated_at)
  const referenceMs = quoteAsOfMs ?? docGeneratedAtMs
  const ageMs = referenceMs != null ? Math.max(0, Date.now() - referenceMs) : null
  const isStale = referenceMs == null ? true : ageMs > LIVE_QUOTE_STALE_MS

  return {
    quoteAsOf: quote?.live_price_as_of ?? null,
    quoteAsOfMs,
    docGeneratedAt: doc?.generated_at ?? null,
    docGeneratedAtMs,
    ageMs,
    isStale,
    staleReason:
      referenceMs == null
        ? 'missing timestamp'
        : isStale
          ? `quote age ${formatAgeMs(ageMs)} > ${LIVE_QUOTE_STALE_MS / 1000}s`
          : null,
  }
}
