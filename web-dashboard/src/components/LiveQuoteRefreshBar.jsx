import React from 'react'

import { formatAgeMs } from '../hooks/liveQuoteFreshness.js'

/**
 * STALE badge + manual refresh + quote timestamps for live OANDA display.
 */
export function LiveQuoteRefreshBar({
  freshness,
  fetchUrl = null,
  fetchedAtMs = null,
  refreshing = false,
  refreshError = null,
  onRefresh,
  compact = false,
}) {
  if (!freshness && !onRefresh) return null

  const quoteTs = freshness?.quoteAsOf
    ? String(freshness.quoteAsOf).slice(0, 19)
    : freshness?.docGeneratedAt
      ? String(freshness.docGeneratedAt).slice(0, 19)
      : '—'

  const browserTs =
    fetchedAtMs != null
      ? new Date(fetchedAtMs).toISOString().slice(0, 19)
      : '—'

  return (
    <div className={`live-quote-bar${compact ? ' live-quote-bar--compact' : ''}`}>
      <div className="live-quote-bar-meta">
        {freshness?.isStale ? (
          <span className="live-quote-stale-badge" title={freshness.staleReason || 'Stale'}>
            STALE
          </span>
        ) : freshness ? (
          <span className="live-quote-fresh-badge">LIVE</span>
        ) : null}
        {!compact ? (
          <>
            <span className="live-quote-bar-ts" title="OANDA quote as-of">
              Quote: {quoteTs}
            </span>
            <span className="live-quote-bar-ts" title="Browser fetch time">
              Loaded: {browserTs}
              {freshness?.ageMs != null ? ` (${formatAgeMs(freshness.ageMs)} ago)` : ''}
            </span>
            {fetchUrl ? (
              <span className="live-quote-bar-url" title={fetchUrl}>
                {fetchUrl.replace(/^https?:\/\/[^/]+/, '')}
              </span>
            ) : null}
          </>
        ) : (
          <span className="live-quote-bar-ts">
            {quoteTs}
            {freshness?.ageMs != null ? ` · ${formatAgeMs(freshness.ageMs)}` : ''}
          </span>
        )}
      </div>
      {onRefresh ? (
        <button
          type="button"
          className="live-quote-refresh-btn"
          onClick={() => onRefresh({ runExport: true })}
          disabled={refreshing}
        >
          {refreshing ? 'Refreshing…' : 'Refresh live quote'}
        </button>
      ) : null}
      {refreshError ? <span className="live-quote-bar-err">{refreshError}</span> : null}
    </div>
  )
}
