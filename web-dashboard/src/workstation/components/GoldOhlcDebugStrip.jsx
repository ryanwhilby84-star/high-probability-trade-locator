import React from 'react'

import { fmtPrice } from '../../priceData.js'

/**
 * Temporary Gold price-source debug strip — remove after verification.
 */
export function GoldOhlcDebugStrip({ debug }) {
  if (!debug) return null

  const last12 = debug.consumedBars || []
  const lastRow = last12[last12.length - 1]

  return (
    <div
      className="workstation-gold-ohlc-debug"
      style={{
        fontFamily: 'ui-monospace, monospace',
        fontSize: '11px',
        lineHeight: 1.45,
        padding: '8px 10px',
        margin: '0 0 8px',
        background: '#1a2332',
        color: '#c8d4e8',
        border: '1px solid #3d5168',
        borderRadius: '4px',
      }}
    >
      <div>
        <strong>Gold OHLC debug</strong> · JSON: <code>{debug.jsonPath}</code> · key:{' '}
        <code>{debug.instrumentKey}</code>
        {debug.exportGeneratedAt ? (
          <>
            {' '}
            · export: <code>{debug.exportGeneratedAt}</code>
          </>
        ) : null}
        {debug.fetchUrl ? (
          <>
            {' '}
            · fetched: <code>{debug.fetchUrl}</code>
          </>
        ) : null}
      </div>
      <div>
        source: <code>{debug.priceSource || '—'}</code> · canonical:{' '}
        <code>{debug.canonicalSymbol || '—'}</code> · resolvedFrom:{' '}
        <code>{debug.resolvedFrom || '—'}</code>
      </div>
      <div>
        last OHLC: <code>{debug.lastOhlcDate || '—'}</code> close{' '}
        <code>{debug.lastOhlcClose != null ? fmtPrice(debug.lastOhlcClose, 2) : '—'}</code> · selected
        COT week: <code>{debug.selectedCotWeek || '—'}</code> · matched OHLC week:{' '}
        <code>{debug.matchedOhlcWeek || '—'}</code>
        {debug.matchedOhlcClose != null ? (
          <>
            {' '}
            close <code>{fmtPrice(debug.matchedOhlcClose, 2)}</code>
          </>
        ) : null}
      </div>
      <div>
        chart tooltip close:{' '}
        <code>{debug.tooltipClose != null ? fmtPrice(debug.tooltipClose, 2) : '—'}</code> · visible
        bars: <code>{debug.visibleBarCount ?? 0}</code>
      </div>
      {last12.length ? (
        <details style={{ marginTop: 4 }}>
          <summary>Final {last12.length} consumed OHLC rows</summary>
          <pre style={{ margin: '4px 0 0', whiteSpace: 'pre-wrap' }}>
            {last12
              .map((b) => `${b.date} C=${b.close != null ? fmtPrice(b.close, 2) : '—'}`)
              .join('\n')}
          </pre>
          {lastRow ? (
            <div>
              last consumed: {lastRow.date} close {fmtPrice(lastRow.close, 2)}
            </div>
          ) : null}
        </details>
      ) : null}
    </div>
  )
}
