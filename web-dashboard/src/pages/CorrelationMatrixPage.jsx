import React from 'react'

import { CorrelationMatrixWorkstation } from '../correlation_matrix/CorrelationMatrixWorkstation.jsx'
import { navigateToScanner } from '../routing.js'
import '../correlation_matrix/correlationMatrix.css'

/**
 * Phase 1 Correlation Matrix Workstation.
 * Controls: return frequency + lookback. Display: colour matrix only.
 */
export function CorrelationMatrixPage() {
  const [frequency, setFrequency] = React.useState('daily')
  const [lookback, setLookback] = React.useState(60)
  const [payload, setPayload] = React.useState(null)
  const [loading, setLoading] = React.useState(true)
  const [error, setError] = React.useState(null)

  React.useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    const url =
      `/api/correlation-matrix?frequency=${encodeURIComponent(frequency)}` +
      `&lookback=${encodeURIComponent(String(lookback))}`
    fetch(url, { cache: 'no-store' })
      .then(async (r) => {
        let body = null
        try {
          body = await r.json()
        } catch {
          body = null
        }
        if (cancelled) return
        if (!body) {
          setError('Invalid response from correlation matrix API.')
          setPayload(null)
          return
        }
        setPayload(body)
        if (body.status !== 'ok') {
          setError(body.message || body.error || 'Correlation matrix failed.')
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err?.message || 'Fetch failed')
          setPayload(null)
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [frequency, lookback])

  return (
    <div className="cmx-page">
      <header className="cmx-topbar">
        <div>
          <button type="button" className="cmx-btn" onClick={navigateToScanner}>
            Scanner
          </button>
        </div>
        <div>
          <h1 className="cmx-title">Correlation Matrix</h1>
          <p className="cmx-sub">
            Phase 1 — Pearson correlation on percentage returns · statistical
            engine only
          </p>
        </div>
        <div />
      </header>
      <CorrelationMatrixWorkstation
        payload={payload}
        frequency={frequency}
        lookback={lookback}
        onFrequency={setFrequency}
        onLookback={setLookback}
        loading={loading}
        error={error}
      />
    </div>
  )
}
