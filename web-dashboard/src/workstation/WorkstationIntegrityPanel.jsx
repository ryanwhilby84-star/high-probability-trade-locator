import React from 'react'
import { navigateToScanner } from '../routing.js'

/**
 * Controlled integrity / API failure panel — never blank the shell.
 */
export function WorkstationIntegrityPanel({
  instrumentId,
  reportDate = null,
  stage = 'Derived COT',
  missingFields = [],
  message = 'Derived COT statistics are incomplete for this instrument.',
  onRetry = null,
}) {
  const fields = Array.isArray(missingFields) ? missingFields : []
  return (
    <div className="cot-ws-integrity-panel" role="alert" data-status="integrity_error">
      <strong>DATA INTEGRITY FAILURE</strong>
      <p>{message}</p>
      <p>
        Instrument: <code>{instrumentId || '—'}</code>
      </p>
      <p>
        Report week: <code>{reportDate || '—'}</code>
      </p>
      <p>Stage: {stage}</p>
      <p>Missing fields:</p>
      <ul>
        {(fields.length ? fields : ['required derived fields']).map((f) => (
          <li key={String(f)}>{String(f)}</li>
        ))}
      </ul>
      <div className="cot-ws-integrity-actions">
        {onRetry ? (
          <button type="button" className="cot-ws-page-btn" onClick={onRetry}>
            Retry
          </button>
        ) : null}
        <button type="button" className="cot-ws-page-btn" onClick={navigateToScanner}>
          Back to Scanner
        </button>
      </div>
    </div>
  )
}

export function WorkstationRenderErrorPanel({ instrumentId, error, onRetry = null }) {
  const ref = React.useMemo(
    () => `ws-err-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`,
    [],
  )
  React.useEffect(() => {
    if (import.meta.env.DEV) {
      console.error('[workstation] WORKSTATION RENDERING ERROR', instrumentId, ref, error)
    }
  }, [instrumentId, error, ref])

  return (
    <div className="cot-ws-integrity-panel cot-ws-integrity-panel--render" role="alert">
      <strong>WORKSTATION RENDERING ERROR</strong>
      <p>The workstation could not be rendered.</p>
      <p>
        Instrument: <code>{instrumentId || '—'}</code>
      </p>
      <p>
        Error reference: <code>{ref}</code>
      </p>
      <p className="cot-ws-status-detail">{String(error?.message || error || 'unknown')}</p>
      <div className="cot-ws-integrity-actions">
        {onRetry ? (
          <button type="button" className="cot-ws-page-btn" onClick={onRetry}>
            Retry
          </button>
        ) : null}
        <button type="button" className="cot-ws-page-btn" onClick={navigateToScanner}>
          Back to Scanner
        </button>
      </div>
    </div>
  )
}
