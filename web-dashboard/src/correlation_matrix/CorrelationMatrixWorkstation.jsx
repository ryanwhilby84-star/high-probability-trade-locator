import React from 'react'
import './correlationMatrix.css'

const FREQUENCIES = [
  { id: 'daily', label: 'Daily' },
  { id: 'weekly', label: 'Weekly' },
]

const LOOKBACKS = [20, 60, 120, 252]

function corrColor(v) {
  if (v == null || !Number.isFinite(Number(v))) return 'transparent'
  const x = Math.max(-1, Math.min(1, Number(v)))
  if (Math.abs(x) < 0.05) return 'rgba(148, 163, 184, 0.25)'
  if (x > 0) {
    const a = 0.2 + 0.75 * x
    return `rgba(34, 197, 94, ${a.toFixed(3)})`
  }
  const a = 0.2 + 0.75 * -x
  return `rgba(185, 28, 28, ${a.toFixed(3)})`
}

function fmtCorr(v) {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  return Number(v).toFixed(2)
}

function shortLabel(id) {
  if (!id) return ''
  const s = String(id)
  if (s.length <= 14) return s
  return s.slice(0, 12) + '…'
}

export function CorrelationMatrixWorkstation({
  payload,
  frequency,
  lookback,
  onFrequency,
  onLookback,
  loading,
  error,
}) {
  const instruments = payload?.instruments || []
  const matrix = payload?.matrix || []

  return (
    <div className="cmx-body">
      <div className="cmx-controls">
        <div className="cmx-group" role="group" aria-label="Return frequency">
          <span className="cmx-label">Returns</span>
          {FREQUENCIES.map((f) => (
            <button
              key={f.id}
              type="button"
              className={`cmx-btn${frequency === f.id ? ' is-active' : ''}`}
              onClick={() => onFrequency?.(f.id)}
            >
              {f.label}
            </button>
          ))}
        </div>
        <div className="cmx-group" role="group" aria-label="Lookback">
          <span className="cmx-label">Lookback</span>
          {LOOKBACKS.map((lb) => (
            <button
              key={lb}
              type="button"
              className={`cmx-btn${Number(lookback) === lb ? ' is-active' : ''}`}
              onClick={() => onLookback?.(lb)}
            >
              {lb}
            </button>
          ))}
        </div>
        <div className="cmx-meta">
          <span>Method: Pearson</span>
          <span>
            Universe: {payload?.universe_size ?? instruments.length ?? '—'}
          </span>
          <span className="cmx-scale">
            <i className="cmx-swatch cmx-swatch-neg" /> Neg
            <i className="cmx-swatch cmx-swatch-zero" /> ~0
            <i className="cmx-swatch cmx-swatch-pos" /> Pos
          </span>
        </div>
      </div>

      {loading ? (
        <p className="cmx-muted">Loading correlation matrix…</p>
      ) : null}

      {error ? (
        <div className="cmx-error">
          <h2>Correlation matrix unavailable</h2>
          <p>{error}</p>
        </div>
      ) : null}

      {!loading && !error && payload?.status === 'ok' ? (
        <div className="cmx-scroll">
          <table className="cmx-table" data-cmx-method={payload.method}>
            <thead>
              <tr>
                <th className="cmx-corner" />
                {instruments.map((id) => (
                  <th key={id} title={id}>
                    {shortLabel(id)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {instruments.map((rowId, i) => (
                <tr key={rowId}>
                  <th title={rowId}>{shortLabel(rowId)}</th>
                  {(matrix[i] || []).map((v, j) => (
                    <td
                      key={`${i}-${j}`}
                      style={{ background: corrColor(v) }}
                      title={`${rowId} × ${instruments[j]}: ${fmtCorr(v)}`}
                    >
                      {fmtCorr(v)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}

      {payload?.warnings?.length ? (
        <details className="cmx-warnings">
          <summary>
            Data-quality warnings ({payload.warnings.length})
          </summary>
          <ul>
            {payload.warnings.slice(0, 40).map((w) => (
              <li key={w}>{w}</li>
            ))}
          </ul>
        </details>
      ) : null}
    </div>
  )
}
