import React from 'react'
import { allInstrumentIds } from '../instrumentRegistry.js'
import { TRACKED_MARKET_IDS } from '../marketResolution.js'
import './macroIntelligenceWorkstation.css'

function instrumentOptions() {
  const ids = allInstrumentIds()
  const list = ids?.length ? ids : TRACKED_MARKET_IDS
  return [...list]
}

function biasClass(bias) {
  const s = String(bias || '')
  if (s.includes('Bullish')) return 'mi-bias-bull'
  if (s.includes('Bearish')) return 'mi-bias-bear'
  return 'mi-bias-neutral'
}

function fmtUpdated(v) {
  if (v == null || v === '') return '—'
  return String(v)
}

/**
 * Macro Intelligence Workstation — Phase 5 architecture display.
 * Presentation only; no live macro mathematics in React.
 */
export function MacroIntelligenceWorkstation() {
  const instruments = React.useMemo(() => instrumentOptions(), [])
  const [instrumentId, setInstrumentId] = React.useState(
    () => (instruments.includes('Gold') ? 'Gold' : instruments[0] || ''),
  )
  const [payload, setPayload] = React.useState(null)
  const [loading, setLoading] = React.useState(false)
  const [fetchError, setFetchError] = React.useState(null)
  const requestSeq = React.useRef(0)

  const refresh = React.useCallback(async () => {
    const seq = ++requestSeq.current
    if (!String(instrumentId || '').trim()) {
      setPayload(null)
      setFetchError(null)
      setLoading(false)
      return
    }
    setLoading(true)
    setFetchError(null)
    try {
      const q = new URLSearchParams({
        instrument_id: String(instrumentId).trim(),
      })
      const res = await fetch(`/api/macro-intelligence?${q.toString()}`, {
        method: 'GET',
        cache: 'no-store',
      })
      const body = await res.json()
      if (seq !== requestSeq.current) return
      setPayload(body)
      if (!body || body.status !== 'ok') {
        const errs = body?.errors?.length
          ? body.errors
          : [body?.message || body?.error || 'Macro intelligence request failed.']
        setFetchError(errs)
      } else {
        setFetchError(null)
      }
    } catch (err) {
      if (seq !== requestSeq.current) return
      setPayload(null)
      setFetchError([err?.message || 'Fetch failed'])
    } finally {
      if (seq === requestSeq.current) setLoading(false)
    }
  }, [instrumentId])

  React.useEffect(() => {
    const t = setTimeout(() => {
      refresh()
    }, 200)
    return () => clearTimeout(t)
  }, [refresh])

  const contributors = Array.isArray(payload?.contributors) ? payload.contributors : []
  const bias = payload?.status === 'ok' ? payload.overall_macro_bias : null

  return (
    <div className="mi-body" data-mi-phase="5">
      <section className="mi-section" aria-label="Instrument selection">
        <h2>Instrument</h2>
        <div className="mi-controls">
          <div className="mi-field">
            <label htmlFor="mi-instrument">Market</label>
            <select
              id="mi-instrument"
              value={instrumentId}
              onChange={(e) => setInstrumentId(e.target.value)}
            >
              <option value="">Select instrument…</option>
              {instruments.map((id) => (
                <option key={id} value={id}>
                  {id}
                </option>
              ))}
            </select>
          </div>
          <button
            type="button"
            className="mi-btn mi-btn-primary"
            onClick={refresh}
            disabled={loading || !instrumentId}
          >
            {loading ? 'Loading…' : 'Refresh'}
          </button>
        </div>
        <p className="mi-muted" style={{ marginTop: '0.65rem' }}>
          Architecture only — contributors return Unavailable until live modules are
          connected. No buy/sell signals.
        </p>
      </section>

      {fetchError?.length ? (
        <div className="mi-error" role="alert">
          <strong>Macro intelligence errors</strong>
          <ul>
            {fetchError.map((e) => (
              <li key={e}>{e}</li>
            ))}
          </ul>
        </div>
      ) : null}

      <section className="mi-section" aria-label="Overall macro bias">
        <h2>Overall Macro Bias</h2>
        {loading && !bias ? <p className="mi-muted">Loading macro bias…</p> : null}
        {!loading && !instrumentId ? (
          <p className="mi-muted">Select an instrument to view macro bias.</p>
        ) : null}
        {bias ? (
          <div className="mi-bias">
            <div>
              <div className="mi-bias-label">Macro Environment</div>
              <div className={`mi-bias-value ${biasClass(bias)}`}>{bias}</div>
            </div>
            <div className="mi-muted">
              Instrument: <strong style={{ color: '#e2e8f0' }}>{payload.instrument_id}</strong>
            </div>
          </div>
        ) : null}
        {payload?.notes?.length ? (
          <ul className="mi-notes">
            {payload.notes.map((n) => (
              <li key={n}>{n}</li>
            ))}
          </ul>
        ) : null}
      </section>

      <section className="mi-section" aria-label="Macro contributors">
        <h2>Macro Contributors</h2>
        {loading && !contributors.length ? (
          <p className="mi-muted">Loading contributors…</p>
        ) : null}
        {contributors.length ? (
          <div className="mi-table-wrap">
            <table className="mi-table">
              <thead>
                <tr>
                  <th>Contributor</th>
                  <th>Status</th>
                  <th>Summary</th>
                  <th>Last Updated</th>
                </tr>
              </thead>
              <tbody>
                {contributors.map((c) => (
                  <tr key={c.contributor_id || c.name}>
                    <td>{c.name}</td>
                    <td
                      className={
                        c.status === 'Unavailable' ? 'mi-status-unavailable' : undefined
                      }
                    >
                      {c.status}
                    </td>
                    <td>{c.summary || '—'}</td>
                    <td>{fmtUpdated(c.last_updated)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
      </section>
    </div>
  )
}
