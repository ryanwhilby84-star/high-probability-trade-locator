import React from 'react'

import { AppShell } from '../components/AppShell.jsx'
import { fetchPublicJson } from '../utils/fetchPublicJson.js'
import {
  navigateToCotWorkstation,
  navigateToInstrument,
  navigateToMacroHub,
  navigateToScanner,
} from '../routing.js'
import './dxyMacroBias.css'

const MARKET = 'US Dollar Index / DX'

function fmt(v, digits = 2) {
  if (v == null || v === '') return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return String(v)
  return n.toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })
}

function fmtSigned(v, digits = 2) {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  const n = Number(v)
  const sign = n > 0 ? '+' : ''
  return `${sign}${fmt(n, digits)}`
}

function toneFor(dir) {
  const d = String(dir || '').toLowerCase()
  if (d.includes('bull')) return 'bull'
  if (d.includes('bear')) return 'bear'
  return 'neutral'
}

function FreshnessBadge({ freshness }) {
  const status = freshness?.status || 'MISSING'
  return (
    <span className={`dxy-fresh dxy-fresh--${String(status).toLowerCase()}`}>
      {status}
      {freshness?.as_of ? ` · ${freshness.as_of}` : ''}
    </span>
  )
}

export function DxyMacroBiasPage() {
  const [doc, setDoc] = React.useState(null)
  const [error, setError] = React.useState(null)
  const [loading, setLoading] = React.useState(true)

  React.useEffect(() => {
    let cancelled = false
    setLoading(true)
    fetchPublicJson('/data/dxy_macro_bias_latest.json')
      .then((d) => {
        if (!cancelled) {
          setDoc(d)
          setError(null)
        }
      })
      .catch((e) => {
        if (!cancelled) setError(e?.message || String(e))
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [])

  const drivers = doc?.drivers || []
  const biasTone = toneFor(doc?.macro_bias)

  return (
    <AppShell
      title="DXY Macro Bias"
      subtitle="USD Index workstation bias — FRED broad USD proxy + ICE DX futures COT"
      topActions={
        <>
          <button type="button" className="ws-btn" onClick={navigateToScanner}>
            ← Scanner
          </button>
          <button type="button" className="ws-btn" onClick={navigateToMacroHub}>
            Macro Hub
          </button>
          <button type="button" className="ws-btn" onClick={() => navigateToInstrument(MARKET)}>
            Instrument
          </button>
          <button
            type="button"
            className="ws-btn ws-btn-primary"
            onClick={() => navigateToCotWorkstation(MARKET)}
          >
            Open COT Workstation
          </button>
        </>
      }
    >
      <div className="dxy-page">
        {loading ? <p className="dxy-muted">Loading DXY macro bias…</p> : null}
        {error ? (
          <p className="dxy-error">
            Failed to load dxy_macro_bias_latest.json — {error}. Run:{' '}
            <code>python scripts/run_dxy_macro_bias.py</code>
          </p>
        ) : null}

        {doc ? (
          <>
            <header className={`dxy-hero dxy-hero--${biasTone}`}>
              <div>
                <p className="dxy-kicker">DXY MACRO BIAS</p>
                <h2 className="dxy-bias">{doc.macro_bias || '—'}</h2>
                <p className="dxy-summary">{doc.macro_bias_summary}</p>
              </div>
              <div className="dxy-hero-meta">
                <div>
                  <span className="dxy-meta-k">Charted price</span>
                  <strong>
                    {fmt(doc.price_instrument?.latest, 4)}{' '}
                    <span className="dxy-muted">as of {doc.price_instrument?.as_of || '—'}</span>
                  </strong>
                  <p className="dxy-note">{doc.price_instrument?.note}</p>
                  <FreshnessBadge freshness={doc.price_instrument?.freshness} />
                </div>
                <div>
                  <span className="dxy-meta-k">Valuation</span>
                  <strong>{doc.valuation_status || 'NOT_YET_VALIDATED'}</strong>
                  <p className="dxy-note">{doc.valuation_note}</p>
                </div>
              </div>
            </header>

            <section className="dxy-section" aria-label="Treasuries">
              <h3>Treasuries</h3>
              <div className="dxy-treasury-grid">
                {['us_2y', 'us_10y', 'us_10y_real', 'curve_2s10s'].map((k) => {
                  const row = doc.treasuries?.[k]
                  if (!row) return null
                  return (
                    <article key={k} className={`dxy-card dxy-card--${toneFor(row.direction_for_usd)}`}>
                      <span className="dxy-card-k">{row.label}</span>
                      <strong>{fmt(row.value, 3)}</strong>
                      <span>
                        5d {fmtSigned(row.change_5d, 3)} · z {fmtSigned(row.zscore_1y, 2)}
                      </span>
                      <span>{row.direction_for_usd}</span>
                      <FreshnessBadge freshness={row.freshness} />
                      <span className="dxy-src">{row.source}</span>
                    </article>
                  )
                })}
              </div>
            </section>

            <section className="dxy-section" aria-label="Drivers">
              <h3>Drivers</h3>
              <div className="dxy-driver-table-wrap">
                <table className="dxy-driver-table">
                  <thead>
                    <tr>
                      <th>Driver</th>
                      <th>Class</th>
                      <th>Value</th>
                      <th>USD direction</th>
                      <th>Freshness</th>
                      <th>Source</th>
                      <th>Why</th>
                    </tr>
                  </thead>
                  <tbody>
                    {drivers.map((d) => (
                      <tr key={d.key} className={`dxy-row--${toneFor(d.direction_for_usd)}`}>
                        <td>{d.label}</td>
                        <td>
                          <code>{d.classification}</code>
                        </td>
                        <td>
                          {fmt(d.value, 3)}
                          {d.change_5d != null ? (
                            <span className="dxy-muted"> ({fmtSigned(d.change_5d, 3)})</span>
                          ) : null}
                        </td>
                        <td>{d.direction_for_usd}</td>
                        <td>
                          <FreshnessBadge freshness={d.freshness} />
                        </td>
                        <td>{d.source}</td>
                        <td>{d.explanation}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>

            <section className="dxy-section" aria-label="Lineage">
              <h3>Provenance</h3>
              <ol className="dxy-lineage">
                {Object.entries(doc.lineage || {}).map(([k, v]) => (
                  <li key={k}>
                    <strong>{k}</strong>: {v}
                  </li>
                ))}
              </ol>
              <p className="dxy-muted">Generated {doc.generated_at || '—'}</p>
            </section>
          </>
        ) : null}
      </div>
    </AppShell>
  )
}
