import React from 'react'
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import { AppShell } from '../components/AppShell.jsx'
import { fetchPublicJson } from '../utils/fetchPublicJson.js'
import { useCanonicalCurrentPrice } from '../prices/canonicalCurrentPrice.js'
import { navigateToCotWorkstation, navigateToInstrument, navigateToScanner } from '../routing.js'
import './naturalGasValuation.css'

const MARKET = 'Natural Gas / NG'

function fmt(v, digits = 3) {
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
  return `${sign}${n.toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })}`
}

function toneClass(tone) {
  const t = String(tone || '').toLowerCase()
  if (t.includes('bull')) return 'ngv-tone-bull'
  if (t.includes('bear')) return 'ngv-tone-bear'
  return 'ngv-tone-neutral'
}

function biasTone(bias) {
  const b = String(bias || '').toLowerCase()
  if (b.includes('under') || b.includes('bull')) return 'bull'
  if (b.includes('over') || b.includes('bear')) return 'bear'
  return 'neutral'
}

function DriverIcon({ id }) {
  const map = {
    market_price: '◎',
    storage: '▣',
    production: '▴',
    lng_exports: '⇢',
    dxy: '$',
    seasonality: '⟳',
  }
  return <span className="ngv-driver-icon" aria-hidden="true">{map[id] || '●'}</span>
}

function SummaryCard({ label, value, sub, tone }) {
  return (
    <article className={`ngv-summary-card ngv-summary-card--${tone || 'neutral'}`}>
      <span className="ngv-summary-label">{label}</span>
      <strong className="ngv-summary-value">{value}</strong>
      {sub ? <span className="ngv-summary-sub">{sub}</span> : null}
    </article>
  )
}

function ValuationScale({ scale, deviationPct }) {
  const pct = scale?.pct ?? 50
  const band = scale?.band || 'Fair Value'
  return (
    <section className="ngv-scale" aria-label="Valuation scale">
      <header className="ngv-section-head">
        <h2>Institutional Valuation Scale</h2>
        <span className="ngv-scale-band">{band}</span>
      </header>
      <div className="ngv-scale-track">
        <div className="ngv-scale-gradient" />
        <div className="ngv-scale-marker" style={{ left: `${pct}%` }}>
          <span className="ngv-scale-pin" />
          <span className="ngv-scale-pin-label">
            Today {fmtSigned(deviationPct, 2)}%
          </span>
        </div>
      </div>
      <div className="ngv-scale-labels">
        <span>Strongly<br />Undervalued</span>
        <span>Moderately<br />Undervalued</span>
        <span>Fair<br />Value</span>
        <span>Moderately<br />Overvalued</span>
        <span>Strongly<br />Overvalued</span>
      </div>
    </section>
  )
}

function roleClass(badge) {
  const r = String(badge || '')
  if (r.includes('INCLUDED') || r.includes('VALIDATED')) return 'ngv-role-validated'
  if (r.includes('REJECTED')) return 'ngv-role-rejected'
  if (r.includes('INSUFFICIENT')) return 'ngv-role-invalid'
  if (r.includes('INVALID')) return 'ngv-role-invalid'
  if (r.includes('EXPERIMENTAL')) return 'ngv-role-experimental'
  return 'ngv-role-info'
}

function ContributionBreakdown({ breakdown }) {
  if (!breakdown?.drivers?.length) {
    return (
      <div className="ngv-contrib-empty">
        Contribution breakdown appears once validated valuation drivers are selected.
      </div>
    )
  }
  return (
    <div className="ngv-contrib">
      <table className="ngv-contrib-table">
        <thead>
          <tr>
            <th>Driver</th>
            <th>Raw observation</th>
            <th>Transformed x</th>
            <th>Coefficient β</th>
            <th>Log contrib βx</th>
            <th>Direction</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>Intercept / baseline</td>
            <td>—</td>
            <td>—</td>
            <td>—</td>
            <td>{fmtSigned(breakdown.intercept_log_contribution, 4)}</td>
            <td>—</td>
          </tr>
          {breakdown.drivers.map((row) => (
            <tr key={row.feature}>
              <td>{row.label}</td>
              <td>{row.raw_observation != null ? fmt(row.raw_observation, 3) : '—'}</td>
              <td>{fmt(row.transformed_input, 4)}</td>
              <td>{fmtSigned(row.coefficient, 4)}</td>
              <td className={row.log_contribution >= 0 ? 'ngv-pos' : 'ngv-neg'}>
                {fmtSigned(row.log_contribution, 4)}
              </td>
              <td>{row.direction}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <dl className="ngv-contrib-totals">
        <div>
          <dt>Σ log terms</dt>
          <dd>{fmt(breakdown.sum_log_contributions, 4)}</dd>
        </div>
        <div>
          <dt>Fair Value = exp(Σ)</dt>
          <dd>{fmt(breakdown.reconstructed_fair_value, 3)}</dd>
        </div>
        <div>
          <dt>Market Price</dt>
          <dd>{fmt(breakdown.market_price, 3)}</dd>
        </div>
        <div>
          <dt>Deviation</dt>
          <dd className={Number(breakdown.deviation_pct) < 0 ? 'ngv-pos' : 'ngv-neg'}>
            {fmtSigned(breakdown.deviation_pct, 2)}%
          </dd>
        </div>
      </dl>
      <p className="ngv-contrib-note">
        {breakdown.identity}
        {breakdown.reconciliation_ok ? ' · Reconciliation OK.' : ' · Reconciliation FAILED.'}
      </p>
      <p className="ngv-contrib-note">{breakdown.note}</p>
    </div>
  )
}

function DriverCard({ card }) {
  const available = card?.available !== false
  const badge =
    card?.valuation_badge || (card?.id === 'seasonality' ? 'INFORMATIONAL ONLY' : null)
  return (
    <article
      className={`ngv-driver-card ${toneClass(card?.tone)} ${available ? '' : 'ngv-driver-card--awaiting'}`}
    >
      <header className="ngv-driver-head">
        <DriverIcon id={card?.id} />
        <div>
          <h3>{card?.label || 'Driver'}</h3>
          <span className="ngv-driver-source">{card?.source || (available ? '—' : 'Pipeline pending')}</span>
        </div>
        <span className={`ngv-effect-pill ${toneClass(card?.tone)}`}>
          {card?.institutional_effect || '—'}
        </span>
      </header>
      {badge ? (
        <div className={`ngv-role-badge ${roleClass(badge)}`}>
          {badge}
          {card?.valuation_note ? ` · ${card.valuation_note}` : ''}
        </div>
      ) : null}

      {card?.id === 'storage' ? (
        <dl className="ngv-driver-metrics">
          <div>
            <dt>Current</dt>
            <dd>{available ? `${fmt(card.current, 0)} ${card.unit || 'Bcf'}` : '—'}</dd>
          </div>
          <div>
            <dt>5-Year Average</dt>
            <dd>{available && card.five_year_average != null ? `${fmt(card.five_year_average, 0)} Bcf` : '—'}</dd>
          </div>
          <div>
            <dt>Difference</dt>
            <dd>{available && card.difference != null ? `${fmtSigned(card.difference, 0)} Bcf` : '—'}</dd>
          </div>
        </dl>
      ) : card?.id === 'hdd' || card?.id === 'cdd' ? (
        <dl className="ngv-driver-metrics">
          <div>
            <dt>Actual</dt>
            <dd>{available && card.current != null ? fmt(card.current, 1) : '—'}</dd>
          </div>
          <div>
            <dt>Week normal</dt>
            <dd>{card.normal != null ? fmt(card.normal, 1) : '—'}</dd>
          </div>
          <div>
            <dt>Anomaly</dt>
            <dd>{card.anomaly != null ? `${fmtSigned(card.anomaly, 2)}σ` : '—'}</dd>
          </div>
        </dl>
      ) : (
        <dl className="ngv-driver-metrics">
          <div>
            <dt>Current</dt>
            <dd>
              {available && card.current != null
                ? `${fmt(card.current, card.unit === 'index' ? 2 : 3)}${card.unit ? ` ${card.unit}` : ''}`
                : '—'}
            </dd>
          </div>
          {card.proxy || card.fallback ? (
            <div>
              <dt>Status</dt>
              <dd>{card.fallback ? 'FALLBACK' : 'V1 proxy'}</dd>
            </div>
          ) : null}
        </dl>
      )}

      <p className="ngv-driver-copy">{card?.interpretation || '—'}</p>
    </article>
  )
}

function ChartPanel({ history }) {
  const data = React.useMemo(() => {
    const rows = Array.isArray(history) ? history : []
    // Show last ~5 years for density
    const sliced = rows.length > 260 ? rows.slice(-260) : rows
    return sliced.map((r) => ({
      date: r.date,
      spot: r.spot_price,
      fair: r.fair_value,
    }))
  }, [history])

  if (!data.length) {
    return (
      <div className="ngv-chart-empty">
        Historical fair-value series will appear once the model has aligned drivers.
      </div>
    )
  }

  return (
    <div className="ngv-chart-wrap">
      <ResponsiveContainer width="100%" height={380}>
        <LineChart data={data} margin={{ top: 12, right: 18, left: 4, bottom: 8 }}>
          <CartesianGrid stroke="rgba(148,163,184,0.12)" strokeDasharray="3 6" />
          <XAxis
            dataKey="date"
            tick={{ fill: '#94a3b8', fontSize: 11 }}
            minTickGap={48}
            tickFormatter={(v) => String(v).slice(0, 7)}
          />
          <YAxis
            tick={{ fill: '#94a3b8', fontSize: 11 }}
            width={48}
            domain={['auto', 'auto']}
            tickFormatter={(v) => Number(v).toFixed(2)}
          />
          <Tooltip
            contentStyle={{
              background: '#0b1220',
              border: '1px solid rgba(148,163,184,0.25)',
              borderRadius: 8,
            }}
            labelStyle={{ color: '#e2e8f0' }}
            formatter={(value, name) => [fmt(value, 3), name === 'spot' ? 'NG Price' : 'Fair Value']}
          />
          <Legend
            wrapperStyle={{ color: '#cbd5e1', paddingTop: 8 }}
            formatter={(value) => (value === 'spot' ? 'Weekly NG Price' : 'Institutional Fair Value')}
          />
          <Line
            type="monotone"
            dataKey="spot"
            name="spot"
            stroke="#38bdf8"
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
          />
          <Line
            type="monotone"
            dataKey="fair"
            name="fair"
            stroke="#fbbf24"
            strokeWidth={2}
            strokeDasharray="6 4"
            dot={false}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

export function NaturalGasValuationPage({ sidebarClass, onSidebarClass }) {
  const [doc, setDoc] = React.useState(null)
  const [error, setError] = React.useState(null)
  const [loading, setLoading] = React.useState(true)
  const canonical = useCanonicalCurrentPrice(MARKET)

  React.useEffect(() => {
    let active = true
    setLoading(true)
    fetchPublicJson('/data/natural_gas_valuation_latest.json')
      .then((d) => {
        if (!active) return
        setDoc(d)
        setError(null)
      })
      .catch((err) => {
        if (!active) return
        setError(String(err?.message || err))
        setDoc(null)
      })
      .finally(() => {
        if (active) setLoading(false)
      })
    return () => {
      active = false
    }
  }, [])

  const inst = doc?.instrument || {}
  const cards = Array.isArray(inst.driver_cards) ? inst.driver_cards : []
  const history = Array.isArray(inst.history) ? inst.history : []
  const tone = biasTone(inst.institutional_bias || inst.valuation_bias)

  return (
    <AppShell
      title="Natural Gas Valuation"
      subtitle="Institutional energy fair value"
      sidebarClass={sidebarClass}
      onSidebarClass={onSidebarClass}
      topActions={
        <div className="ngv-top-actions">
          <button type="button" className="ws-btn" onClick={() => navigateToInstrument(MARKET)}>
            ← Back to Natural Gas
          </button>
          <button
            type="button"
            className="ws-btn ws-btn-primary"
            onClick={() => navigateToCotWorkstation(MARKET)}
          >
            Open COT Workstation
          </button>
          <button type="button" className="ws-btn" onClick={navigateToScanner}>
            Scanner
          </button>
        </div>
      }
    >
      <div className="ngv-page">
        <header className="ngv-hero">
          <p className="ngv-eyebrow">HPTL · Energy · Validated Drivers Only</p>
          <h1>Natural Gas Institutional Valuation</h1>
          <p className="ngv-hero-sub">
            Fair value uses only walk-forward-validated valuation drivers. Experimental and
            informational drivers are displayed for context but do not enter the calculation.
          </p>
        </header>

        {loading ? <div className="ngv-loading">Loading valuation…</div> : null}
        {error ? (
          <div className="ngv-error">
            Could not load valuation export. Run{' '}
            <code>python scripts/run_natural_gas_valuation_v1.py</code>
            <div className="ngv-error-detail">{error}</div>
          </div>
        ) : null}

        {!loading && !error ? (
          <>
            <section className="ngv-summary-grid" aria-label="Valuation summary">
              <SummaryCard
                label="Current Price"
                value={fmt(canonical.price ?? null, 3)}
                sub={`USD / MMBtu · ${canonical.label}${
                  inst.spot_price != null &&
                  canonical.price != null &&
                  Math.abs(Number(inst.spot_price) - Number(canonical.price)) > 0.01
                    ? ` · model spot ${fmt(inst.spot_price, 3)} (valuation only)`
                    : ''
                }`}
                tone="neutral"
              />
              <SummaryCard
                label="Fair Value"
                value={fmt(inst.fair_value, 3)}
                sub={inst.model_id || 'energy_natural_gas_v1'}
                tone="neutral"
              />
              <SummaryCard
                label="Deviation %"
                value={fmtSigned(inst.deviation_pct, 2)}
                sub={inst.scale?.band || '—'}
                tone={tone}
              />
              <SummaryCard
                label="Institutional Bias"
                value={inst.institutional_bias || inst.valuation_bias || '—'}
                sub={inst.valuation_bias || '—'}
                tone={tone}
              />
              <SummaryCard
                label="Confidence"
                value={inst.confidence || '—'}
                sub={inst.regression?.r_squared != null ? `R² ${inst.regression.r_squared}` : '—'}
                tone="neutral"
              />
              <SummaryCard
                label="Last Updated"
                value={String(inst.generated_at || doc?.generated_at || '—').slice(0, 16).replace('T', ' ')}
                sub={inst.as_of_week ? `As of ${inst.as_of_week}` : '—'}
                tone="neutral"
              />
            </section>

            <section className="ngv-chart-section">
              <header className="ngv-section-head">
                <h2>Price vs Institutional Fair Value</h2>
                <span className="ngv-chart-legend-hint">
                  Solid = weekly NG · Dashed = model fair value
                </span>
              </header>
              <ChartPanel history={history} />
            </section>

            <ValuationScale scale={inst.scale} deviationPct={inst.deviation_pct} />

            <section className="ngv-contrib-section" aria-label="Valuation contribution breakdown">
              <header className="ngv-section-head">
                <h2>Valuation Contribution Breakdown</h2>
                <span className="ngv-section-meta">
                  {(inst.validated_features || inst.active_features || []).length} validated drivers
                </span>
              </header>
              <ContributionBreakdown breakdown={inst.contribution_breakdown} />
            </section>

            <section className="ngv-drivers-section">
              <header className="ngv-section-head">
                <h2>Institutional Driver Summary</h2>
                <span className="ngv-section-meta">
                  {(inst.validated_features || []).length} validated ·{' '}
                  {(inst.experimental_features || []).length} experimental ·{' '}
                  {(inst.informational_features || ['seasonality']).length} informational
                </span>
              </header>
              <div className="ngv-driver-grid">
                {cards
                  .filter((c) => c.id !== 'market_price')
                  .map((card) => (
                    <DriverCard key={card.id} card={card} />
                  ))}
              </div>
            </section>

            <section className="ngv-narrative">
              <header className="ngv-section-head">
                <h2>Institutional Summary</h2>
              </header>
              <p className="ngv-narrative-text">{inst.summary_text || '—'}</p>
              {inst.model_note ? <p className="ngv-model-note">{inst.model_note}</p> : null}
            </section>
          </>
        ) : null}
      </div>
    </AppShell>
  )
}

export default NaturalGasValuationPage
