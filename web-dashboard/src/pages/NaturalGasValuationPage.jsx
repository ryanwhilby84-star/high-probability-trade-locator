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
import {
  navigateToCotWorkstation,
  navigateToInstrument,
  navigateToNaturalGasValuation,
  navigateToScanner,
} from '../routing.js'
import {
  NG_HEADLINE_V2,
  contributionRows,
  resolveNgValuationView,
} from './naturalGasValuationModel.js'
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
        <h2>Valuation Scale</h2>
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
  if (r.includes('REJECTED') || r.includes('FALLBACK')) return 'ngv-role-rejected'
  if (r.includes('INSUFFICIENT')) return 'ngv-role-invalid'
  if (r.includes('INVALID')) return 'ngv-role-invalid'
  if (r.includes('EXPERIMENTAL')) return 'ngv-role-experimental'
  return 'ngv-role-info'
}

function ContributionBreakdown({ breakdown, contributions }) {
  const rows =
    Array.isArray(breakdown?.drivers) && breakdown.drivers.length
      ? breakdown.drivers
      : contributionRows({ contributions }).map((r) => ({
          feature: r.feature,
          label: r.label,
          raw_observation: r.value,
          transformed_input: r.value,
          coefficient: r.coefficient,
          log_contribution: r.logContribution,
          direction: r.direction,
        }))

  if (!rows.length) {
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
            <th>Value</th>
            <th>Coefficient β</th>
            <th>Log contrib βx</th>
            <th>Price impact %</th>
            <th>Direction</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>Intercept / baseline</td>
            <td>—</td>
            <td>—</td>
            <td>{fmtSigned(breakdown?.intercept_log_contribution, 4)}</td>
            <td>—</td>
            <td>—</td>
          </tr>
          {rows.map((row) => (
            <tr key={row.feature}>
              <td>{row.label || row.feature}</td>
              <td>{row.raw_observation != null ? fmt(row.raw_observation, 3) : '—'}</td>
              <td>{fmtSigned(row.coefficient, 4)}</td>
              <td className={row.log_contribution >= 0 ? 'ngv-pos' : 'ngv-neg'}>
                {fmtSigned(row.log_contribution, 4)}
              </td>
              <td>
                {row.price_impact_pct != null
                  ? fmtSigned(row.price_impact_pct, 2)
                  : row.priceImpactPct != null
                    ? fmtSigned(row.priceImpactPct, 2)
                    : '—'}
              </td>
              <td>{row.direction}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <dl className="ngv-contrib-totals">
        <div>
          <dt>Σ log terms</dt>
          <dd>{fmt(breakdown?.sum_log_contributions, 4)}</dd>
        </div>
        <div>
          <dt>Fair Value = exp(Σ)</dt>
          <dd>{fmt(breakdown?.reconstructed_fair_value, 3)}</dd>
        </div>
        <div>
          <dt>Market Price</dt>
          <dd>{fmt(breakdown?.market_price, 3)}</dd>
        </div>
        <div>
          <dt>Deviation</dt>
          <dd className={Number(breakdown?.deviation_pct) < 0 ? 'ngv-pos' : 'ngv-neg'}>
            {fmtSigned(breakdown?.deviation_pct, 2)}%
          </dd>
        </div>
      </dl>
      <p className="ngv-contrib-note">
        {breakdown?.identity}
        {breakdown?.reconciliation_ok ? ' · Reconciliation OK.' : ' · Reconciliation FAILED.'}
      </p>
      <p className="ngv-contrib-note">{breakdown?.note}</p>
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
            <dt>Surplus / deficit</dt>
            <dd>{available && card.difference != null ? `${fmtSigned(card.difference, 0)} Bcf` : '—'}</dd>
          </div>
          {card.contribution_magnitude_log != null ? (
            <div>
              <dt>Log contribution</dt>
              <dd>{fmtSigned(card.contribution_magnitude_log, 4)}</dd>
            </div>
          ) : null}
        </dl>
      ) : card?.id === 'production' ? (
        <dl className="ngv-driver-metrics">
          <div>
            <dt>YoY change (model)</dt>
            <dd>
              {card.yoy_pct != null || card.current != null
                ? `${fmtSigned(card.yoy_pct ?? card.current, 2)}%`
                : '—'}
            </dd>
          </div>
          <div>
            <dt>Production level</dt>
            <dd>
              {card.production_level_bcf_d != null
                ? `${fmt(card.production_level_bcf_d, 3)} ${card.production_level_unit || 'Bcf/d'}`
                : '—'}
            </dd>
          </div>
          <div>
            <dt>Observation date</dt>
            <dd>{card.observation_date || card.as_of || '—'}</dd>
          </div>
          <div>
            <dt>Source cadence</dt>
            <dd>{card.source_cadence || 'monthly'}</dd>
          </div>
          {card.contribution_direction ? (
            <div>
              <dt>Contribution</dt>
              <dd>
                {card.contribution_direction}
                {card.contribution_magnitude_log != null
                  ? ` (${fmtSigned(card.contribution_magnitude_log, 4)} log)`
                  : ''}
              </dd>
            </div>
          ) : null}
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
        </dl>
      )}

      <p className="ngv-driver-copy">{card?.interpretation || '—'}</p>
    </article>
  )
}

function ChartPanel({ history }) {
  const data = React.useMemo(() => {
    const rows = Array.isArray(history) ? history : []
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
            formatter={(value) => (value === 'spot' ? 'Weekly NG Price' : 'Model Fair Value')}
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
  const view = resolveNgValuationView(doc)
  const cards = Array.isArray(inst.driver_cards) ? inst.driver_cards : []
  const history = Array.isArray(inst.history) ? inst.history : []
  const tone = biasTone(inst.institutional_bias || inst.valuation_bias)

  return (
    <AppShell
      title="Natural Gas Valuation"
      subtitle="Validated two-driver fair value"
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
            onClick={() => navigateToNaturalGasValuation()}
          >
            Valuation workstation
          </button>
          <button
            type="button"
            className="ws-btn"
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
          <h1>{view.headline || NG_HEADLINE_V2}</h1>
          <p className="ngv-hero-sub">
            Active model uses only walk-forward-validated drivers. Storage surplus and production
            year-over-year change enter fair value when available; experimental drivers stay
            display-only.
          </p>
        </header>

        {loading ? <div className="ngv-loading">Loading valuation…</div> : null}
        {error ? (
          <div className="ngv-error">
            Could not load valuation export. Run{' '}
            <code>python scripts/refresh_natural_gas_drivers.py</code>
            <div className="ngv-error-detail">{error}</div>
          </div>
        ) : null}

        {!loading && !error ? (
          <>
            {view.fallback || (view.freshnessWarnings || []).length ? (
              <div className="ngv-error" role="status">
                {view.fallback
                  ? `Fallback to ${view.activeModel}: ${view.fallbackReason || 'production YoY unavailable/stale'}`
                  : null}
                {(view.freshnessWarnings || []).map((w) => (
                  <div key={w} className="ngv-error-detail">
                    {w}
                  </div>
                ))}
              </div>
            ) : null}

            <section className="ngv-summary-grid" aria-label="Valuation summary">
              <SummaryCard
                label="Active Model"
                value={view.activeModel || '—'}
                sub={view.fallback ? 'v1 fallback active' : 'v2 published'}
                tone="neutral"
              />
              <SummaryCard
                label="Live / Market Quote"
                value={fmt(canonical.price ?? view.livePrice ?? view.marketPrice, 3)}
                sub={`${canonical.label || view.livePriceStatus || view.priceStatus || '—'} · ${view.priceSource || 'OANDA'}`}
                tone={view.priceStatus === 'Stale' || !view.deviationTrusted ? 'bear' : 'neutral'}
              />
              <SummaryCard
                label="v2 Fair Value"
                value={fmt(view.v2FairValue ?? (view.activeModel?.includes('v2') ? view.fairValue : null), 3)}
                sub="Storage + Production YoY"
                tone="neutral"
              />
              <SummaryCard
                label="Trusted Deviation %"
                value={view.deviationTrusted ? fmtSigned(view.deviationPct, 2) : 'Unavailable'}
                sub={
                  view.deviationTrusted
                    ? inst.scale?.band || '—'
                    : 'Price stale — fair value retained'
                }
                tone={view.deviationTrusted ? tone : 'bear'}
              />
              <SummaryCard
                label="v1 Benchmark FV"
                value={fmt(view.v1FairValue, 3)}
                sub={
                  view.v1V2Diff != null
                    ? `v2 − v1 = ${fmtSigned(view.v1V2Diff, 3)}`
                    : 'Storage-only'
                }
                tone="neutral"
              />
              <SummaryCard
                label="Price Status"
                value={view.priceStatus || canonical.status || '—'}
                sub={
                  view.dataAgeHours != null
                    ? `Age ${fmt(view.dataAgeHours, 1)}h`
                    : (view.confidenceReasons || [])[0] || '—'
                }
                tone={view.priceStatus === 'Current' ? 'bull' : 'bear'}
              />
            </section>

            <section className="ngv-narrative" aria-label="Price freshness">
              <header className="ngv-section-head">
                <h2>Price Freshness</h2>
              </header>
              <dl className="ngv-contrib-totals">
                <div>
                  <dt>Price source</dt>
                  <dd>{view.priceSource || '—'}</dd>
                </div>
                <div>
                  <dt>Live quote timestamp</dt>
                  <dd>{view.livePriceAsOf || canonical.asOf || '—'}</dd>
                </div>
                <div>
                  <dt>Latest completed daily</dt>
                  <dd>
                    {view.latestCompletedDaily?.date || '—'}
                    {view.latestCompletedDaily?.close != null
                      ? ` @ ${fmt(view.latestCompletedDaily.close, 3)}`
                      : ''}
                  </dd>
                </div>
                <div>
                  <dt>Latest weekly bar</dt>
                  <dd>
                    {view.latestCompletedWeekly?.date || '—'}
                    {view.latestCompletedWeekly?.close != null
                      ? ` @ ${fmt(view.latestCompletedWeekly.close, 3)}`
                      : ''}
                  </dd>
                </div>
                <div>
                  <dt>Model anchor (weekly)</dt>
                  <dd>
                    {fmt(view.modelAnchorPrice, 3)} · as of {view.asOfWeek || '—'}
                  </dd>
                </div>
                <div>
                  <dt>Forming daily (incomplete)</dt>
                  <dd>
                    {view.formingDaily?.date
                      ? `${view.formingDaily.date} @ ${fmt(view.formingDaily.close, 3)}`
                      : '—'}
                  </dd>
                </div>
              </dl>
              {!view.deviationTrusted ? (
                <p className="ngv-error">
                  Market comparison is stale or unavailable. Fair value is retained; do not treat
                  over/undervaluation % as current.
                  {view.deviationUntrusted != null
                    ? ` (Untrusted illustrative deviation: ${fmtSigned(view.deviationUntrusted, 2)}%.)`
                    : ''}
                </p>
              ) : null}
            </section>

            <section className="ngv-narrative">
              <header className="ngv-section-head">
                <h2>Model Status</h2>
              </header>
              <dl className="ngv-contrib-totals">
                <div>
                  <dt>Validated drivers</dt>
                  <dd>{(view.validatedDrivers || []).join(', ') || '—'}</dd>
                </div>
                <div>
                  <dt>Model as-of</dt>
                  <dd>{view.asOfWeek || '—'}</dd>
                </div>
                <div>
                  <dt>Production observation</dt>
                  <dd>{view.productionObservationDate || '—'}</dd>
                </div>
                <div>
                  <dt>Production cadence</dt>
                  <dd>{view.productionSourceCadence || 'monthly'}</dd>
                </div>
              </dl>
              {(view.confidenceReasons || []).length ? (
                <ul className="ngv-model-note">
                  {view.confidenceReasons.map((r) => (
                    <li key={r}>{r}</li>
                  ))}
                </ul>
              ) : null}
              {view.equation ? <p className="ngv-model-note">{view.equation}</p> : null}
            </section>

            <section className="ngv-chart-section">
              <header className="ngv-section-head">
                <h2>Price vs Model Fair Value</h2>
                <span className="ngv-chart-legend-hint">
                  Solid = weekly NG · Dashed = active model fair value
                </span>
              </header>
              <ChartPanel history={history} />
            </section>

            <ValuationScale scale={inst.scale} deviationPct={view.deviationPct} />

            <section className="ngv-contrib-section" aria-label="Valuation contribution breakdown">
              <header className="ngv-section-head">
                <h2>Driver Contributions</h2>
                <span className="ngv-section-meta">
                  {(view.validatedDrivers || []).length} validated drivers
                </span>
              </header>
              <ContributionBreakdown
                breakdown={view.contributionBreakdown}
                contributions={view.contributions}
              />
            </section>

            <section className="ngv-drivers-section">
              <header className="ngv-section-head">
                <h2>Driver Summary</h2>
                <span className="ngv-section-meta">
                  transform={view.productionTransformation} · raw level in FV=
                  {view.rawLevelUsed ? 'yes' : 'no'}
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
                <h2>Summary</h2>
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
