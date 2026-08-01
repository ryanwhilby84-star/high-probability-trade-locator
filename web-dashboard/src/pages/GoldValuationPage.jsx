import React from 'react'
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
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
  navigateToScanner,
} from '../routing.js'
import './goldValuation.css'

const MARKET = 'Gold'
const DXY_MARKET = 'Broad US Dollar Index — DTWEXBGS'
const SILVER_MARKET = 'Silver'

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
  return `${sign}${n.toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })}`
}

function biasTone(bias) {
  const b = String(bias || '').toLowerCase()
  if (b.includes('under') || b.includes('bull')) return 'bull'
  if (b.includes('over') || b.includes('bear')) return 'bear'
  return 'neutral'
}

function SummaryCard({ label, value, sub, tone }) {
  return (
    <article className={`gld-summary-card gld-summary-card--${tone || 'neutral'}`}>
      <span className="gld-summary-label">{label}</span>
      <strong className="gld-summary-value">{value}</strong>
      {sub ? <span className="gld-summary-sub">{sub}</span> : null}
    </article>
  )
}

function weeklyCloses(rec) {
  const bars = rec?.weekly_ohlc || rec?.aligned_weekly_ohlc || []
  return bars
    .map((b) => {
      const d = String(b?.date || '').slice(0, 10)
      const c = Number(b?.close)
      if (!d || !Number.isFinite(c)) return null
      return { date: d, close: c }
    })
    .filter(Boolean)
}

function buildChartRows(goldBars, silverBars, dxyBars, fairValue) {
  const silverMap = new Map(silverBars.map((b) => [b.date, b.close]))
  const dxyMap = new Map(dxyBars.map((b) => [b.date, b.close]))
  const window = goldBars.length > 260 ? goldBars.slice(-260) : goldBars
  if (!window.length) return []

  const gold0 = window[0].close
  const silver0 = silverMap.get(window[0].date) ?? silverBars.find((b) => b.date >= window[0].date)?.close
  const scale = gold0 && silver0 ? gold0 / silver0 : null

  return window.map((g) => {
    const silver = silverMap.get(g.date)
    const dxy = dxyMap.get(g.date)
    return {
      date: g.date,
      gold: g.close,
      fair: fairValue != null && Number.isFinite(Number(fairValue)) ? Number(fairValue) : null,
      dxy: dxy != null ? dxy : null,
      silverScaled: silver != null && scale != null ? silver * scale : null,
      silverRaw: silver != null ? silver : null,
    }
  })
}

function ValuationScale({ deviationPct, state }) {
  const dev = Number(deviationPct)
  const pct = Number.isFinite(dev) ? Math.max(0, Math.min(100, 50 + dev)) : 50
  return (
    <section className="gld-scale" aria-label="Valuation scale">
      <header className="gld-section-head">
        <h2>Valuation Scale</h2>
        <span className="gld-scale-band">{state || '—'}</span>
      </header>
      <div className="gld-scale-track">
        <div className="gld-scale-gradient" />
        <div className="gld-scale-marker" style={{ left: `${pct}%` }}>
          <span className="gld-scale-pin" />
          <span className="gld-scale-pin-label">Today {fmtSigned(deviationPct, 2)}%</span>
        </div>
      </div>
      <div className="gld-scale-labels">
        <span>Undervalued</span>
        <span>Fair Value</span>
        <span>Overvalued</span>
      </div>
    </section>
  )
}

function ChartPanel({ rows, fairValue }) {
  if (!rows.length) {
    return (
      <div className="gld-chart-empty">
        Weekly Gold OHLC unavailable — cannot render valuation board chart.
      </div>
    )
  }

  return (
    <div className="gld-chart-wrap">
      <ResponsiveContainer width="100%" height={420}>
        <LineChart data={rows} margin={{ top: 12, right: 56, left: 8, bottom: 8 }}>
          <CartesianGrid stroke="rgba(148,163,184,0.12)" strokeDasharray="3 6" />
          <XAxis
            dataKey="date"
            tick={{ fill: '#94a3b8', fontSize: 11 }}
            minTickGap={48}
            tickFormatter={(v) => String(v).slice(0, 7)}
          />
          <YAxis
            yAxisId="gold"
            tick={{ fill: '#fbbf24', fontSize: 11 }}
            width={56}
            domain={['auto', 'auto']}
            tickFormatter={(v) => Number(v).toFixed(0)}
            label={{ value: 'Gold ($)', angle: -90, position: 'insideLeft', fill: '#fbbf24', fontSize: 11 }}
          />
          <YAxis
            yAxisId="dxy"
            orientation="right"
            tick={{ fill: '#38bdf8', fontSize: 11 }}
            width={48}
            domain={['auto', 'auto']}
            tickFormatter={(v) => Number(v).toFixed(1)}
            label={{ value: 'DXY', angle: 90, position: 'insideRight', fill: '#38bdf8', fontSize: 11 }}
          />
          <Tooltip
            contentStyle={{
              background: '#0b1220',
              border: '1px solid rgba(148,163,184,0.25)',
              borderRadius: 8,
            }}
            labelStyle={{ color: '#e2e8f0' }}
            formatter={(value, name) => {
              if (value == null || !Number.isFinite(Number(value))) return ['—', name]
              if (name === 'gold') return [fmt(value, 2), 'Gold']
              if (name === 'fair') return [fmt(value, 2), 'Fair value']
              if (name === 'dxy') return [fmt(value, 2), 'DXY (broad USD)']
              if (name === 'silverScaled') return [fmt(value, 2), 'Silver (rescaled to Gold)']
              return [fmt(value, 2), name]
            }}
          />
          <Legend
            wrapperStyle={{ color: '#cbd5e1', paddingTop: 8 }}
            formatter={(value) => {
              if (value === 'gold') return 'Gold price'
              if (value === 'fair') return 'Fair value (current)'
              if (value === 'dxy') return 'DXY (right axis)'
              if (value === 'silverScaled') return 'Silver (rescaled)'
              return value
            }}
          />
          <Line
            yAxisId="gold"
            type="monotone"
            dataKey="gold"
            name="gold"
            stroke="#fbbf24"
            strokeWidth={2.25}
            dot={false}
            isAnimationActive={false}
          />
          {fairValue != null && Number.isFinite(Number(fairValue)) ? (
            <>
              <Line
                yAxisId="gold"
                type="monotone"
                dataKey="fair"
                name="fair"
                stroke="#a78bfa"
                strokeWidth={2}
                strokeDasharray="7 4"
                dot={false}
                isAnimationActive={false}
              />
              <ReferenceLine
                yAxisId="gold"
                y={Number(fairValue)}
                stroke="#a78bfa"
                strokeDasharray="2 4"
                strokeOpacity={0.35}
              />
            </>
          ) : null}
          <Line
            yAxisId="dxy"
            type="monotone"
            dataKey="dxy"
            name="dxy"
            stroke="#38bdf8"
            strokeWidth={1.75}
            dot={false}
            isAnimationActive={false}
            connectNulls
          />
          <Line
            yAxisId="gold"
            type="monotone"
            dataKey="silverScaled"
            name="silverScaled"
            stroke="#94a3b8"
            strokeWidth={1.4}
            strokeDasharray="4 3"
            dot={false}
            isAnimationActive={false}
            connectNulls
          />
        </LineChart>
      </ResponsiveContainer>
      <p className="gld-chart-note">
        Gold and fair value on the left axis. DXY (Fed broad USD / DTWEXBGS) on the right axis so it
        does not squash Gold. Silver is rescaled to Gold&apos;s start level for relative comparison
        only — not a fair-value input.
      </p>
    </div>
  )
}

export function GoldValuationPage({ sidebarClass, onSidebarClass }) {
  const [metalsDoc, setMetalsDoc] = React.useState(null)
  const [ohlcDoc, setOhlcDoc] = React.useState(null)
  const [error, setError] = React.useState(null)
  const [loading, setLoading] = React.useState(true)
  const canonical = useCanonicalCurrentPrice(MARKET)

  React.useEffect(() => {
    let active = true
    setLoading(true)
    Promise.all([
      fetchPublicJson('/data/metals_valuation_latest.json'),
      fetchPublicJson('/data/workstation_ohlc_latest.json'),
    ])
      .then(([metals, ohlc]) => {
        if (!active) return
        setMetalsDoc(metals)
        setOhlcDoc(ohlc)
        setError(null)
      })
      .catch((err) => {
        if (!active) return
        setError(String(err?.message || err))
        setMetalsDoc(null)
        setOhlcDoc(null)
      })
      .finally(() => {
        if (active) setLoading(false)
      })
    return () => {
      active = false
    }
  }, [])

  const inst = metalsDoc?.instruments?.[MARKET] || {}
  const instruments = ohlcDoc?.instruments || {}
  const goldBars = React.useMemo(() => weeklyCloses(instruments[MARKET]), [instruments])
  const silverBars = React.useMemo(() => weeklyCloses(instruments[SILVER_MARKET]), [instruments])
  const dxyBars = React.useMemo(() => weeklyCloses(instruments[DXY_MARKET]), [instruments])
  const chartRows = React.useMemo(
    () => buildChartRows(goldBars, silverBars, dxyBars, inst.fair_value),
    [goldBars, silverBars, dxyBars, inst.fair_value],
  )

  const tone = biasTone(inst.valuation_bias || inst.valuation_state)
  const wired = inst.wired === true
  const drivers = inst.drivers || {}

  return (
    <AppShell
      title="Gold Valuation"
      subtitle="Metals real-yield + DXY fair value"
      sidebarClass={sidebarClass}
      onSidebarClass={onSidebarClass}
      topActions={
        <div className="gld-top-actions">
          <button type="button" className="ws-btn" onClick={() => navigateToInstrument(MARKET)}>
            ← Back to Gold
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
      <div className="gld-page">
        <header className="gld-hero">
          <p className="gld-eyebrow">HPTL · Metals · {inst.model_id || 'metals_real_yield_v1'}</p>
          <h1>Gold Valuation Board</h1>
          <p className="gld-hero-sub">
            Macro fair value from 10Y real yield and broad USD (DXY). Chart overlays Gold price,
            current fair value, DXY, and Silver for visual context.
          </p>
        </header>

        {loading ? <div className="gld-loading">Loading Gold valuation…</div> : null}
        {error ? (
          <div className="gld-error">
            Could not load metals valuation or workstation OHLC.
            <div className="gld-error-detail">{error}</div>
          </div>
        ) : null}

        {!loading && !error ? (
          <>
            <section className="gld-summary-grid" aria-label="Gold valuation summary">
              <SummaryCard
                label="Gold Price"
                value={fmt(canonical.price ?? null, 2)}
                sub={`USD / oz · ${canonical.label}${
                  inst.spot_price != null &&
                  canonical.price != null &&
                  Math.abs(Number(inst.spot_price) - Number(canonical.price)) > 0.5
                    ? ` · model spot ${fmt(inst.spot_price, 2)} (valuation only)`
                    : ''
                }`}
                tone="neutral"
              />
              <SummaryCard
                label="Fair Value"
                value={wired ? fmt(inst.fair_value, 2) : '—'}
                sub={inst.model_id || 'metals_real_yield_v1'}
                tone="neutral"
              />
              <SummaryCard
                label="Premium / Discount"
                value={wired ? `${fmtSigned(inst.deviation_pct, 2)}%` : '—'}
                sub={wired ? (Number(inst.deviation_pct) > 0 ? 'Premium to FV' : 'Discount to FV') : 'Unavailable'}
                tone={tone}
              />
              <SummaryCard
                label="Valuation"
                value={inst.valuation_state || inst.valuation_bias || 'Unavailable'}
                sub={inst.confidence ? `Confidence ${inst.confidence}` : '—'}
                tone={tone}
              />
              <SummaryCard
                label="Real Yield (10Y)"
                value={drivers.real_yield_10y != null ? fmt(drivers.real_yield_10y, 2) : '—'}
                sub="DFII10"
                tone="neutral"
              />
              <SummaryCard
                label="DXY (broad USD)"
                value={drivers.dxy_broad != null ? fmt(drivers.dxy_broad, 2) : '—'}
                sub="DTWEXBGS"
                tone="neutral"
              />
            </section>

            <section className="gld-chart-section">
              <header className="gld-section-head">
                <h2>Gold · Fair Value · DXY · Silver</h2>
                <span className="gld-chart-legend-hint">
                  {goldBars.length} Gold weeks · {dxyBars.length} DXY weeks · {silverBars.length}{' '}
                  Silver weeks
                </span>
              </header>
              <ChartPanel rows={chartRows} fairValue={wired ? inst.fair_value : null} />
            </section>

            {wired ? (
              <ValuationScale
                deviationPct={inst.deviation_pct}
                state={inst.valuation_state || inst.valuation_bias}
              />
            ) : null}

            <section className="gld-drivers-section">
              <header className="gld-section-head">
                <h2>Driver Summary</h2>
                <span className="gld-section-meta">
                  {inst.trust_grade ? `Trust ${inst.trust_grade}` : '—'}
                  {inst.regression?.r_squared != null ? ` · R² ${inst.regression.r_squared}` : ''}
                  {inst.regression?.n != null ? ` · n=${inst.regression.n}` : ''}
                </span>
              </header>
              <div className="gld-driver-grid">
                <article className="gld-driver-card">
                  <h3>Real yield</h3>
                  <p>
                    10Y TIPS real yield{' '}
                    <strong>
                      {drivers.real_yield_10y != null ? `${fmt(drivers.real_yield_10y, 2)}%` : '—'}
                    </strong>
                  </p>
                </article>
                <article className="gld-driver-card">
                  <h3>DXY / Broad USD</h3>
                  <p>
                    Fed broad dollar index{' '}
                    <strong>{drivers.dxy_broad != null ? fmt(drivers.dxy_broad, 2) : '—'}</strong>
                  </p>
                </article>
                <article className="gld-driver-card">
                  <h3>Model</h3>
                  <p>{inst.valuation_reason || inst.model_note || '—'}</p>
                </article>
              </div>
            </section>

            <section className="gld-narrative">
              <header className="gld-section-head">
                <h2>Summary</h2>
              </header>
              <p className="gld-narrative-text">
                {inst.driver_summary ||
                  (wired
                    ? 'Gold metals valuation is wired.'
                    : 'Gold valuation unavailable — metals export not wired for this instrument.')}
              </p>
              {inst.as_of_week || metalsDoc?.generated_at ? (
                <p className="gld-model-note">
                  As of {inst.as_of_week || '—'} · Export{' '}
                  {String(metalsDoc?.generated_at || '').slice(0, 16).replace('T', ' ')}
                </p>
              ) : null}
            </section>
          </>
        ) : null}
      </div>
    </AppShell>
  )
}

export default GoldValuationPage
