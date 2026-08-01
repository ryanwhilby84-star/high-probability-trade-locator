import React from 'react'
import {
  NormalisedSeasonalityChart,
  PriceForecastChart,
  PriceHistoryChart,
  SeasonalPricePathChart,
  SeasonalRoadmapChart,
  SeasonalityCurveChart,
} from './SeasonalityCharts.jsx'
import {
  ROADMAP_HORIZON_WEEKS,
  ROADMAP_METHOD_DESCRIPTION,
  ROADMAP_METHOD_LABEL,
  classifyRoadmapHorizon,
  defaultSeasonalView,
  resolveRoadmapSeriesSource,
} from './roadmapView.js'
import './seasonalityWorkstation.css'

const LOOKBACKS = ['5Y', '10Y', '15Y', '20Y', 'FULL']
const HORIZONS = [4, 8, 12]
const MODELS = [
  { id: 'median', label: 'Median' },
  { id: 'mean', label: 'Average' },
]

function Meta({ label, value }) {
  return (
    <div className="sws-meta-item">
      <label>{label}</label>
      <strong>{value ?? '—'}</strong>
    </div>
  )
}

function dirClass(dir) {
  if (dir === 'UP') return 'sws-bias-bull'
  if (dir === 'DOWN') return 'sws-bias-bear'
  return ''
}

function fmtRetPct(v) {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  const n = Number(v)
  const sign = n > 0 ? '+' : ''
  return `${sign}${n.toFixed(2)}%`
}

function fmtFreq(v) {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  return `${Math.round(Number(v) * 100)}%`
}

function directionClass(label) {
  if (label === 'Bullish') return 'sws-bias-bull'
  if (label === 'Bearish') return 'sws-bias-bear'
  return ''
}

function RoadmapForecastPanel({ roadmap }) {
  const stats = roadmap?.forecast_stats || {}
  return (
    <aside className="sws-side">
      <h3>{ROADMAP_METHOD_LABEL}</h3>
      <p className="sws-muted" style={{ fontSize: '0.68rem', marginBottom: '0.65rem' }}>
        {ROADMAP_METHOD_DESCRIPTION}
      </p>
      <p className="sws-muted" style={{ fontSize: '0.68rem', marginBottom: '0.55rem' }}>
        Horizon stats from historical as-of→horizon returns (not from plotted amplitude).
      </p>
      {ROADMAP_HORIZON_WEEKS.map((w) => {
        const row = stats[`${w}w`] || {}
        const direction = classifyRoadmapHorizon(row)
        return (
          <div className="sws-stat sws-stat-block" key={w}>
            <span>
              {w}-week{' '}
              <strong className={directionClass(direction)} style={{ marginLeft: '0.25rem' }}>
                {direction}
              </strong>
            </span>
            <strong>
              mean {fmtRetPct(row.mean_pct)} · median {fmtRetPct(row.median_pct)}
            </strong>
            <strong>
              bull {fmtFreq(row.bullish_frequency)} · bear {fmtFreq(row.bearish_frequency)} · n=
              {row.n ?? '—'}
            </strong>
          </div>
        )
      })}
      <div className="sws-stat">
        <span>Sample years</span>
        <strong>{roadmap?.sample_size ?? '—'}</strong>
      </div>
      <div className="sws-stat">
        <span>Anchor price</span>
        <strong>
          {roadmap?.asof_price != null || roadmap?.anchor_price != null
            ? Number(roadmap.asof_price ?? roadmap.anchor_price).toFixed(3)
            : '—'}
        </strong>
      </div>
    </aside>
  )
}

function StatsPanel({ panel, walkForward, seasonalView }) {
  if (seasonalView === 'roadmap') return null
  if (!panel) return null
  const bias = panel.current_seasonal_bias
  const biasClass =
    bias === 'BULLISH' ? 'sws-bias-bull' : bias === 'BEARISH' ? 'sws-bias-bear' : ''
  const conf = panel.confidence || {}
  const wf = walkForward || {}
  const h4 = panel.horizon_4w || {}
  const h8 = panel.horizon_8w || {}
  const h12 = panel.horizon_12w || {}

  return (
    <aside className="sws-side">
      <h3>Seasonality Forecast</h3>
      <div className="sws-stat">
        <span>Current Seasonal Bias</span>
        <strong className={biasClass}>{bias}</strong>
      </div>
      <div className="sws-stat">
        <span>4-week expected</span>
        <strong className={dirClass(h4.direction || panel.direction_4w)}>
          {h4.direction || panel.direction_4w || '—'}
          {panel.average_4w_return_pct != null
            ? ` · ${panel.average_4w_return_pct}% median`
            : ''}
        </strong>
      </div>
      <div className="sws-stat">
        <span>8-week expected</span>
        <strong className={dirClass(h8.direction || panel.direction_8w)}>
          {h8.direction || panel.direction_8w || '—'}
          {panel.average_8w_return_pct != null
            ? ` · ${panel.average_8w_return_pct}% median`
            : ''}
        </strong>
      </div>
      <div className="sws-stat">
        <span>12-week expected</span>
        <strong className={dirClass(h12.direction || panel.direction_12w)}>
          {h12.direction || panel.direction_12w || '—'}
          {panel.average_12w_return_pct != null
            ? ` · ${panel.average_12w_return_pct}% median`
            : ''}
        </strong>
      </div>
      <div className="sws-stat">
        <span>Historical positive % (8W)</span>
        <strong>
          {panel.positive_years_pct != null ? `${panel.positive_years_pct}%` : '—'}
        </strong>
      </div>
      <div className="sws-stat">
        <span>Historical negative % (8W)</span>
        <strong>
          {panel.negative_years_pct != null ? `${panel.negative_years_pct}%` : '—'}
        </strong>
      </div>
      <div className="sws-stat">
        <span>Years used</span>
        <strong>{panel.sample_years ?? '—'}</strong>
      </div>
      <div className="sws-stat">
        <span>Walk-forward hit rate (8W)</span>
        <strong>
          {wf.hit_rate != null || panel.walk_forward_hit_rate != null
            ? `${Math.round((wf.hit_rate ?? panel.walk_forward_hit_rate) * 100)}%`
            : '—'}
          {(wf.n ?? panel.walk_forward_n) != null
            ? ` · n=${wf.n ?? panel.walk_forward_n}`
            : ''}
        </strong>
      </div>
      <div className="sws-stat">
        <span>Confidence</span>
        <strong>
          {conf.label || '—'}
          {conf.composite != null ? ` (${conf.composite})` : ''}
        </strong>
      </div>
      <p className="sws-muted" style={{ marginTop: '0.75rem', fontSize: '0.68rem' }}>
        {seasonalView === 'price_path'
          ? 'Seasonal Price Path — mean daily-return cumsum → as-of rebase.'
          : 'Freeze v1.0 — centred seasonal index.'}{' '}
        Separate from Seasonal Roadmap.
      </p>
    </aside>
  )
}

function IntegrityBlock({ marketId, payload, error }) {
  const issues = payload?.integrity?.issues || []
  const warnings = payload?.integrity?.warnings || []
  const quality = payload?.integrity?.data_quality || payload?.data_quality
  return (
    <div className="sws-error">
      <h2>Seasonality unavailable — data quality gate</h2>
      <p>
        {payload?.message ||
          error ||
          'This instrument failed the seasonality integrity audit. No forecast will be invented from poor data.'}
      </p>
      <p className="sws-muted">
        Instrument: <strong>{marketId}</strong>
        {quality ? ` · Quality: ${quality}` : ''}
        {payload?.error ? ` · Code: ${payload.error}` : ''}
      </p>
      {issues.length ? (
        <>
          <p className="sws-muted">Issues</p>
          <ul>
            {issues.map((i) => (
              <li key={i}>{i}</li>
            ))}
          </ul>
        </>
      ) : null}
      {warnings.length ? (
        <>
          <p className="sws-muted">Warnings</p>
          <ul>
            {warnings.map((i) => (
              <li key={i}>{i}</li>
            ))}
          </ul>
        </>
      ) : null}
    </div>
  )
}

export function SeasonalityWorkstation({
  marketId,
  payload,
  lookback,
  onLookback,
  loading,
  error,
}) {
  const [advancedOpen, setAdvancedOpen] = React.useState(false)
  const [seasonalView, setSeasonalView] = React.useState(() =>
    defaultSeasonalView(payload?.display_defaults),
  )
  const [roadmapSmoothed, setRoadmapSmoothed] = React.useState(true)
  const [horizon, setHorizon] = React.useState(12)
  const [model, setModel] = React.useState('median')
  const [showBands, setShowBands] = React.useState(true)
  const [showAverage, setShowAverage] = React.useState(true)
  const [showMedian, setShowMedian] = React.useState(true)
  const [showCurrentYear, setShowCurrentYear] = React.useState(true)
  const [showYears, setShowYears] = React.useState(false)

  React.useEffect(() => {
    // Keep Roadmap as the production default when a fresh payload arrives.
    setSeasonalView(defaultSeasonalView(payload?.display_defaults))
    setRoadmapSmoothed(true)
  }, [payload?.report_date, payload?.instrument_id, payload?.selected_lookback])

  if (error || (payload && payload.status && payload.status !== 'ok')) {
    return <IntegrityBlock marketId={marketId} payload={payload} error={error} />
  }

  if (loading || !payload) {
    return (
      <p className="sws-muted" style={{ padding: '1rem' }}>
        Loading seasonality research…
      </p>
    )
  }

  const conf = payload.confidence || {}
  const normalised = payload.normalised_seasonality || payload.seasonality?.normalised
  const pricePath =
    payload.seasonal_price_path || payload.seasonality?.seasonal_price_path || null
  const roadmap = payload.seasonal_roadmap || payload.seasonality?.seasonal_roadmap || null
  const advanced = payload.advanced || {}
  const forecast = advanced.price_unit_forecast || payload.seasonality?.forecast || {}
  const modelPath = (forecast.models && forecast.models[model]) || []
  const clipped = modelPath.filter((p) => (p.week_offset || 0) <= horizon)
  const upper = (forecast.bands?.upper || []).filter((p) => (p.week_offset || 0) <= horizon)
  const lower = (forecast.bands?.lower || []).filter((p) => (p.week_offset || 0) <= horizon)
  const modelLabel = MODELS.find((m) => m.id === model)?.label || model
  const priceId = payload.price_instrument_id || payload.instrument_id || marketId
  const method = normalised?.method || {}
  const pathMethod = pricePath?.method || {}
  const roadMethod = roadmap?.method || {}
  const roadmapSeries = resolveRoadmapSeriesSource(roadmap, roadmapSmoothed)
  const activeMethodologyLabel =
    seasonalView === 'roadmap'
      ? ROADMAP_METHOD_LABEL
      : seasonalView === 'price_path'
        ? 'Mean-return Path'
        : 'Freeze v1.0 Index'
  const activeDataset =
    seasonalView === 'roadmap'
      ? roadMethod.version || 'seasonal_roadmap_v1'
      : seasonalView === 'price_path'
        ? pathMethod.version || 'seasonal_price_path_v1'
        : method.version || 'freeze_v1.0'
  const activeSourcePath =
    seasonalView === 'roadmap'
      ? roadmapSeries.sourcePath
      : seasonalView === 'price_path'
        ? 'payload.seasonal_price_path.full_year'
        : 'payload.normalised_seasonality.full_year'
  const activeSmooth =
    seasonalView === 'roadmap'
      ? roadmapSmoothed
        ? 'SMA(5)'
        : 'Unsmoothed'
      : seasonalView === 'freeze_index'
        ? `SMA(${method.smooth ?? 5})`
        : 'n/a'
  const anchorPrice =
    roadmap?.asof_price ?? roadmap?.anchor_price ?? payload.anchor?.price ?? null

  return (
    <div
      className="sws-body"
      data-active-seasonal-view={seasonalView}
      data-active-dataset={activeDataset}
      data-active-source={activeSourcePath || ''}
      data-active-methodology={activeMethodologyLabel}
    >
      <div className="sws-main">
        <div className="sws-header-meta">
          <Meta label="Instrument" value={payload.instrument_id || marketId} />
          <Meta label="Price series" value={priceId} />
          <Meta label="Exchange" value={payload.exchange || '—'} />
          <Meta label="Report Date" value={payload.report_date} />
          <Meta label="Lookback" value={payload.selected_lookback} />
          <Meta label="Sample Size" value={`${payload.sample_size} years`} />
          <Meta label="Methodology" value={activeMethodologyLabel} />
          <Meta
            label="Anchor price"
            value={anchorPrice != null ? Number(anchorPrice).toFixed(3) : '—'}
          />
          <Meta label="Series" value={activeSmooth} />
          <Meta label="Data Quality" value={payload.data_quality} />
        </div>

        <div className="sws-controls">
          <div className="sws-lookbacks" role="group" aria-label="Lookback">
            {LOOKBACKS.map((lb) => (
              <button
                key={lb}
                type="button"
                className={`sws-btn${lookback === lb ? ' is-active' : ''}`}
                onClick={() => onLookback?.(lb)}
              >
                {lb === 'FULL' ? 'All' : lb}
              </button>
            ))}
          </div>
          <div className="sws-lookbacks" role="group" aria-label="Seasonal view">
            <button
              type="button"
              className={`sws-btn${seasonalView === 'roadmap' ? ' is-active' : ''}`}
              onClick={() => setSeasonalView('roadmap')}
            >
              {ROADMAP_METHOD_LABEL}
            </button>
            <button
              type="button"
              className={`sws-btn${seasonalView === 'price_path' ? ' is-active' : ''}`}
              onClick={() => setSeasonalView('price_path')}
            >
              Mean-return Path
            </button>
            <button
              type="button"
              className={`sws-btn${seasonalView === 'freeze_index' ? ' is-active' : ''}`}
              onClick={() => setSeasonalView('freeze_index')}
            >
              Freeze v1.0 Index
            </button>
          </div>
          {seasonalView === 'roadmap' ? (
            <div className="sws-lookbacks" role="group" aria-label="Roadmap smooth">
              <button
                type="button"
                className={`sws-btn${roadmapSmoothed ? ' is-active' : ''}`}
                onClick={() => setRoadmapSmoothed(true)}
              >
                SMA(5)
              </button>
              <button
                type="button"
                className={`sws-btn${!roadmapSmoothed ? ' is-active' : ''}`}
                onClick={() => setRoadmapSmoothed(false)}
              >
                Unsmoothed
              </button>
            </div>
          ) : null}
          <button
            type="button"
            className={`sws-btn${advancedOpen ? ' is-active' : ''}`}
            onClick={() => setAdvancedOpen((v) => !v)}
          >
            Advanced
          </button>
        </div>

        <section className="sws-pane">
          <h3>Price</h3>
          <p className="sws-muted">
            Actual market closes only. Separate from the seasonal views below.
          </p>
          <PriceHistoryChart
            priceSeries={payload.price_series}
            anchorDate={payload.anchor?.date}
          />
        </section>

        {seasonalView === 'roadmap' ? (
          <section className="sws-pane sws-pane-primary">
            <h3>{ROADMAP_METHOD_LABEL}</h3>
            <p className="sws-muted">{ROADMAP_METHOD_DESCRIPTION}</p>
            <p className="sws-muted">
              Grey ≤ today · Blue &gt; today · price units · anchor{' '}
              {anchorPrice != null ? Number(anchorPrice).toFixed(3) : '—'} ·{' '}
              {roadmapSmoothed ? 'SMA(5) series' : 'Unsmoothed series'} ·{' '}
              {roadMethod.lookback_years ?? 15}Y.
            </p>
            <SeasonalRoadmapChart
              key={`chart-seasonal-roadmap-${roadmapSmoothed ? 'sma5' : 'raw'}`}
              roadmap={roadmap}
              anchorDate={payload.anchor?.date || roadmap?.asof}
              anchorPrice={anchorPrice}
              useSmoothed={roadmapSmoothed}
            />
          </section>
        ) : null}

        {seasonalView === 'price_path' ? (
          <section className="sws-pane sws-pane-primary">
            <h3>Mean-return Path</h3>
            <p className="sws-muted">
              Secondary comparison · avg daily returns → cumulate → as-of rebase ·{" "}
              {pathMethod.lookback_years ?? 15}Y · price units.
            </p>
            <SeasonalPricePathChart
              key="chart-mean-return-path"
              pricePath={pricePath}
              anchorDate={payload.anchor?.date || pricePath?.asof}
            />
          </section>
        ) : null}

        {seasonalView === 'freeze_index' ? (
          <section className="sws-pane sws-pane-primary">
            <h3>Normalised seasonal curve (Freeze v1.0)</h3>
            <p className="sws-muted">
              Grey ≤ today · Red forward · centred % index · SMA {method.smooth ?? 5} ·{" "}
              {method.lookback_years ?? 15}Y. Not a price path.
            </p>
            <NormalisedSeasonalityChart
              key="chart-freeze-v1-index"
              normalised={normalised}
              anchorDate={payload.anchor?.date || normalised?.asof}
            />
          </section>
        ) : null}

        {advancedOpen ? (
          <>
            <section className="sws-pane">
              <h3>Advanced — price-unit seasonal forecast</h3>
              <p className="sws-muted">
                Optional ISO-week return model projected in price units. Separate from the
                normalised indexed curve above — do not treat these as the same chart.
              </p>
              <div className="sws-controls" style={{ marginBottom: '0.4rem' }}>
                <div className="sws-lookbacks" role="group" aria-label="Forecast horizon">
                  {HORIZONS.map((h) => (
                    <button
                      key={h}
                      type="button"
                      className={`sws-btn${horizon === h ? ' is-active' : ''}`}
                      onClick={() => setHorizon(h)}
                    >
                      {h}W
                    </button>
                  ))}
                </div>
                <div className="sws-lookbacks" role="group" aria-label="Model">
                  {MODELS.map((m) => (
                    <button
                      key={m.id}
                      type="button"
                      className={`sws-btn${model === m.id ? ' is-active' : ''}`}
                      onClick={() => setModel(m.id)}
                    >
                      {m.label}
                    </button>
                  ))}
                </div>
                <label>
                  <input
                    type="checkbox"
                    checked={showBands}
                    onChange={(e) => setShowBands(e.target.checked)}
                  />
                  Confidence band
                </label>
              </div>
              <PriceForecastChart
                priceSeries={payload.price_series}
                forecastPath={clipped}
                upperBand={upper}
                lowerBand={lower}
                showBands={showBands}
                anchorDate={payload.anchor?.date || forecast.start_date}
                confidence={conf.label}
                sampleSize={payload.sample_size}
                modelLabel={modelLabel}
              />
            </section>

            <section className="sws-pane">
              <h3>Advanced — legacy week study</h3>
              <div className="sws-controls" style={{ marginBottom: '0.4rem' }}>
                <label>
                  <input
                    type="checkbox"
                    checked={showAverage}
                    onChange={(e) => setShowAverage(e.target.checked)}
                  />
                  Trimmed mean
                </label>
                <label>
                  <input
                    type="checkbox"
                    checked={showMedian}
                    onChange={(e) => setShowMedian(e.target.checked)}
                  />
                  Median
                </label>
                <label>
                  <input
                    type="checkbox"
                    checked={showCurrentYear}
                    onChange={(e) => setShowCurrentYear(e.target.checked)}
                  />
                  Current year
                </label>
                <label>
                  <input
                    type="checkbox"
                    checked={showYears}
                    onChange={(e) => setShowYears(e.target.checked)}
                  />
                  Individual years
                </label>
              </div>
              <SeasonalityCurveChart
                payload={payload}
                showAverage={showAverage}
                showMedian={showMedian}
                showCurrentYear={showCurrentYear}
                showBands={showBands}
                showYears={showYears}
              />
            </section>

            <section className="sws-pane">
              <h3>Advanced — confidence factors</h3>
              <div className="sws-controls">
                {Object.entries(conf.factors || {}).map(([k, v]) => (
                  <span key={k} className="sws-muted">
                    {k.replace(/_/g, ' ')}: <strong style={{ color: '#e2e8f0' }}>{v}</strong>
                  </span>
                ))}
              </div>
              {advanced.note ? <p className="sws-muted">{advanced.note}</p> : null}
            </section>
          </>
        ) : null}
      </div>
      {seasonalView === 'roadmap' ? (
        <RoadmapForecastPanel roadmap={roadmap} />
      ) : (
        <StatsPanel
          panel={payload.stats_panel}
          walkForward={payload.walk_forward}
          seasonalView={seasonalView}
        />
      )}
    </div>
  )
}
