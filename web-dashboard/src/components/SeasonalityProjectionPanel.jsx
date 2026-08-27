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

import {
  buildSeasonalityConfidenceClarity,
  CONFIDENCE_TOOLTIPS,
} from '../seasonality/seasonalityConfidenceInterpret.js'
import {
  WEEK_TICKS,
  buildDivergenceLabel,
  buildHorizonCards,
  buildMetadata,
  buildProjectionChartRows,
  isNum,
  weekTickLabel,
} from '../seasonality/seasonalityProjectionModel.js'
import { auditPriceSourceContract } from '../seasonality/priceSourceContract.js'

const fmtIdx = (v) => (isNum(v) ? v.toFixed(1) : '—')

function ProjectionTooltip({ active, payload, currentWeek, currentYear }) {
  if (!active || !payload?.length) return null
  const p = payload[0]?.payload
  if (!p) return null
  const atNow = p.week === currentWeek
  return (
    <div className="sea-price-tooltip sea-proj-tooltip">
      <div className="sea-price-tip-date">
        ISO week {p.week}
        {atNow ? ' · Current week' : ''}
        {p.week > currentWeek ? ' · projected' : p.week <= currentWeek && isNum(p.currentYearPath) ? ' · actual YTD' : ''}
      </div>
      {isNum(p.seasonal_10y) ? <div>10Y seasonal avg: {fmtIdx(p.seasonal_10y)}</div> : null}
      {isNum(p.seasonal_5y) ? <div>5Y seasonal avg: {fmtIdx(p.seasonal_5y)}</div> : null}
      {isNum(p.seasonal_3y) ? <div>3Y seasonal avg: {fmtIdx(p.seasonal_3y)}</div> : null}
      {isNum(p.currentYearPath) ? <div>{currentYear} indexed: {fmtIdx(p.currentYearPath)}</div> : null}
      {isNum(p.forwardSeasonalPath) ? <div>Forward projection: {fmtIdx(p.forwardSeasonalPath)}</div> : null}
      {isNum(p.divergencePts) && atNow ? (
        <div className="sea-proj-tip-div">
          vs 10Y at this week: {p.divergencePts >= 0 ? '+' : ''}
          {p.divergencePts.toFixed(1)} pts
        </div>
      ) : null}
    </div>
  )
}

function ClarityRow({ label, summary, tooltip, level }) {
  const tone = String(level || '').toLowerCase()
  return (
    <div className={`sea-proj-clarity-row sea-proj-clarity-row--${tone}`}>
      <dt title={tooltip}>{label}</dt>
      <dd>{summary}</dd>
    </div>
  )
}

function ConfidenceClarityPanel({ clarity }) {
  if (!clarity) return null
  const { dataQuality, pathAgreement, forward8w, tradeUsefulness, tooltips } = clarity
  return (
    <section className="sea-proj-clarity" aria-label="Seasonality confidence breakdown">
      <h3 className="sea-proj-section-title">Confidence breakdown</h3>
      <dl className="sea-proj-clarity-grid">
        <ClarityRow
          label="Data quality"
          summary={dataQuality.summary}
          level={dataQuality.level}
          tooltip={tooltips.dataQuality}
        />
        <ClarityRow
          label="Seasonal path agreement"
          summary={pathAgreement.summary}
          level={pathAgreement.level}
          tooltip={tooltips.pathAgreement}
        />
        <ClarityRow
          label="Forward window read (8W)"
          summary={forward8w.summary}
          level={forward8w.level}
          tooltip={tooltips.forwardWindow}
        />
        <ClarityRow
          label="Trade usefulness"
          summary={tradeUsefulness.summary}
          level={tradeUsefulness.level}
          tooltip={tooltips.tradeUsefulness}
        />
      </dl>
      <p className="sea-proj-clarity-note">
        <span title={tooltips.strongNotGuaranteed}>Strong path agreement ≠ guaranteed move.</span>{' '}
        <span title={tooltips.weakNotUnusable}>
          Weak path agreement ≠ unusable — a forward window can still show a medium-quality read.
        </span>
      </p>
    </section>
  )
}

function HorizonCard({ card }) {
  const windowConfTooltip = CONFIDENCE_TOOLTIPS.forwardWindow
  return (
    <article className="sea-proj-window-card">
      <h4 className="sea-proj-window-title">{card.key} forward window read</h4>
      <dl className="sea-proj-window-stats">
        <div>
          <dt>Direction</dt>
          <dd>
            <span className={`sea-proj-dir sea-proj-dir--${card.directionTone}`}>{card.direction}</span>
          </dd>
        </div>
        <div>
          <dt>Avg return</dt>
          <dd>{card.avgReturn}</dd>
        </div>
        <div>
          <dt>Median return</dt>
          <dd>{card.medianReturn}</dd>
        </div>
        <div>
          <dt>Win rate</dt>
          <dd>{card.winRate}</dd>
        </div>
        <div>
          <dt>Sample years</dt>
          <dd>{card.sampleYears}</dd>
        </div>
        <div>
          <dt title={windowConfTooltip}>Window read confidence</dt>
          <dd>{card.windowReadConfidence ?? card.confidence}</dd>
        </div>
      </dl>
    </article>
  )
}

function MetadataFooter({ meta }) {
  return (
    <footer className="sea-proj-meta">
      <dl className="sea-proj-meta-grid">
        <div>
          <dt>Data source</dt>
          <dd>{meta.dataSource}</dd>
        </div>
        <div>
          <dt>Canonical</dt>
          <dd>
            {meta.canonicalSource}
            {meta.canonicalSymbol !== '—' ? ` · ${meta.canonicalSymbol}` : ''}
          </dd>
        </div>
        <div>
          <dt>History years</dt>
          <dd>{meta.historyYears}</dd>
        </div>
        <div>
          <dt>Trust grade</dt>
          <dd>{meta.trustGrade}</dd>
        </div>
        <div>
          <dt>8W sample years</dt>
          <dd>{meta.sampleYears8w}</dd>
        </div>
        <div>
          <dt>Last updated</dt>
          <dd>{meta.lastUpdated}</dd>
        </div>
      </dl>
      {meta.trustNotes ? <p className="sea-proj-meta-note">{meta.trustNotes}</p> : null}
    </footer>
  )
}

/** Unified seasonality projection chart — historical path, YTD actual, Bernd-style forward projection. */
export function SeasonalityProjectionPanel({
  block,
  toggles,
  generatedAt = null,
  marketId = null,
  cotBlock = null,
}) {
  const currentWeek = isNum(block?.current_week) ? block.current_week : 1
  const currentYear = block?.current_year ?? new Date().getFullYear()
  const chartRows = React.useMemo(
    () => buildProjectionChartRows(block, toggles),
    [block, toggles],
  )

  const clarity = React.useMemo(() => buildSeasonalityConfidenceClarity(block), [block])
  const divergence = React.useMemo(() => buildDivergenceLabel(block), [block])
  const horizonCards = React.useMemo(() => buildHorizonCards(block, toggles), [block, toggles])
  const meta = React.useMemo(() => buildMetadata(block, generatedAt), [block, generatedAt])

  const contract = React.useMemo(() => {
    if (!cotBlock || !marketId) return null
    return auditPriceSourceContract(cotBlock, block, marketId)
  }, [cotBlock, block, marketId])

  const showSeasonalBaselines = !toggles?.currentYearOnly
  const show3y = showSeasonalBaselines && toggles?.show3y
  const show5y = showSeasonalBaselines && toggles?.show5y
  const show10y = showSeasonalBaselines && (toggles?.show10y !== false)

  const latestPrice = block?.latest_price
  const grade = block?.trust_grade || 'C'
  const showReliabilityBanner =
    grade !== 'A' || (block?.forward_read?.next_8w?.sample_years ?? 0) < 5

  if (!chartRows.length) {
    return <p className="sea-v2-outcome-empty">Seasonality chart data unavailable for this market.</p>
  }

  return (
    <div className="sea-proj-panel">
      <ConfidenceClarityPanel clarity={clarity} />

      {block?.data_quality_warning ? (
        <p className="sea-proj-reliability-banner" role="status">
          Data reliability: {block.data_quality_warning}
        </p>
      ) : null}

      {showReliabilityBanner ? (
        <p className="sea-proj-reliability-banner">
          {grade === 'C'
            ? 'Insufficient seasonal history — directional labels suppressed.'
            : grade === 'B'
              ? 'Context only — sparse curve; do not treat as a high-confidence signal.'
              : (block?.forward_read?.next_8w?.sample_years ?? 0) < 5
                ? 'Low sample reliability — forward windows have fewer than 5 sample years.'
                : null}
        </p>
      ) : null}

      <p className="sea-proj-divergence" role="note">
        <span className="sea-proj-divergence-label">Path read:</span> {divergence}
        {isNum(latestPrice?.index) ? (
          <span className="sea-proj-now-price">
            · Now: {fmtIdx(latestPrice.index)} indexed (week {currentWeek})
          </span>
        ) : null}
      </p>

      {contract?.timelineHidden ? (
        <p className="sea-proj-contract-warn" role="alert">
          Price source mismatch vs COT panel — seasonality chart still shown from its own canonical timeline.
        </p>
      ) : null}

      <div className="sea-proj-chart-wrap">
        <h3 className="sea-proj-chart-title">
          Seasonal path vs {currentYear} · indexed to 100 at week 1
        </h3>
        <ResponsiveContainer width="100%" height={340}>
          <LineChart data={chartRows} margin={{ top: 20, right: 24, left: 8, bottom: 8 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.2)" />
            <XAxis
              dataKey="week"
              type="number"
              domain={[1, 52]}
              ticks={WEEK_TICKS}
              tickFormatter={weekTickLabel}
              tick={{ fontSize: 11, fill: '#94a3b8' }}
            />
            <YAxis
              tick={{ fontSize: 11, fill: '#94a3b8' }}
              width={48}
              tickFormatter={fmtIdx}
              domain={['auto', 'auto']}
            />
            <Tooltip content={<ProjectionTooltip currentWeek={currentWeek} currentYear={currentYear} />} />
            <Legend wrapperStyle={{ fontSize: 12, paddingTop: 6 }} />

            <ReferenceLine y={100} stroke="#475569" strokeDasharray="2 4" strokeOpacity={0.6} />
            <ReferenceLine
              x={currentWeek}
              stroke="#f1f5f9"
              strokeWidth={2}
              strokeDasharray="4 3"
              label={{
                value: 'Current week',
                position: 'insideTopLeft',
                fontSize: 11,
                fill: '#e2e8f0',
                offset: 4,
              }}
            />

            {show10y ? (
              <Line
                type="linear"
                dataKey="seasonal_10y"
                name="10Y seasonal average"
                stroke="#3b82f6"
                strokeWidth={2}
                strokeDasharray="8 4"
                dot={false}
                connectNulls={false}
                isAnimationActive={false}
              />
            ) : null}
            {show5y ? (
              <Line
                type="linear"
                dataKey="seasonal_5y"
                name="5Y seasonal average"
                stroke="#60a5fa"
                strokeWidth={1.5}
                strokeDasharray="6 4"
                dot={false}
                connectNulls={false}
                isAnimationActive={false}
              />
            ) : null}
            {show3y ? (
              <Line
                type="linear"
                dataKey="seasonal_3y"
                name="3Y seasonal average"
                stroke="#93c5fd"
                strokeWidth={1.5}
                strokeDasharray="6 4"
                dot={false}
                connectNulls={false}
                isAnimationActive={false}
              />
            ) : null}

            <Line
              type="linear"
              dataKey="currentYearPath"
              name={`${currentYear} actual (YTD)`}
              stroke="#2dd4bf"
              strokeWidth={2.5}
              dot={{ r: 2, fill: '#2dd4bf' }}
              activeDot={{ r: 5 }}
              connectNulls
              isAnimationActive={false}
            />

            {toggles?.forwardProjection !== false ? (
              <Line
                type="linear"
                dataKey="forwardSeasonalPath"
                name="Forward seasonal projection"
                stroke="#fbbf24"
                strokeWidth={2}
                strokeDasharray="5 4"
                dot={false}
                connectNulls
                isAnimationActive={false}
              />
            ) : null}
          </LineChart>
        </ResponsiveContainer>
        <p className="sea-proj-chart-legend-note">
          Solid teal = actual {currentYear} path through week {currentWeek}. Gold dashed line = historical seasonal
          shape projected forward from today&apos;s indexed level (not a price forecast).
        </p>
      </div>

      <div className="sea-proj-windows">
        <h3 className="sea-proj-section-title">Forward window reads (4W / 8W / 12W)</h3>
        <p className="sea-proj-window-intro">
          Each card is a historical return read for that horizon — not path agreement. Window read confidence
          measures sample depth and return consistency for that window only.
        </p>
        <div className="sea-proj-window-grid">
          {horizonCards.map((card) => (
            <HorizonCard key={card.key} card={card} />
          ))}
        </div>
      </div>

      <MetadataFooter meta={meta} />
    </div>
  )
}

export class SeasonalityProjectionErrorBoundary extends React.Component {
  constructor(props) {
    super(props)
    this.state = { error: null }
  }

  static getDerivedStateFromError(error) {
    return { error }
  }

  componentDidCatch(error, info) {
    console.error('SeasonalityProjectionPanel render failed:', error, info)
  }

  render() {
    if (this.state.error) {
      return (
        <p className="sea-v2-outcome-empty">
          Seasonality projection could not render — {this.state.error.message || 'unknown error'}.
        </p>
      )
    }
    return this.props.children
  }
}
