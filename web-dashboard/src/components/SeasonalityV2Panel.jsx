import React from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)
const fmtIdx = (v) => (isNum(v) ? v.toFixed(1) : '—')
const fmtPct = (v) => (isNum(v) ? `${v >= 0 ? '+' : ''}${v.toFixed(2)}%` : '—')

const WEEK_TICKS = [1, 5, 9, 14, 18, 23, 27, 31, 36, 40, 45, 49, 52]
const MONTH_LABELS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', '']

function weekTickLabel(week) {
  const monthIdx = [1, 5, 9, 14, 18, 23, 27, 31, 36, 40, 45, 49]
  const i = monthIdx.indexOf(week)
  return i >= 0 ? MONTH_LABELS[i] : week === 52 ? 'Dec' : ''
}

/** Display-only confidence for forward horizons (not used for scoring). */
export function horizonDisplayConfidence(row) {
  if (!row?.available) return '—'
  const n = row.sample_years ?? 0
  const wr = row.win_rate_pct
  const avg = row.avg_return_pct
  if (n >= 10 && isNum(avg) && Math.abs(avg) >= 0.5 && isNum(wr) && (wr >= 60 || wr <= 40)) return 'High'
  if (n >= 7 && isNum(avg) && Math.abs(avg) >= 0.3) return 'Medium'
  return 'Low'
}

export function yearlyForwardReturns(histPaths, currentWeek, horizon = 8) {
  if (!Array.isArray(histPaths) || !histPaths.length) return []
  const week = isNum(currentWeek) ? currentWeek : 1
  const endWeek = Math.min(52, week + horizon)
  const out = []
  for (const hp of histPaths) {
    if (!hp || typeof hp !== 'object') continue
    const points = Array.isArray(hp.points) ? hp.points : []
    const byWeek = new Map()
    for (const p of points) {
      if (!p || !isNum(p.week) || !isNum(p.index)) continue
      byWeek.set(p.week, p.index)
    }
    const start = byWeek.get(week)
    const end = byWeek.get(endWeek)
    if (!isNum(start) || !isNum(end) || start === 0) continue
    out.push({
      year: hp.year ?? '?',
      return_pct: Math.round(((end / start) - 1) * 1000) / 10,
    })
  }
  return out.sort((a, b) => Number(a.year) - Number(b.year))
}

function edgePhrase(direction, confidence) {
  const d = String(direction || 'Neutral')
  const c = String(confidence || 'Low')
  if (c === 'Low' || d === 'Neutral') return 'No strong edge'
  if (d === 'Bullish') return c === 'High' ? 'Bullish edge' : 'Mild bullish edge'
  if (d === 'Bearish') return c === 'High' ? 'Bearish edge' : 'Mild bearish edge'
  return 'No strong edge'
}

export function buildV2PlainEnglish(forwardRead, v2Stats, horizonWeeks = 8) {
  const row =
    horizonWeeks === 4
      ? forwardRead?.next_4w
      : horizonWeeks === 12
        ? forwardRead?.next_12w
        : forwardRead?.next_8w
  if (!row?.available) {
    return 'Seasonality V2: Forward outlook unavailable for this ISO week.'
  }
  const conf = v2Stats?.confidence || horizonDisplayConfidence(row)
  const edge = edgePhrase(row.direction, conf)
  return (
    `Seasonality V2: ${edge}. Over the next ${horizonWeeks} weeks, the 10-year average return is ` +
    `${fmtPct(row.avg_return_pct)}, with a ${isNum(row.win_rate_pct) ? `${row.win_rate_pct}%` : '—'} win rate. ` +
    `Confidence is ${conf}.`
  )
}

function SeasonalTooltip({ active, payload, currentWeek, lines }) {
  if (!active || !payload?.length) return null
  const p = payload[0]?.payload
  if (!p) return null
  return (
    <div className="sea-price-tooltip sea-v2-tooltip">
      <div className="sea-price-tip-date">
        ISO week {p.week}
        {p.week === currentWeek ? ' · You are here' : ''}
        {p.is_forward ? ' · forward' : ''}
      </div>
      {isNum(p.actual) ? <div>{p.actualLabel || 'Actual'}: {fmtIdx(p.actual)}</div> : null}
      {(lines || []).map(({ key, label }) =>
        isNum(p[key]) ? <div key={key}>{label}: {fmtIdx(p[key])}</div> : null,
      )}
    </div>
  )
}

function ForwardOutlookTable({ forwardRead, v2Stats }) {
  if (!forwardRead || typeof forwardRead !== 'object') {
    return (
      <div className="sea-v2-outlook-wrap">
        <h4 className="sea-v2-section-title">Forward outlook (10Y sample)</h4>
        <p className="sea-v2-outcome-empty">Forward outlook data unavailable.</p>
      </div>
    )
  }
  const rows = [
    { horizon: 'Next 4 weeks', key: 'next_4w', data: forwardRead?.next_4w },
    { horizon: 'Next 8 weeks', key: 'next_8w', data: forwardRead?.next_8w },
    { horizon: 'Next 12 weeks', key: 'next_12w', data: forwardRead?.next_12w },
  ]
  return (
    <div className="sea-v2-outlook-wrap">
      <h4 className="sea-v2-section-title">Forward outlook (10Y sample)</h4>
      <table className="sea-v2-outlook-table">
        <thead>
          <tr>
            <th>Horizon</th>
            <th>Bias</th>
            <th>Avg return</th>
            <th>Win rate</th>
            <th>Sample size</th>
            <th>Confidence</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(({ horizon, key, data }) => {
            const conf = data?.available ? horizonDisplayConfidence(data) : '—'
            const confCls = conf === '—' ? 'neutral' : String(conf).toLowerCase()
            return (
              <tr key={key}>
                <td>{horizon}</td>
                <td>{data?.available ? data.direction || '—' : '—'}</td>
                <td>{data?.available ? fmtPct(data.avg_return_pct) : '—'}</td>
                <td>{data?.available && isNum(data.win_rate_pct) ? `${data.win_rate_pct}%` : '—'}</td>
                <td>{data?.available ? data.sample_years ?? '—' : '—'}</td>
                <td>
                  <span className={`sea-v2-pill sea-v2-pill--${confCls}`}>{conf}</span>
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
      {v2Stats ? (
        <p className="sea-v2-iso-meta">
          Current ISO week {v2Stats.iso_week ?? '—'} · sample {v2Stats.sample_size ?? '—'} · z{' '}
          {isNum(v2Stats.z_score) ? v2Stats.z_score.toFixed(3) : '—'}
        </p>
      ) : null}
    </div>
  )
}

function YearlyOutcomeBars({ data, horizonWeeks, histAvailable }) {
  if (!histAvailable) {
    return (
      <div className="sea-v2-outcome-wrap">
        <h4 className="sea-v2-section-title">Historical outcomes by year ({horizonWeeks}W forward)</h4>
        <p className="sea-v2-outcome-empty">Historical outcome data unavailable.</p>
      </div>
    )
  }
  if (!data?.length) {
    return (
      <div className="sea-v2-outcome-wrap">
        <h4 className="sea-v2-section-title">Historical outcomes by year ({horizonWeeks}W forward)</h4>
        <p className="sea-v2-outcome-empty">Not enough yearly paths to show outcome bars.</p>
      </div>
    )
  }
  return (
    <div className="sea-v2-outcome-wrap">
      <h4 className="sea-v2-section-title">Historical outcomes by year ({horizonWeeks}W forward)</h4>
      <p className="sea-v2-outcome-hint">
        Each bar is that year&apos;s indexed return from the current ISO week over the next {horizonWeeks} weeks.
        Use this to see whether the average is driven by outliers.
      </p>
      <ResponsiveContainer width="100%" height={200}>
        <BarChart data={data} margin={{ top: 8, right: 12, left: 4, bottom: 4 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.25)" vertical={false} />
          <XAxis dataKey="year" tick={{ fontSize: 12 }} />
          <YAxis tick={{ fontSize: 12 }} width={44} tickFormatter={(v) => `${v}%`} />
          <Tooltip
            formatter={(v) => [fmtPct(v), 'Return']}
            labelFormatter={(y) => `Year ${y}`}
            contentStyle={{ fontSize: 12 }}
          />
          <Bar dataKey="return_pct" name="Forward return" radius={[4, 4, 0, 0]}>
            {data.map((entry) => (
              <Cell
                key={entry.year}
                fill={entry.return_pct >= 0 ? 'rgba(34,197,94,0.75)' : 'rgba(239,68,68,0.75)'}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

export function SeasonalityV2Panel({
  block,
  rows,
  currentWeek,
  currentYear,
  forwardRead,
  v2Stats,
  histYearPaths,
  projectionLabel,
  showActual = true,
  show3y = false,
  show5y = false,
  show10y = true,
  showForwardProjection = false,
  hideForwardOutlook = false,
  hideReadout = false,
  hideLowConfBanner = false,
  hideYearlyPaths = true,
  /** @deprecated use show3y/show5y/show10y */
  showForward10,
  use10yPrimary = true,
}) {
  const safeRows = Array.isArray(rows) ? rows : []
  const safeWeek = isNum(currentWeek) ? currentWeek : 1
  const safeYear = currentYear ?? new Date().getFullYear()
  const safeHist = Array.isArray(histYearPaths) ? histYearPaths : []
  const histAvailable = safeHist.length > 0 && !hideYearlyPaths

  const outcomeHorizon = 8
  const yearlyOutcomes = React.useMemo(
    () => (histAvailable ? yearlyForwardReturns(safeHist, safeWeek, outcomeHorizon) : []),
    [safeHist, safeWeek, histAvailable],
  )
  const plainEnglish = buildV2PlainEnglish(forwardRead, v2Stats, outcomeHorizon)
  const overallConf = String(v2Stats?.confidence || horizonDisplayConfidence(forwardRead?.next_8w) || 'Low')
  const isLowConf = overallConf.toLowerCase() === 'low'

  const show3yLine = show3y || (showForward10 === undefined && !show5y && !show10y && use10yPrimary === false)
  const show5yLine = show5y
  const show10yLine = show10y || showForward10 || (showForward10 === undefined && use10yPrimary !== false && !show3y && !show5y)

  const avgLines = [
    show3yLine ? { key: 'seasonal_3y', label: '3Y seasonal average', stroke: '#93c5fd' } : null,
    show5yLine ? { key: 'seasonal_5y', label: '5Y seasonal average', stroke: '#60a5fa' } : null,
    show10yLine ? { key: 'seasonal_10y', label: '10Y seasonal average', stroke: '#3b82f6' } : null,
  ].filter(Boolean)

  const projLines = []
  if (showForwardProjection || showForward10) {
    if (show10yLine) projLines.push({ key: 'proj_10y', label: '10Y projection' })
    else if (show5yLine) projLines.push({ key: 'proj_5y', label: '5Y projection' })
    else if (show3yLine) projLines.push({ key: 'proj_3y', label: '3Y projection' })
  }

  const tooltipLines = [...avgLines, ...projLines.map((p) => ({ ...p, label: `${p.label} (forward)` }))]

  const chartRows = safeRows
  const actualLabel = `${safeYear} actual (YTD indexed)`

  if (!safeRows.length) {
    return (
      <div className="sea-v2-decision-panel">
        <p className="sea-v2-outcome-empty">Seasonality V2 chart data unavailable for this market.</p>
      </div>
    )
  }

  return (
    <div className="sea-v2-decision-panel">
      {!hideReadout ? <p className="sea-v2-readout">{plainEnglish}</p> : null}

      {!hideLowConfBanner && isLowConf ? (
        <p className="sea-v2-low-conf-banner">
          Low confidence — use as context only, not a trade signal.
        </p>
      ) : null}

      {!hideForwardOutlook ? <ForwardOutlookTable forwardRead={forwardRead} v2Stats={v2Stats} /> : null}

      <div className="sea-v2-chart-toolbar">
        <span className="sea-v2-section-title sea-v2-section-title--inline">
          Where we are in the year (indexed paths, ISO weeks)
        </span>
      </div>

      <div className="sea-panel sea-panel--unified sea-v2-main-chart">
        <ResponsiveContainer width="100%" height={320}>
          <LineChart data={chartRows} margin={{ top: 16, right: 20, left: 8, bottom: 8 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.25)" />
            <XAxis
              dataKey="week"
              type="number"
              domain={[1, 52]}
              ticks={WEEK_TICKS}
              tickFormatter={weekTickLabel}
              tick={{ fontSize: 12 }}
            />
            <YAxis tick={{ fontSize: 12 }} width={52} tickFormatter={fmtIdx} domain={['auto', 'auto']} />
            <Tooltip content={<SeasonalTooltip currentWeek={safeWeek} lines={tooltipLines} />} />
            <Legend wrapperStyle={{ fontSize: 13, paddingTop: 8 }} />

            <ReferenceLine y={100} stroke="#64748b" strokeDasharray="2 4" strokeOpacity={0.5} />
            <ReferenceLine
              x={safeWeek}
              stroke="#f8fafc"
              strokeWidth={2}
              strokeDasharray="4 3"
              label={{ value: 'Now', position: 'insideTopLeft', fontSize: 12, fill: '#e2e8f0' }}
            />

            {avgLines.map((line) => (
              <Line
                key={line.key}
                type="linear"
                dataKey={line.key}
                name={line.label}
                stroke={line.stroke}
                strokeWidth={2}
                strokeDasharray="8 4"
                dot={false}
                connectNulls={false}
              />
            ))}

            {showActual ? (
              <Line
                type="linear"
                dataKey="actual"
                name={actualLabel}
                stroke="#2dd4bf"
                strokeWidth={3}
                dot={{ r: 2, fill: '#2dd4bf' }}
                activeDot={{ r: 5 }}
                connectNulls={false}
              />
            ) : null}

            {projLines.map((line) => (
              <Line
                key={line.key}
                type="linear"
                dataKey={line.key}
                name={`${line.label} — not a forecast`}
                stroke="#fbbf24"
                strokeWidth={2}
                strokeDasharray="4 4"
                dot={false}
                connectNulls={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {projectionLabel ? <p className="sea-v2-projection-note">{projectionLabel}</p> : null}

      <YearlyOutcomeBars
        data={yearlyOutcomes}
        horizonWeeks={outcomeHorizon}
        histAvailable={histAvailable}
      />
    </div>
  )
}

/** Prevent a seasonality panel fault from blanking the whole instrument page. */
export class SeasonalityV2ErrorBoundary extends React.Component {
  constructor(props) {
    super(props)
    this.state = { error: null }
  }

  static getDerivedStateFromError(error) {
    return { error }
  }

  componentDidCatch(error, info) {
    console.error('SeasonalityV2Panel render failed:', error, info)
  }

  render() {
    if (this.state.error) {
      return (
        <div className="sea-v2-decision-panel">
          <p className="sea-v2-outcome-empty">
            Seasonality V2 panel could not render — {this.state.error.message || 'unknown error'}.
            The rest of the instrument page remains available.
          </p>
        </div>
      )
    }
    return this.props.children
  }
}
