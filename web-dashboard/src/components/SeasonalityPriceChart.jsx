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

import { SeasonalityV2ErrorBoundary, SeasonalityV2Panel } from './SeasonalityV2Panel.jsx'
import {
  SeasonalityProjectionErrorBoundary,
  SeasonalityProjectionPanel,
} from './SeasonalityProjectionPanel.jsx'
import { defaultToggles } from '../seasonality/seasonalityControls.js'



const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

const fmtIdx = (v) => (isNum(v) ? v.toFixed(1) : '—')

const fmtPct = (v) => (isNum(v) ? `${v >= 0 ? '+' : ''}${v.toFixed(2)}%` : '—')

const fmtPrice = (v) => (isNum(v) ? v.toLocaleString(undefined, { maximumFractionDigits: 2 }) : '—')



const WEEK_TICKS = [1, 5, 9, 14, 18, 23, 27, 31, 36, 40, 45, 49, 52]

const MONTH_LABELS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', '']



function weekTickLabel(week) {

  const monthIdx = [1, 5, 9, 14, 18, 23, 27, 31, 36, 40, 45, 49]

  const i = monthIdx.indexOf(week)

  return i >= 0 ? MONTH_LABELS[i] : week === 52 ? 'Dec' : ''

}



function BiasChip({ label, data }) {

  if (!data?.available) {

    return (

      <span className="sea-bias-chip sea-bias-chip--empty">

        {label}: n/a

      </span>

    )

  }

  const tone =

    data.direction === 'Bullish' ? 'bull' : data.direction === 'Bearish' ? 'bear' : 'neutral'

  return (

    <span className={`sea-bias-chip sea-bias-chip--${tone}`}>

      {label}: {data.direction} {fmtPct(data.avg_return_pct)} (n={data.sample_years})

    </span>

  )

}



function UnifiedTooltip({ active, payload, currentWeek, windows, v2Mode }) {

  if (!active || !payload?.length) return null

  const p = payload[0]?.payload

  if (!p) return null

  const divTone = isNum(p.divergence) ? (p.divergence > 0 ? 'above' : p.divergence < 0 ? 'below' : 'inline') : null

  return (

    <div className="sea-price-tooltip">

      <div className="sea-price-tip-date">

        ISO week {p.week}

        {p.week === currentWeek ? ' · You are here' : ''}

        {p.is_forward ? ' · forward' : ''}

      </div>

      {isNum(p.actual) ? <div>Actual index: {fmtIdx(p.actual)}</div> : null}

      {!v2Mode && isNum(p.seasonal_3y) ? <div>3Y seasonal avg: {fmtIdx(p.seasonal_3y)}</div> : null}

      {!v2Mode && windows.includes('5Y') && isNum(p.seasonal_5y) ? <div>5Y seasonal avg: {fmtIdx(p.seasonal_5y)}</div> : null}

      {(v2Mode || windows.includes('10Y')) && isNum(p.seasonal_10y) ? (

        <div>10Y seasonal average: {fmtIdx(p.seasonal_10y)}</div>

      ) : null}

      {isNum(p.divergence) ? (

        <div className={divTone ? `sea-tip-div sea-tip-div--${divTone}` : ''}>

          Divergence (actual − seasonal): {p.divergence >= 0 ? '+' : ''}

          {fmtIdx(p.divergence)}

        </div>

      ) : null}

      {!v2Mode && isNum(p.proj_3y) && p.week >= currentWeek ? <div>3Y forward proj: {fmtIdx(p.proj_3y)}</div> : null}

      {!v2Mode && windows.includes('5Y') && isNum(p.proj_5y) && p.week >= currentWeek ? (

        <div>5Y forward proj: {fmtIdx(p.proj_5y)}</div>

      ) : null}

      {(v2Mode || windows.includes('10Y')) && isNum(p.proj_10y) && p.week >= currentWeek ? (

        <div>10Y forward proj: {fmtIdx(p.proj_10y)}</div>

      ) : null}

      {isNum(p.close) ? <div className="sea-price-tip-meta">Close: {fmtPrice(p.close)}</div> : null}

    </div>

  )

}



/** Merge legacy export blocks into unified chart_series when needed. */

function chartRows(block) {

  if (!block || typeof block !== 'object') return []

  if (Array.isArray(block.chart_series) && block.chart_series.length) {

    return block.chart_series

  }

  const current = block.current_path || []

  const forward = block.forward_projection || []

  const byWeek = new Map()

  for (const r of current) {

    byWeek.set(r.week, { week: r.week, actual: r.index, close: r.close, divergence: null })

  }

  for (const r of forward) {

    const row = byWeek.get(r.week) || { week: r.week }

    if (isNum(r.anchor)) row.actual = r.anchor

    row.proj_3y = r.proj_3y

    row.proj_5y = r.proj_5y

    row.proj_10y = r.proj_10y

    byWeek.set(r.week, row)

  }

  return Array.from({ length: 52 }, (_, i) => byWeek.get(i + 1) || { week: i + 1 })

}



export function SeasonalityPriceChart({ block, dataMode = 'production' }) {

  const v2Mode = dataMode === 'seasonality_v2_staging'



  if (!block?.available) {

    return (

      <section className="sea-price-section">

        <h3 className="sea-price-title">SEASONALITY VS CURRENT PRICE</h3>

        <p className="sea-price-empty">{block?.reason || 'Seasonality price chart unavailable for this instrument.'}</p>

      </section>

    )

  }



  const {

    windows_available: windows = [],

    current_week: currentWeek = 1,

    current_year: currentYear,

    latest_price: latestPrice,

    availability_note: availabilityNote,

    price_stale_note: staleNote,

    forward_read: forwardRead,

    divergence_read: divRead,

    forward_projection_available: forwardOk,

    projection_label: projectionLabel,

    v2_current_week_stats: v2Stats,
    historical_year_paths: histYearPaths = [],
    confidence: prodConfidence,

  } = block || {}

  const safeHistPaths = Array.isArray(histYearPaths) ? histYearPaths : []
  const rows = chartRows(block)

  const hasSeasonal = v2Mode
    ? rows.some((r) => isNum(r.seasonal_10y))
    : rows.some((r) => isNum(r.seasonal_3y) || isNum(r.seasonal_10y))

  const hasActual = rows.some((r) => isNum(r.actual))

  const proj10Points = rows.filter((r) => r.week >= currentWeek && isNum(r.proj_10y)).length

  const proj3Points = rows.filter((r) => r.week >= currentWeek && isNum(r.proj_3y)).length

  const showForward10 = forwardOk !== false && (v2Mode || windows.includes('10Y')) && proj10Points >= 2

  const showForward3 = !v2Mode && forwardOk !== false && windows.includes('3Y') && proj3Points >= 2



  if (!hasActual || !hasSeasonal) {

    return (

      <section className="sea-price-section">

        <h3 className="sea-price-title">SEASONALITY VS CURRENT PRICE</h3>

        <p className="sea-price-empty">

          Insufficient data to plot actual price against seasonal path (actual={hasActual ? 'yes' : 'no'},

          seasonal={hasSeasonal ? 'yes' : 'no'}).

        </p>

      </section>

    )

  }



  const div = divRead?.available ? divRead : null

  const divTone = div?.position || 'inline'



  return (

    <section className="sea-price-section">

      <header className="sea-price-head">

        <div>

          <h3 className="sea-price-title">{v2Mode ? 'SEASONALITY V2 — DECISION PANEL' : 'SEASONALITY VS CURRENT PRICE'}</h3>

          <p className="sea-price-sub">

            {v2Mode

              ? `Indexed to 100 at ISO week 1 — ${currentYear} actual vs 10-year seasonal average (V2 staging validation).`

              : `Indexed to 100 at ISO week 1 — ${currentYear} actual vs historical seasonal average and forward projection.`}

          </p>

          <span className={`sea-data-mode-badge sea-data-mode-badge--${v2Mode ? 'staging' : 'production'}`}>
            {v2Mode ? 'Source: Seasonality V2 staging' : 'Production seasonality'}
          </span>

        </div>

        {latestPrice ? (

          <div className="sea-current-spot">

            <span className="sea-current-spot-price">{fmtPrice(latestPrice.close)}</span>

            <span className="sea-current-spot-meta">

              {latestPrice.date} · ISO week {latestPrice.week}

            </span>

          </div>

        ) : null}

      </header>



      {staleNote ? <p className="sea-price-warn">{staleNote}</p> : null}

      {v2Mode ? (
        <p className="sea-price-note">
          {availabilityNote && !/Only \d+ year/i.test(availabilityNote)
            ? availabilityNote
            : '10Y history available (Seasonality V2 staging — OANDA backfill; not production).'}
        </p>
      ) : availabilityNote ? (
        <p className="sea-price-note">{availabilityNote}</p>
      ) : null}

      {!v2Mode && prodConfidence?.level ? (
        <div className={`sea-conf-badge sea-conf-${String(prodConfidence.level).toLowerCase()}`}>
          <span className="sea-conf-level" title="Agreement between 3Y, 5Y, and 10Y seasonal forward directions">
            Seasonal path agreement: {prodConfidence.level}
          </span>
          {prodConfidence.detail ? <span className="sea-conf-detail"> — {prodConfidence.detail}</span> : null}
        </div>
      ) : null}

      {v2Mode ? (
        <SeasonalityV2ErrorBoundary>
          <SeasonalityV2Panel
            block={block}
            rows={rows}
            currentWeek={currentWeek}
            currentYear={currentYear}
            forwardRead={forwardRead}
            v2Stats={v2Stats}
            histYearPaths={safeHistPaths}
            projectionLabel={projectionLabel}
            showForward10={showForward10}
          />
        </SeasonalityV2ErrorBoundary>
      ) : (
        <SeasonalityProjectionErrorBoundary>
          <SeasonalityProjectionPanel block={block} toggles={defaultToggles(block)} />
        </SeasonalityProjectionErrorBoundary>
      )}

    </section>

  )

}


