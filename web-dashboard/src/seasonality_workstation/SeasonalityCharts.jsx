import React from 'react'
import {
  CartesianGrid,
  ComposedChart,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  Scatter,
} from 'recharts'

function fmtPx(v) {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  const n = Number(v)
  if (Math.abs(n) >= 1000) return n.toFixed(2)
  if (Math.abs(n) >= 10) return n.toFixed(3)
  return n.toFixed(5)
}

function fmtPct(v) {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  return `${(Number(v) * 100).toFixed(2)}%`
}

function fmtIdx(v) {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  return Number(v).toFixed(2)
}

function shortDate(d) {
  if (!d) return ''
  const s = String(d)
  return s.length >= 10 ? s.slice(5) : s
}

/**
 * Price history only (no seasonal forecast overlay).
 * Price-unit forecast lives in Advanced view separately.
 */
export function PriceHistoryChart({ priceSeries, anchorDate }) {
  const data = React.useMemo(
    () => (priceSeries || []).map((r) => ({ date: r.date, actual: r.close })),
    [priceSeries],
  )

  if (!data.length) {
    return <p className="sws-muted">No price series available.</p>
  }

  return (
    <ResponsiveContainer width="100%" height={220}>
      <LineChart data={data} margin={{ top: 8, right: 16, left: 4, bottom: 4 }}>
        <CartesianGrid stroke="rgba(148,163,184,0.12)" vertical={false} />
        <XAxis
          dataKey="date"
          tick={{ fill: '#94a3b8', fontSize: 10 }}
          minTickGap={28}
          tickFormatter={shortDate}
        />
        <YAxis
          domain={['auto', 'auto']}
          tick={{ fill: '#94a3b8', fontSize: 10 }}
          width={68}
          tickFormatter={fmtPx}
        />
        <Tooltip
          contentStyle={{ background: '#0f172a', border: '1px solid #334155' }}
          labelFormatter={(d) => String(d)}
          formatter={(value) => [fmtPx(value), 'Close']}
        />
        {anchorDate ? (
          <ReferenceLine
            x={anchorDate}
            stroke="#fbbf24"
            strokeDasharray="4 3"
            label={{ value: 'TODAY', fill: '#fbbf24', fontSize: 10, position: 'insideTopLeft' }}
          />
        ) : null}
        <Line
          type="monotone"
          dataKey="actual"
          name="Close"
          stroke="#e2e8f0"
          dot={false}
          strokeWidth={1.6}
          connectNulls={false}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}

/**
 * Select plotted full_year points for a price-unit seasonal product.
 * seriesMode is explicit so Roadmap / Mean-return cannot cross-bind.
 *  - roadmap: smoothed.full_year OR unsmoothed.full_year (never Freeze / mean-return)
 *  - mean_return: pack.full_year only (never roadmap smoothed/unsmoothed bundles)
 */
export function selectPriceUnitFullYear(pack, { seriesMode, useSmoothed = false } = {}) {
  if (!pack?.available) {
    return { full: [], sourcePath: null, datasetName: null, valueKey: 'price' }
  }
  if (seriesMode === 'roadmap') {
    if (useSmoothed && pack.smoothed?.full_year?.length) {
      return {
        full: pack.smoothed.full_year,
        sourcePath: 'payload.seasonal_roadmap.smoothed.full_year',
        datasetName: pack.method?.version || 'seasonal_roadmap_v1',
        valueKey: 'price',
        sourceFunction: 'build_seasonal_roadmap_curve',
      }
    }
    if (pack.unsmoothed?.full_year?.length) {
      return {
        full: pack.unsmoothed.full_year,
        sourcePath: 'payload.seasonal_roadmap.unsmoothed.full_year',
        datasetName: pack.method?.version || 'seasonal_roadmap_v1',
        valueKey: 'price',
        sourceFunction: 'build_seasonal_roadmap_curve',
      }
    }
    return {
      full: [],
      sourcePath: null,
      datasetName: pack.method?.version || 'seasonal_roadmap_v1',
      valueKey: 'price',
      sourceFunction: 'build_seasonal_roadmap_curve',
    }
  }
  if (seriesMode === 'mean_return') {
    return {
      full: Array.isArray(pack.full_year) ? pack.full_year : [],
      sourcePath: 'payload.seasonal_price_path.full_year',
      datasetName: pack.method?.version || 'seasonal_price_path_v1',
      valueKey: 'price',
      sourceFunction: 'build_seasonal_price_path_curve',
    }
  }
  return { full: [], sourcePath: null, datasetName: null, valueKey: 'price' }
}

function usePriceUnitSeasonalSeries(pack, { seriesMode, useSmoothed = false } = {}) {
  return React.useMemo(() => {
    const selected = selectPriceUnitFullYear(pack, { seriesMode, useSmoothed })
    const full = selected.full
    if (!full.length) {
      return { data: [], yDomain: ['auto', 'auto'], binding: selected }
    }

    const asofTd = pack.asof_trading_day
    const valueKey = selected.valueKey
    const rows = full.map((p) => {
      const td = p.trading_day
      const isHist = p.segment === 'historical' || p.segment === 'today' || td <= asofTd
      const isFwd = p.segment === 'forward' || p.segment === 'today' || td >= asofTd
      const v = p[valueKey]
      return {
        date: p.date,
        trading_day: td,
        historical: isHist ? v : null,
        forward: isFwd ? v : null,
        segment: p.segment,
      }
    })

    const vals = rows
      .flatMap((r) => [r.historical, r.forward])
      .filter((v) => v != null && Number.isFinite(Number(v)))
      .map(Number)
    if (!vals.length) {
      return { data: rows, yDomain: ['auto', 'auto'], binding: selected }
    }
    const min = Math.min(...vals)
    const max = Math.max(...vals)
    const pad = Math.max((max - min) * 0.12, Math.abs(max) * 0.002)
    return {
      data: rows,
      yDomain: [min - pad, max + pad],
      binding: selected,
    }
  }, [pack, seriesMode, useSmoothed])
}

function PriceUnitSeasonalChart({
  pack,
  anchorDate,
  anchorPrice,
  seriesMode,
  useSmoothed = false,
  unavailableLabel = 'Seasonal path',
}) {
  const { data, yDomain, binding } = usePriceUnitSeasonalSeries(pack, {
    seriesMode,
    useSmoothed,
  })

  if (!pack?.available) {
    return (
      <p className="sws-muted">
        {unavailableLabel} unavailable{pack?.reason ? `: ${pack.reason}` : '.'}
      </p>
    )
  }
  if (!data.length) {
    return <p className="sws-muted">{unavailableLabel} unavailable.</p>
  }

  const todayLabel =
    anchorPrice != null && Number.isFinite(Number(anchorPrice))
      ? `TODAY · ${fmtPx(anchorPrice)}`
      : 'TODAY'

  return (
    <div
      data-seasonal-dataset={binding?.datasetName || ''}
      data-seasonal-source={binding?.sourcePath || ''}
      data-seasonal-units="price"
    >
      <ResponsiveContainer width="100%" height={300}>
        <ComposedChart data={data} margin={{ top: 12, right: 16, left: 4, bottom: 4 }}>
          <CartesianGrid stroke="rgba(148,163,184,0.12)" vertical={false} />
          <XAxis
            dataKey="date"
            tick={{ fill: '#94a3b8', fontSize: 10 }}
            minTickGap={28}
            tickFormatter={shortDate}
          />
          <YAxis
            domain={yDomain}
            tick={{ fill: '#94a3b8', fontSize: 10 }}
            width={68}
            tickFormatter={fmtPx}
          />
          <Tooltip
            contentStyle={{ background: '#0f172a', border: '1px solid #334155' }}
            labelFormatter={(d) => String(d)}
            formatter={(value, name) => [fmtPx(value), name]}
          />
          {anchorDate ? (
            <ReferenceLine
              x={anchorDate}
              stroke="#fbbf24"
              strokeWidth={1.5}
              label={{
                value: todayLabel,
                fill: '#fbbf24',
                fontSize: 10,
                position: 'insideTopLeft',
              }}
            />
          ) : null}
          <Line
            type="monotone"
            dataKey="historical"
            name="Seasonal path"
            stroke="#94a3b8"
            dot={false}
            strokeWidth={2.2}
            connectNulls
          />
          <Line
            type="monotone"
            dataKey="forward"
            name="Projected path"
            stroke="#38bdf8"
            dot={false}
            strokeWidth={2.6}
            connectNulls
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}

/** Seasonal Roadmap — avg indexed year paths, as-of rebase, price units. */
export function SeasonalRoadmapChart({
  roadmap,
  anchorDate,
  anchorPrice,
  useSmoothed = true,
}) {
  return (
    <PriceUnitSeasonalChart
      pack={roadmap}
      anchorDate={anchorDate}
      anchorPrice={anchorPrice ?? roadmap?.asof_price ?? roadmap?.anchor_price}
      seriesMode="roadmap"
      useSmoothed={useSmoothed}
      unavailableLabel="Seasonal Roadmap"
    />
  )
}

/**
 * Seasonal Price Path — mean daily-return cumsum, as-of rebase (not Roadmap).
 */
export function SeasonalPricePathChart({ pricePath, anchorDate }) {
  return (
    <PriceUnitSeasonalChart
      pack={pricePath}
      anchorDate={anchorDate}
      seriesMode="mean_return"
      useSmoothed={false}
      unavailableLabel="Seasonal Price Path"
    />
  )
}

/**
 * Freeze v1.0 lower panel: normalised indexed seasonal curve.
 * Grey historical path, solid red forward, current date marked, weekly points.
 */
export function NormalisedSeasonalityChart({ normalised, anchorDate }) {
  const { data, yDomain } = React.useMemo(() => {
    if (!normalised?.available) return { data: [], yDomain: ['auto', 'auto'] }

    // Prefer the continuous full-year seasonal path (one coherent curve).
    // Value key is ALWAYS index (% units) — never price from Roadmap / mean-return.
    const full = normalised.full_year
    let rows
    if (Array.isArray(full) && full.length) {
      const asofDoy = normalised.asof_doy
      rows = full.map((p) => {
        const isHist = p.segment === 'historical' || p.segment === 'today' || p.doy <= asofDoy
        const isFwd = p.segment === 'forward' || p.segment === 'today' || p.doy >= asofDoy
        return {
          date: p.date,
          doy: p.doy,
          historical: isHist ? p.index : null,
          forward: isFwd ? p.index : null,
          weekly: null,
          segment: p.segment,
        }
      })
      // Small weekly markers on the forward seasonal path only
      for (const wp of normalised.weekly_points || []) {
        if (!wp?.date || !(wp.offset_days > 0)) continue
        const row = rows.find((r) => r.date === wp.date)
        if (row) row.weekly = wp.index
      }
    } else {
      const hist = normalised.historical || []
      const fwd = normalised.forward || []
      const byKey = new Map()
      for (const p of hist) {
        byKey.set(p.date, {
          date: p.date,
          doy: p.doy,
          historical: p.index,
          forward: p.segment === 'today' ? p.index : null,
          weekly: null,
          segment: p.segment,
        })
      }
      for (const p of fwd) {
        const row = byKey.get(p.date) || {
          date: p.date,
          doy: p.doy,
          historical: null,
          forward: null,
          weekly: null,
          segment: p.segment,
        }
        row.forward = p.index
        if (p.segment === 'today') row.historical = p.index
        byKey.set(p.date, row)
      }
      rows = Array.from(byKey.values()).sort((a, b) => String(a.date).localeCompare(String(b.date)))
    }

    const vals = rows
      .flatMap((r) => [r.historical, r.forward])
      .filter((v) => v != null && Number.isFinite(Number(v)))
      .map(Number)
    if (!vals.length) return { data: rows, yDomain: ['auto', 'auto'] }
    const min = Math.min(...vals)
    const max = Math.max(...vals)
    // Enough vertical room that seasonal swings remain readable
    const pad = Math.max(0.25, (max - min) * 0.22)
    return { data: rows, yDomain: [min - pad, max + pad] }
  }, [normalised])

  if (!normalised?.available) {
    return (
      <p className="sws-muted">
        Normalised seasonality unavailable{normalised?.reason ? `: ${normalised.reason}` : '.'}
      </p>
    )
  }

  if (!data.length) {
    return <p className="sws-muted">Normalised seasonality unavailable.</p>
  }

  const datasetName = normalised?.method?.version || 'freeze_v1.0'

  return (
    <div
      data-seasonal-dataset={datasetName}
      data-seasonal-source="payload.normalised_seasonality.full_year"
    >
      <ResponsiveContainer width="100%" height={280}>
        <ComposedChart data={data} margin={{ top: 12, right: 16, left: 4, bottom: 4 }}>
          <CartesianGrid stroke="rgba(148,163,184,0.12)" vertical={false} />
          <XAxis
            dataKey="date"
            tick={{ fill: '#94a3b8', fontSize: 10 }}
            minTickGap={28}
            tickFormatter={shortDate}
          />
          <YAxis
            domain={yDomain}
            tick={{ fill: '#94a3b8', fontSize: 10 }}
            width={52}
            tickFormatter={(v) => `${Number(v).toFixed(2)}%`}
          />
          <Tooltip
            contentStyle={{ background: '#0f172a', border: '1px solid #334155' }}
            labelFormatter={(d) => String(d)}
            formatter={(value, name) => [`${Number(value).toFixed(3)}%`, name]}
          />
          {anchorDate ? (
            <ReferenceLine
              x={anchorDate}
              stroke="#fbbf24"
              strokeWidth={1.5}
              label={{
                value: 'TODAY',
                fill: '#fbbf24',
                fontSize: 10,
                position: 'insideTopLeft',
              }}
            />
          ) : null}
          <Line
            type="monotone"
            dataKey="historical"
            name="Seasonal path"
            stroke="#94a3b8"
            dot={false}
            strokeWidth={2}
            connectNulls
          />
          <Line
            type="monotone"
            dataKey="forward"
            name="Forward seasonal"
            stroke="#ef4444"
            dot={false}
            strokeWidth={2.4}
            connectNulls
          />
          <Scatter dataKey="weekly" name="Weekly markers" fill="#fca5a5" stroke="#ef4444" />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}

/**
 * Advanced-only: real historical price + seasonal forecast in price units.
 * Must not be mixed into the normalised seasonal curve panel.
 */
export function PriceForecastChart({
  priceSeries,
  forecastPath,
  upperBand,
  lowerBand,
  showBands,
  anchorDate,
  confidence,
  sampleSize,
  modelLabel,
}) {
  const data = React.useMemo(() => {
    const hist = (priceSeries || []).map((r) => ({
      date: r.date,
      actual: r.close,
      forecast: null,
      upper: null,
      lower: null,
      cumulative_return: null,
      segment: 'historical',
    }))
    if (!hist.length) return []

    const lastHist = hist[hist.length - 1]
    const path = forecastPath || []
    const up = upperBand || []
    const lo = lowerBand || []

    if (path.length) {
      lastHist.forecast = path[0].price
      lastHist.cumulative_return = 0
      lastHist.segment = 'today'
      if (showBands && up[0] && lo[0]) {
        lastHist.upper = up[0].price
        lastHist.lower = lo[0].price
      }
    }

    const byDate = new Map(hist.map((r) => [r.date, r]))
    for (let i = 1; i < path.length; i += 1) {
      const p = path[i]
      byDate.set(p.date, {
        date: p.date,
        actual: null,
        forecast: p.price,
        upper: showBands && up[i] ? up[i].price : null,
        lower: showBands && lo[i] ? lo[i].price : null,
        cumulative_return: p.cumulative_return,
        week_offset: p.week_offset,
        segment: 'forecast',
      })
    }
    return Array.from(byDate.values()).sort((a, b) => String(a.date).localeCompare(String(b.date)))
  }, [priceSeries, forecastPath, upperBand, lowerBand, showBands])

  if (!data.length) {
    return <p className="sws-muted">No price series available.</p>
  }

  return (
    <ResponsiveContainer width="100%" height={320}>
      <ComposedChart data={data} margin={{ top: 12, right: 16, left: 4, bottom: 4 }}>
        <CartesianGrid stroke="rgba(148,163,184,0.12)" vertical={false} />
        <XAxis
          dataKey="date"
          tick={{ fill: '#94a3b8', fontSize: 10 }}
          minTickGap={28}
          tickFormatter={shortDate}
        />
        <YAxis
          domain={['auto', 'auto']}
          tick={{ fill: '#94a3b8', fontSize: 10 }}
          width={68}
          tickFormatter={fmtPx}
        />
        <Tooltip
          contentStyle={{ background: '#0f172a', border: '1px solid #334155' }}
          labelFormatter={(d) => String(d)}
          formatter={(value, name, ctx) => {
            if (name === 'Seasonal Forecast') {
              const cum = ctx?.payload?.cumulative_return
              return [
                `${fmtPx(value)}  ·  cum ${fmtPct(cum)}  ·  ${modelLabel || 'median'}  ·  conf ${confidence || '—'}  ·  n=${sampleSize ?? '—'}`,
                'Seasonal Forecast',
              ]
            }
            if (name === 'Actual price') return [fmtPx(value), name]
            if (name === 'Upper band' || name === 'Lower band') return [fmtPx(value), name]
            return [fmtPx(value), name]
          }}
        />
        {anchorDate ? (
          <ReferenceLine
            x={anchorDate}
            stroke="#fbbf24"
            strokeDasharray="4 3"
            label={{
              value: 'TODAY / FORECAST START',
              fill: '#fbbf24',
              fontSize: 10,
              position: 'insideTopLeft',
            }}
          />
        ) : null}
        {showBands ? (
          <Line
            type="monotone"
            dataKey="upper"
            name="Upper band"
            stroke="rgba(251,113,133,0.35)"
            dot={false}
            strokeWidth={1}
            connectNulls
          />
        ) : null}
        {showBands ? (
          <Line
            type="monotone"
            dataKey="lower"
            name="Lower band"
            stroke="rgba(251,113,133,0.35)"
            dot={false}
            strokeWidth={1}
            connectNulls
          />
        ) : null}
        <Line
          type="monotone"
          dataKey="actual"
          name="Actual price"
          stroke="#e2e8f0"
          dot={false}
          strokeWidth={1.6}
          connectNulls={false}
        />
        <Line
          type="monotone"
          dataKey="forecast"
          name="Seasonal Forecast"
          stroke="#fb7185"
          strokeDasharray="7 4"
          dot={false}
          strokeWidth={2.4}
          connectNulls
        />
      </ComposedChart>
    </ResponsiveContainer>
  )
}

/** Legacy week study — Advanced research only. */
export function SeasonalityCurveChart({
  payload,
  showAverage,
  showMedian,
  showCurrentYear,
  showBands,
  showYears,
}) {
  const data = React.useMemo(() => {
    if (!payload?.seasonality) return []
    const seas = payload.seasonality
    const aligned = seas.price_aligned || {}
    const trimmed = aligned.trimmed_mean || {}
    const median = aligned.median || {}
    const upper = aligned.upper_band || {}
    const lower = aligned.lower_band || {}
    const current = seas.current_year_price || {}
    const yearPaths = seas.historical_year_paths || {}

    const rows = []
    for (let w = 1; w <= 52; w += 1) {
      const key = String(w)
      const row = {
        week: w,
        label: `W${w}`,
        seasonal: trimmed[key] ?? null,
        median: median[key] ?? null,
        upper: upper[key] ?? null,
        lower: lower[key] ?? null,
        current: current[key] ?? null,
      }
      if (showYears) {
        Object.entries(yearPaths).forEach(([y, path]) => {
          row[`y${y}`] = path?.[key] ?? null
        })
      }
      rows.push(row)
    }
    return rows
  }, [payload, showYears])

  const yearKeys = React.useMemo(() => {
    if (!showYears || !payload?.seasonality?.historical_year_paths) return []
    return Object.keys(payload.seasonality.historical_year_paths).map((y) => `y${y}`)
  }, [payload, showYears])

  if (!data.length) {
    return <p className="sws-muted">Seasonality study unavailable.</p>
  }

  return (
    <ResponsiveContainer width="100%" height={300}>
      <LineChart data={data} margin={{ top: 8, right: 12, left: 0, bottom: 0 }}>
        <CartesianGrid stroke="rgba(148,163,184,0.12)" vertical={false} />
        <XAxis dataKey="label" tick={{ fill: '#94a3b8', fontSize: 10 }} minTickGap={16} />
        <YAxis
          domain={['auto', 'auto']}
          tick={{ fill: '#94a3b8', fontSize: 10 }}
          width={64}
          tickFormatter={fmtPx}
        />
        <Tooltip contentStyle={{ background: '#0f172a', border: '1px solid #334155' }} />
        {showBands ? (
          <Line type="monotone" dataKey="upper" stroke="rgba(56,189,248,0.35)" dot={false} name="Upper" />
        ) : null}
        {showBands ? (
          <Line type="monotone" dataKey="lower" stroke="rgba(56,189,248,0.35)" dot={false} name="Lower" />
        ) : null}
        {yearKeys.map((k) => (
          <Line
            key={k}
            type="monotone"
            dataKey={k}
            stroke="rgba(148,163,184,0.22)"
            dot={false}
            strokeWidth={1}
            name={k.slice(1)}
          />
        ))}
        {showAverage ? (
          <Line type="monotone" dataKey="seasonal" stroke="#38bdf8" dot={false} strokeWidth={2} name="Trimmed mean" />
        ) : null}
        {showMedian ? (
          <Line
            type="monotone"
            dataKey="median"
            stroke="#a78bfa"
            dot={false}
            strokeWidth={1.5}
            strokeDasharray="4 3"
            name="Median"
          />
        ) : null}
        {showCurrentYear ? (
          <Line type="monotone" dataKey="current" stroke="#fbbf24" dot={false} strokeWidth={1.8} name="Current year" />
        ) : null}
      </LineChart>
    </ResponsiveContainer>
  )
}

/** @deprecated kept for imports — use PriceHistoryChart / PriceForecastChart */
export function SeasonalityPriceChart({ series }) {
  return <PriceHistoryChart priceSeries={series} />
}
