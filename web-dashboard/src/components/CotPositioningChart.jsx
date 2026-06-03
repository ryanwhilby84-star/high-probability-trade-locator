import React from 'react'
import {
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

const fmt = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return n.toLocaleString(undefined, { maximumFractionDigits: 0 })
}

const fmtPct = (v) => {
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return `${n.toFixed(1)}%`
}

function ChartTooltip({ active, payload, label, groupId, hasPrice, priceLabel, profile }) {
  if (!active || !payload?.length) return null
  const p = payload[0]?.payload
  if (!p) return null
  const legacy = profile === 'legacy' || profile === 'commodity'
  return (
    <div className="cot-chart-tooltip">
      <div className="cot-chart-tooltip-date">{label}</div>
      {groupId !== 'combined' ? (
        <>
          <div>Long: {fmt(p.long)}</div>
          <div>Short: {fmt(p.short)}</div>
          <div>Net: {fmt(p.net)}</div>
          {p.pctLong != null ? <div>% long: {fmtPct(p.pctLong)}</div> : null}
          {p.pctShort != null ? <div>% short: {fmtPct(p.pctShort)}</div> : null}
          {p.band13NetMin != null ? (
            <div className="cot-chart-tooltip-meta">
              13W net band: {fmt(p.band13NetMin)} – {fmt(p.band13NetMax)}
            </div>
          ) : null}
        </>
      ) : (
        <>
          {p.mmAvailable ? <div>Non-commercial net: {fmt(p.mmNet)}</div> : null}
          {p.commAvailable ? <div>Commercial net: {fmt(p.commNet)}</div> : null}
          {p.nrAvailable ? <div>Non-reportable net: {fmt(p.nrNet)}</div> : null}
        </>
      )}
      {hasPrice && p.pricePct != null ? (
        <div>
          {priceLabel}: {fmtPct(p.pricePct)} (rebased)
        </div>
      ) : null}
    </div>
  )
}

export function CotPositioningChart({ chartData, groupId, showBands = '13', chartMode = 'full' }) {
  const { series, hasPrice, priceLabel, anyAvailable, profile = 'commodity' } = chartData
  const mode = chartMode || 'full'
  const showLongShort = mode === 'full'
  const showNet = mode === 'full' || mode === 'net' || mode === 'bands'
  const showPrice = (mode === 'full' || mode === 'price') && hasPrice
  const bandsOn = (mode === 'full' || mode === 'bands') && showBands !== 'off'
  if (!anyAvailable) {
    return (
      <p className="cot-chart-empty">
        No Legacy COT positioning history for this group — run <code>python -m hptl.cot.run_legacy_cot</code> and
        rebuild confluence.
      </p>
    )
  }

  const show52 = showBands === '52' || showBands === 'both'
  const show13 = showBands === '13' || showBands === 'both'

  return (
    <div className="cot-chart-stack">
      <ResponsiveContainer width="100%" height={340}>
        <ComposedChart data={series} margin={{ top: 12, right: showPrice ? 48 : 12, left: 4, bottom: 4 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
          <XAxis dataKey="label" tick={{ fontSize: 10 }} interval="preserveStartEnd" minTickGap={28} />
          <YAxis yAxisId="pos" tick={{ fontSize: 10 }} tickFormatter={(v) => fmt(v)} width={72} />
          {showPrice ? (
            <YAxis
              yAxisId="price"
              orientation="right"
              tick={{ fontSize: 10 }}
              tickFormatter={(v) => `${Number(v).toFixed(0)}%`}
              width={48}
            />
          ) : null}
          <Tooltip
            content={<ChartTooltip groupId={groupId} hasPrice={showPrice} priceLabel={priceLabel} profile={profile} />}
          />
          <Legend wrapperStyle={{ fontSize: 11 }} />

          {groupId !== 'combined' ? (
            <>
              {bandsOn && show13 ? (
                <>
                  <Line
                    yAxisId="pos"
                    type="monotone"
                    dataKey="band13NetMin"
                    name="13W net min"
                    stroke="#93c5fd"
                    strokeDasharray="3 3"
                    dot={false}
                    strokeWidth={1}
                    connectNulls
                  />
                  <Line
                    yAxisId="pos"
                    type="monotone"
                    dataKey="band13NetMax"
                    name="13W net max"
                    stroke="#93c5fd"
                    strokeDasharray="3 3"
                    dot={false}
                    strokeWidth={1}
                    connectNulls
                  />
                </>
              ) : null}
              {bandsOn && show52 ? (
                <>
                  <Line
                    yAxisId="pos"
                    type="monotone"
                    dataKey="band52NetMin"
                    name="52W net min"
                    stroke="#cbd5e1"
                    strokeDasharray="2 4"
                    dot={false}
                    strokeWidth={1}
                    connectNulls
                  />
                  <Line
                    yAxisId="pos"
                    type="monotone"
                    dataKey="band52NetMax"
                    name="52W net max"
                    stroke="#cbd5e1"
                    strokeDasharray="2 4"
                    dot={false}
                    strokeWidth={1}
                    connectNulls
                  />
                </>
              ) : null}
              {showLongShort ? (
                <>
                  <Line yAxisId="pos" type="monotone" dataKey="long" name="Long" stroke="#1d4ed8" dot={false} strokeWidth={2} />
                  <Line yAxisId="pos" type="monotone" dataKey="short" name="Short" stroke="#b91c1c" dot={false} strokeWidth={2} />
                </>
              ) : null}
              {showNet ? (
                <Line yAxisId="pos" type="monotone" dataKey="net" name="Net" stroke="#0f766e" dot={false} strokeWidth={2.5} />
              ) : null}
            </>
          ) : groupId === 'combined' ? (
            <>
              <Line
                yAxisId="pos"
                type="monotone"
                dataKey="mmNet"
                name="Non-commercial net"
                stroke="#1d4ed8"
                dot={false}
                strokeWidth={2}
                connectNulls
              />
              <Line
                yAxisId="pos"
                type="monotone"
                dataKey="commNet"
                name="Commercial net"
                stroke="#ca8a04"
                dot={false}
                strokeWidth={2}
                connectNulls
              />
              <Line
                yAxisId="pos"
                type="monotone"
                dataKey="nrNet"
                name="Non-reportable net"
                stroke="#7c3aed"
                dot={false}
                strokeWidth={2}
                connectNulls
              />
            </>
          ) : null}

          {showPrice ? (
            <Line
              yAxisId="price"
              type="monotone"
              dataKey="pricePct"
              name={priceLabel}
              stroke="#64748b"
              strokeDasharray="4 3"
              dot={false}
              strokeWidth={1.5}
              connectNulls
            />
          ) : null}
        </ComposedChart>
      </ResponsiveContainer>

      {groupId !== 'combined' && show52 ? (
        <p className="cot-chart-meta">
          Dashed net line uses rolling min/max bands: 13-week (blue shading on net when enabled). Toggle
          52-week range in header when enough history exists ({series.length} weeks loaded).
        </p>
      ) : null}
    </div>
  )
}
