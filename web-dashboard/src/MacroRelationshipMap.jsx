import React from 'react'
import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceArea,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { interpretMacroVsTapeTension } from './macroIntelligence.js'
import {
  buildMacroMapRead,
  expectsMacroRelationshipMap,
  getMacroMapDivergenceLens,
  getMacroRelationshipDrivers,
  marketsMacroAlign,
  readMacroFreshness,
} from './macroRelationshipMapData.js'
import {
  buildOverlayChartRows,
  buildOverlayInterpretationHeadline,
  correlationRegimeLabel,
  getSeriesPair,
  mergeShadeSegments,
  plainRollingRead,
  rollingStripLabels,
  shadeFill,
} from './macroRelationshipChartNarrative.js'

const toneSection = {
  emerald: 'mrm-tone-emerald',
  amber: 'mrm-tone-amber',
  slate: '',
}

const toneStatus = {
  emerald: 'mrm-status-card mrm-tone-emerald',
  amber: 'mrm-status-card mrm-tone-amber',
  slate: 'mrm-status-card',
}

function getToneSection(tone) {
  return toneSection[tone] ?? toneSection.slate
}

function getToneStatus(tone) {
  return toneStatus[tone] ?? toneStatus.slate
}

function fmtCorr(v) {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  return Number(v).toFixed(2)
}

const FRESH_TONE = {
  emerald: 'border-emerald-500/35 bg-emerald-950/40 text-emerald-200/95',
  sky: 'border-sky-500/35 bg-sky-950/40 text-sky-200/95',
  amber: 'border-amber-500/35 bg-amber-950/40 text-amber-200/95',
  rose: 'border-rose-500/35 bg-rose-950/40 text-rose-200/95',
  slate: 'border-slate-600/50 bg-slate-900/60 text-slate-300',
}

function fmtRefreshTs(ts) {
  if (!ts) return '—'
  const d = new Date(ts)
  if (Number.isNaN(d.getTime())) return String(ts)
  return d.toLocaleString(undefined, { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit' })
}

function MacroFreshnessChip({ fresh }) {
  if (!fresh) return null
  return (
    <span
      className={`rounded-md border px-2 py-0.5 text-[0.6rem] font-semibold uppercase tracking-wider ${FRESH_TONE[fresh.tone] || FRESH_TONE.slate}`}
      title={`Macro data status: ${fresh.status}${fresh.carriedOver ? ' (carried over from last good refresh)' : ''}`}
    >
      {fresh.label}
    </span>
  )
}

function MacroFreshnessStrip({ fresh }) {
  if (!fresh) return null
  const ids = (fresh.sourceSeriesIds || []).join(' · ')
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 rounded-lg border border-slate-800/70 bg-slate-950/40 px-3 py-2 text-[0.68rem] text-slate-400">
      <MacroFreshnessChip fresh={fresh} />
      {ids ? (
        <span>
          Source series: <span className="font-mono text-slate-300">{ids}</span>
        </span>
      ) : null}
      {fresh.latestObservationDate ? (
        <span>
          Obs end: <span className="text-slate-300">{fresh.latestObservationDate}</span>
        </span>
      ) : null}
      {fresh.latencyDays != null ? (
        <span>
          Latency: <span className="text-slate-300">{fresh.latencyDays}d</span>
        </span>
      ) : null}
      <span>
        Last refresh: <span className="text-slate-300">{fmtRefreshTs(fresh.lastSuccessfulRefresh)}</span>
      </span>
      {fresh.carriedOver ? (
        <span className="text-amber-300/90">Showing last valid data (refresh failed)</span>
      ) : null}
    </div>
  )
}

function cadenceBadge(cadence) {
  const c = String(cadence || 'daily').toLowerCase()
  if (c === 'monthly') return 'Monthly'
  if (c === 'quarterly') return 'Quarterly'
  return 'Daily'
}

export function humanMacroMapUnavailableReason(err) {
  const s = String(err || '').trim()
  if (!s) return 'Macro overlay could not be built for this market in the latest export.'
  if (/insufficient overlapping/i.test(s)) return 'Not enough overlapping history yet to plot this pair reliably.'
  if (/FRED fetch failed|HTTPError|timeout/i.test(s)) return 'Macro data source was unreachable — try again after a rebuild or check connectivity.'
  if (/Insufficient rows/i.test(s)) return 'Series overlap is too thin for a stable chart window.'
  return 'Macro overlay is temporarily unavailable for this contract.'
}

const CHART_AREA_CLASS = 'relative min-h-[40rem] h-[min(56rem,85vh)]'
const CHART_AREA_COMPACT_CLASS = 'relative min-h-[12rem] h-52 sm:min-h-[13rem] sm:h-56'

function MrmTooltip({ active, payload, priceLabel, driverLabel }) {
  if (!active || !payload?.length) return null
  const p = payload[0].payload
  return (
    <div className="mrm-tip">
      <div className="mrm-tip-date">{p.dt}</div>
      <div className="flex items-center justify-between gap-6 py-0.5 mrm-tip-line--price">
        <span>{priceLabel}</span>
        <span className="font-mono tabular-nums">{Number(p.nq).toFixed(2)}%</span>
      </div>
      <div className="flex items-center justify-between gap-6 py-0.5 mrm-tip-line--driver">
        <span>{driverLabel}</span>
        <span className="font-mono tabular-nums">{Number(p.y10).toFixed(2)}%</span>
      </div>
      <p className="mrm-tip-foot">
        Rebased % from the first date on this chart (not annualized).
      </p>
    </div>
  )
}

function ChartSkeleton({ expectsMacro = false }) {
  const bars = [38, 55, 44, 62, 51, 58, 41, 53, 47, 59, 43, 56]
  return (
    <div
      className={`${CHART_AREA_CLASS} overflow-hidden rounded-2xl border border-slate-700/50 bg-gradient-to-b from-slate-950/98 via-slate-900/90 to-slate-950`}
      aria-hidden="true"
    >
      <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(ellipse_70%_55%_at_50%_0%,rgba(56,189,248,0.07),transparent_55%)]" />
      <div className="absolute left-4 top-4 flex items-center gap-2">
        <span className="h-1.5 w-1.5 rounded-full bg-sky-400/60 shadow-[0_0_12px_rgba(56,189,248,0.45)]" />
        <span className="text-[0.68rem] font-medium uppercase tracking-[0.14em] text-slate-500">Macro overlay</span>
        <span className="rounded border border-slate-600/60 bg-slate-900/90 px-2 py-0.5 text-[0.6rem] font-semibold uppercase tracking-wider text-slate-500">
          Unavailable
        </span>
      </div>
      <div className="pointer-events-none absolute bottom-24 left-4 right-4 top-16 flex flex-col justify-between opacity-[0.1]">
        {[0, 1, 2, 3, 4].map((i) => (
          <div key={i} className="h-px w-full bg-slate-400" />
        ))}
      </div>
      <div className="absolute bottom-0 left-0 right-0 flex h-[5.5rem] items-end justify-between gap-[3px] px-4 pb-3 pt-10">
        {bars.map((h, i) => (
          <div key={i} className="relative flex-1">
            <div
              className="mx-auto w-full max-w-[12px] rounded-t-[3px] bg-gradient-to-t from-slate-800/90 to-sky-500/25 ring-1 ring-slate-700/30"
              style={{ height: `${h}%` }}
            />
          </div>
        ))}
      </div>
      <div className="absolute bottom-3 right-4 max-w-[min(100%,28rem)] text-right text-[0.7rem] leading-snug text-slate-500">
        {expectsMacro ? (
          <>
            <span className="font-medium text-amber-200/90">Data source pending.</span> Re-run the confluence export so this contract&apos;s macro map is included in the weekly JSON bundle.
          </>
        ) : (
          'Macro price overlay is not published for this symbol in the current dashboard bundle.'
        )}
      </div>
    </div>
  )
}

function LiveMacroOverlay({ rm, compact = false }) {
  const { priceLabel, driverLabel, cadence } = getSeriesPair(rm)
  const chartRows = React.useMemo(() => buildOverlayChartRows(rm), [rm])
  const shadeSegs = React.useMemo(() => mergeShadeSegments(chartRows), [chartRows])

  const yDomain = React.useMemo(() => {
    if (!chartRows.length) return [-1, 1]
    let lo = Infinity
    let hi = -Infinity
    for (const r of chartRows) {
      lo = Math.min(lo, r.nq, r.y10)
      hi = Math.max(hi, r.nq, r.y10)
    }
    const pad = Math.max((hi - lo) * 0.12, 0.75)
    return [lo - pad, hi + pad]
  }, [chartRows])

  const xTicks = React.useMemo(() => {
    const r = chartRows
    const n = r.length
    if (!n) return []
    if (n === 1) return [r[0].i]
    const mid = Math.floor(n / 2)
    return [r[0].i, r[mid].i, r[n - 1].i]
  }, [chartRows])

  const tickFmt = React.useCallback(
    (val) => {
      const row = chartRows.find((x) => x.i === val)
      if (!row?.dt) return ''
      const d = String(row.dt)
      if (d.length >= 7) return d.slice(0, 7)
      return d
    },
    [chartRows],
  )

  const tip = React.useCallback(
    (props) => <MrmTooltip {...props} priceLabel={priceLabel} driverLabel={driverLabel} />,
    [priceLabel, driverLabel],
  )

  const outer = compact ? CHART_AREA_COMPACT_CLASS : CHART_AREA_CLASS
  const topChrome = compact ? 'left-2 top-2 gap-1.5 sm:left-3 sm:top-2.5' : 'left-3 top-3 sm:left-5 sm:top-4'
  const chartTop = compact ? 'top-9' : 'top-11 sm:top-12'
  const radius = compact ? 'rounded-xl' : 'rounded-2xl'

  return (
    <div
      className={`${outer} overflow-hidden ${radius} border border-slate-600/40 bg-gradient-to-b from-[#070d18] via-slate-950/95 to-slate-950 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]`}
    >
      <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(ellipse_75%_50%_at_50%_-5%,rgba(56,189,248,0.09),transparent_50%)]" />
      <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(ellipse_55%_40%_at_80%_100%,rgba(139,92,246,0.06),transparent_45%)]" />

      <div className={`absolute ${topChrome} z-10 flex flex-wrap items-center gap-2`}>
        <span className={`rounded-full bg-sky-400/80 shadow-[0_0_16px_rgba(56,189,248,0.5)] ${compact ? 'h-1.5 w-1.5' : 'h-2 w-2'}`} />
        <span className={`font-semibold uppercase tracking-[0.16em] text-slate-400 ${compact ? 'text-[0.62rem]' : 'text-[0.68rem]'}`}>
          Live overlay
        </span>
        <span className="rounded-md border border-emerald-500/35 bg-emerald-950/40 px-2 py-0.5 text-[0.6rem] font-semibold uppercase tracking-wider text-emerald-200/95">
          {cadenceBadge(cadence)}
        </span>
      </div>

      <div className={`absolute inset-x-0 bottom-0 ${chartTop}`}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartRows} margin={{ top: 8, right: 14, left: 4, bottom: 6 }}>
            {shadeSegs.map((s, idx) => (
              <ReferenceArea
                key={`${s.zone}-${s.x1}-${s.x2}-${idx}`}
                x1={s.x1}
                x2={s.x2}
                y1={yDomain[0]}
                y2={yDomain[1]}
                strokeOpacity={0}
                fill={shadeFill(s.zone)}
                ifOverflow="visible"
              />
            ))}
            <CartesianGrid strokeDasharray="4 6" stroke="#1e293b" strokeOpacity={0.85} vertical={false} />
            <XAxis
              type="number"
              dataKey="i"
              domain={['dataMin', 'dataMax']}
              ticks={xTicks}
              tickFormatter={tickFmt}
              tick={{ fill: '#64748b', fontSize: 10 }}
              axisLine={{ stroke: '#334155' }}
              tickLine={false}
            />
            <YAxis
              width={48}
              domain={yDomain}
              tickFormatter={(v) => `${Number(v).toFixed(0)}%`}
              tick={{ fill: '#64748b', fontSize: 10 }}
              axisLine={false}
              tickLine={false}
            />
            <Tooltip content={tip} cursor={{ stroke: '#475569', strokeWidth: 1, strokeDasharray: '4 4' }} />
            <Line
              type="linear"
              dataKey="nq"
              name={priceLabel}
              stroke="#38bdf8"
              strokeWidth={2.4}
              dot={false}
              isAnimationActive={false}
              activeDot={{ r: 4, strokeWidth: 0, fill: '#7dd3fc' }}
            />
            <Line
              type="linear"
              dataKey="y10"
              name={driverLabel}
              stroke="#c4b5fd"
              strokeWidth={2.1}
              dot={false}
              isAnimationActive={false}
              activeDot={{ r: 4, strokeWidth: 0, fill: '#ddd6fe' }}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div className={`pointer-events-none absolute left-3 right-3 sm:left-5 sm:right-5 ${compact ? 'bottom-1' : 'bottom-2'}`}>
        <p className={`mb-2 font-semibold uppercase tracking-[0.12em] text-slate-500 ${compact ? 'hidden' : 'text-[0.62rem]'}`}>Legend</p>
        <div className={`flex flex-wrap items-center gap-4 font-medium text-slate-400 ${compact ? 'gap-3 text-[0.62rem]' : 'text-[0.7rem]'}`}>
          <span className="flex items-center gap-2">
            <span className="inline-block h-1 w-8 rounded-full bg-sky-400" />
            {priceLabel}
          </span>
          <span className="flex items-center gap-2">
            <span className="inline-block h-1 w-8 rounded-full bg-violet-300/90" />
            {driverLabel}
          </span>
        </div>
      </div>
    </div>
  )
}

/**
 * Standalone overlay chart for dashboard gallery (same payload as Macro Relationship Map).
 * @param {{ rm: Record<string, unknown>, compact?: boolean }} props
 */
export function MacroRelationshipOverlayChart({ rm, compact = false }) {
  if (!rm || rm.available !== true) return null
  return <LiveMacroOverlay rm={rm} compact={compact} />
}

export function MacroRelationshipMap({
  market,
  row,
  latestParticipation,
  relationshipMapData = null,
  hideWeatherPlaceholder = false,
}) {
  const pc = latestParticipation?.category || ''
  const tension = React.useMemo(() => interpretMacroVsTapeTension(row, pc), [row, pc])
  const macroExpected = expectsMacroRelationshipMap(market)
  const macroFailed = relationshipMapData && relationshipMapData.available === false
  const macroMissing = macroExpected && relationshipMapData == null

  const liveRm =
    relationshipMapData &&
    relationshipMapData.available === true &&
    marketsMacroAlign(relationshipMapData.market, market)

  const lensOpts = React.useMemo(
    () => ({ relationshipMapLive: !!liveRm, relationshipRm: liveRm ? relationshipMapData : null }),
    [liveRm, relationshipMapData],
  )
  const drivers = React.useMemo(() => {
    const m = String(market ?? '').trim()
    const list = getMacroRelationshipDrivers(m, {
      hidePlaceholderWeather: Boolean(hideWeatherPlaceholder),
    })
    return Array.isArray(list) ? list : []
  }, [market, hideWeatherPlaceholder])
  const lens = React.useMemo(() => getMacroMapDivergenceLens(row, tension, lensOpts), [row, tension, lensOpts])
  const read = React.useMemo(() => buildMacroMapRead(market, row, tension, lensOpts), [market, row, tension, lensOpts])
  const ring = getToneSection(lens?.tone)
  const badge = getToneStatus(lens?.tone)

  const rm = liveRm ? relationshipMapData : null
  const fresh = React.useMemo(() => readMacroFreshness(relationshipMapData), [relationshipMapData])
  const chartHeadline = React.useMemo(() => (rm ? buildOverlayInterpretationHeadline(rm) : ''), [rm])
  const rollLab = React.useMemo(() => (rm ? rollingStripLabels(rm) : { a: '', b: '', c: null }), [rm])
  const regime = React.useMemo(() => (rm ? correlationRegimeLabel(rm) : null), [rm])

  const readAudit = React.useMemo(() => {
    if (!rm) return ''
    const extra = rm.interpretation_summary || plainRollingRead(rm)
    return String(extra || '').trim()
  }, [rm])

  const footerPair = React.useMemo(() => {
    if (!rm) return ''
    const { priceLabel, driverLabel } = getSeriesPair(rm)
    return `${priceLabel} × ${driverLabel} (rebased window)`
  }, [rm])

  return (
    <section
      className={`mrm-section ${ring}`}
      aria-labelledby={`mrm-heading-${String(market || 'm').replace(/\s+/g, '-')}`}
    >
      <div className="pointer-events-none absolute -right-24 -top-24 h-48 w-48 rounded-full bg-sky-500/[0.045] blur-3xl" />
      <div className="pointer-events-none absolute -bottom-16 -left-16 h-40 w-40 rounded-full bg-indigo-500/[0.04] blur-3xl" />

      <div className="relative flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <p className="text-[0.65rem] font-semibold uppercase tracking-[0.2em] text-slate-500">Macro intelligence</p>
          <h3 id={`mrm-heading-${String(market || 'm').replace(/\s+/g, '-')}`} className="mt-1 text-lg font-semibold tracking-tight text-slate-100 sm:text-xl">
            Macro Relationship Map
          </h3>
          <p className="mt-1 max-w-2xl text-[0.8rem] leading-relaxed text-slate-500">
            Weekly export: <span className="text-slate-300">{market || '—'}</span> vs one macro driver — chart for shape, optional block below for rolling correlation.
            {liveRm ? (
              <span className="text-slate-500">
                {' '}
                Window {rm.observation_start} → {rm.observation_end}.
              </span>
            ) : (
              <span className="text-slate-500">
                {' '}
                {macroExpected
                  ? 'Overlay appears when this contract is in macro_relationship_maps for the week.'
                  : 'Macro overlay is not published for this index in this bundle.'}
              </span>
            )}
          </p>
        </div>
        <div className="flex shrink-0 items-start gap-2">
          {fresh && (liveRm || fresh.carriedOver) ? (
            <div className="flex flex-col items-end gap-1 pt-0.5">
              <p className="text-[0.58rem] font-semibold uppercase tracking-[0.16em] text-slate-500/90">Data</p>
              <MacroFreshnessChip fresh={fresh} />
            </div>
          ) : null}
          <div className={`rounded-xl border px-3.5 py-2.5 ${badge}`}>
            <p className="text-[0.58rem] font-semibold uppercase tracking-[0.16em] text-slate-500/90">This week</p>
            <p className="mt-0.5 text-sm font-semibold tracking-tight text-slate-100">{lens.status}</p>
          </div>
        </div>
      </div>

      {!liveRm && macroExpected && macroMissing ? (
        <div className="relative mt-4 rounded-xl border border-amber-500/35 bg-amber-950/30 px-4 py-3 text-[0.82rem] leading-relaxed text-amber-100/95">
          <strong className="text-amber-50">Data source pending.</strong> This export does not yet include a macro relationship map for{' '}
          <span className="text-slate-100">{market}</span>. Rebuild the confluence JSON (macro map step) — COT tables are unchanged.
        </div>
      ) : null}

      {!liveRm ? <p className="relative mt-3 text-[0.8rem] leading-relaxed text-slate-400">{lens.detail}</p> : null}

      {liveRm ? (
        <div className="relative mt-6 space-y-3">
          <div className="rounded-xl border border-slate-700/35 bg-slate-900/40 px-4 py-3 sm:px-5 sm:py-4">
            <p className="text-[0.62rem] font-semibold uppercase tracking-[0.18em] text-slate-500">Read on the chart</p>
            <p className="mt-1.5 text-[0.95rem] font-medium leading-snug tracking-tight text-slate-100 sm:text-[1.05rem]">{chartHeadline}</p>
            <p className="mt-2 text-[0.8rem] leading-relaxed text-slate-400">{lens.detail}</p>
            <p className="mt-2 text-[0.68rem] leading-snug text-slate-600">
              Shaded bands: green tint = macro line easing while price firms · rose = macro line pressuring price · amber = both rising (macro ignored for a stretch).
            </p>
          </div>
          <MacroFreshnessStrip fresh={fresh} />
          <LiveMacroOverlay rm={rm} />
          <details className="rounded-lg border border-slate-800/80 bg-slate-950/40 px-3 py-2 text-[0.72rem] text-slate-400">
            <summary className="cursor-pointer select-none text-[0.62rem] font-semibold uppercase tracking-wider text-slate-500">
              Rolling correlation (audit)
            </summary>
            <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-2">
              <span>
                {rollLab.a}: <span className="font-mono text-sky-200/90">{fmtCorr(rm.latest_rolling_corr_20)}</span>
              </span>
              <span className="text-slate-600">·</span>
              <span>
                {rollLab.b}: <span className="font-mono text-violet-200/85">{fmtCorr(rm.latest_rolling_corr_30)}</span>
              </span>
              {rm.latest_rolling_corr_60 != null && Number.isFinite(Number(rm.latest_rolling_corr_60)) && rollLab.c ? (
                <>
                  <span className="text-slate-600">·</span>
                  <span>
                    {rollLab.c}: <span className="font-mono text-slate-200/90">{fmtCorr(rm.latest_rolling_corr_60)}</span>
                  </span>
                </>
              ) : null}
              {regime ? (
                <span
                  className={`rounded border px-2 py-0.5 text-[0.62rem] font-semibold ${
                    regime.tone === 'emerald'
                      ? 'border-emerald-500/30 bg-emerald-950/35 text-emerald-200/90'
                      : regime.tone === 'amber'
                        ? 'border-amber-500/30 bg-amber-950/35 text-amber-200/90'
                        : 'border-slate-600/50 bg-slate-900/60 text-slate-400'
                  }`}
                >
                  {regime.text}
                </span>
              ) : null}
              <span className="rounded border border-slate-600/50 bg-slate-900/50 px-2 py-0.5 text-[0.62rem] font-semibold text-slate-400">
                Weekly bundle
              </span>
            </div>
          </details>
        </div>
      ) : macroExpected ? (
        <div className="relative mt-5">
          {macroFailed ? (
            <div
              className={`${CHART_AREA_CLASS} flex flex-col items-center justify-center rounded-2xl border border-rose-600/35 bg-gradient-to-b from-slate-950 via-rose-950/20 to-slate-950 px-6 text-center`}
            >
              <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-rose-300/90">Macro overlay</p>
              <p className="mt-3 max-w-md text-[0.95rem] leading-snug text-slate-200">
                {humanMacroMapUnavailableReason(relationshipMapData?.error)}
              </p>
            </div>
          ) : (
            <ChartSkeleton expectsMacro />
          )}
        </div>
      ) : (
        <div className="relative mt-5 rounded-2xl border border-slate-700/50 bg-slate-950/50 px-5 py-8 text-[0.85rem] leading-relaxed text-slate-400">
          Macro price overlays ship for the core futures list (equities, metals, energy, grains, softs). This index is outside that set — use the week backdrop and deep read for cross-asset context.
        </div>
      )}

      <div className={`relative mt-8 grid gap-6 ${liveRm ? 'lg:grid-cols-2' : 'lg:grid-cols-5'}`}>
        {!liveRm ? (
          <div className="lg:col-span-2">
            <div className="mt-3 flex flex-wrap items-center gap-2">
              <span className="rounded-md border border-slate-700/60 bg-slate-900/50 px-2 py-1 text-[0.65rem] font-medium uppercase tracking-wider text-slate-500">
                Rolling correlation
              </span>
              <span className="text-[0.72rem] text-slate-500">Macro map —</span>
              <span className="rounded border border-amber-500/25 bg-amber-950/30 px-2 py-0.5 text-[0.68rem] font-semibold text-amber-200/90">
                {macroFailed ? 'Chart error' : macroMissing ? 'Data pending' : macroExpected ? 'Awaiting bundle map' : 'Not applicable'}
              </span>
            </div>
          </div>
        ) : null}

        <div className={`flex flex-col gap-4 ${liveRm ? '' : 'lg:col-span-3'}`}>
          <div className="rounded-xl border border-slate-700/35 bg-slate-900/30 p-4 sm:p-5">
            <p className="text-[0.58rem] font-semibold uppercase tracking-[0.16em] text-slate-500">Current read</p>
            <p className="mt-2 text-[0.84rem] leading-relaxed text-slate-300/95 whitespace-pre-wrap">{read}</p>
            {readAudit ? (
              <details className="mt-3 rounded-lg border border-slate-800/60 bg-slate-950/50 px-3 py-2">
                <summary className="cursor-pointer text-[0.68rem] font-medium text-slate-500">Engine / correlation notes</summary>
                <p className="mt-2 text-[0.78rem] leading-relaxed text-slate-400 whitespace-pre-wrap">{readAudit}</p>
              </details>
            ) : null}
          </div>

          <div className="grid gap-3 sm:grid-cols-2">
            {drivers.map((d) => {
              const isLiveDriver = liveRm && rm && d.id === rm.driver_id
              return (
                <div
                  key={d.id}
                  className="group rounded-xl border border-slate-700/40 bg-slate-950/40 p-3.5 transition-colors hover:border-slate-600/50 hover:bg-slate-900/35 sm:p-4"
                >
                  <div className="flex items-start justify-between gap-2">
                    <p className="text-[0.58rem] font-semibold uppercase tracking-[0.12em] text-slate-500">Driver</p>
                    {isLiveDriver ? (
                      <span className="shrink-0 rounded border border-sky-500/30 bg-sky-950/40 px-1.5 py-0.5 text-[0.58rem] font-medium text-sky-200/90">
                        On chart
                      </span>
                    ) : (
                      <span className="shrink-0 rounded border border-slate-600/50 px-1.5 py-0.5 text-[0.58rem] font-medium text-slate-500">Context</span>
                    )}
                  </div>
                  <p className="mt-1.5 text-[0.92rem] font-semibold text-slate-100">{d.label}</p>
                  <p className="mt-1 text-[0.74rem] leading-snug text-slate-400">{d.relationship}</p>
                  <div className="mt-2 flex flex-wrap items-baseline gap-x-2 gap-y-1 border-t border-slate-800/80 pt-2">
                    <span className="text-[0.65rem] text-slate-500">Short-term link</span>
                    {isLiveDriver ? (
                      <>
                        <span className="text-[0.78rem] text-sky-200/95">
                          {rollLab.a} <span className="font-mono">{fmtCorr(rm.latest_rolling_corr_20)}</span>
                        </span>
                        <span className="text-[0.65rem] text-slate-600">·</span>
                        <span className="text-[0.78rem] text-violet-200/90">
                          {rollLab.b} <span className="font-mono">{fmtCorr(rm.latest_rolling_corr_30)}</span>
                        </span>
                      </>
                    ) : (
                      <>
                        <span className="font-mono text-[0.78rem] text-slate-500">—</span>
                        <span className="text-[0.65rem] text-slate-600">(not on chart)</span>
                      </>
                    )}
                  </div>
                  <p className="mt-1.5 text-[0.65rem] leading-snug text-slate-600">
                    {isLiveDriver ? (
                      <span className="text-slate-500">As of {rm.latest_date}.</span>
                    ) : (
                      <span className="text-slate-500">{d.cohortNote ? `${d.cohortNote}` : lens.status}</span>
                    )}
                  </p>
                </div>
              )
            })}
          </div>
        </div>
      </div>

      <div className="relative mt-6 flex flex-wrap gap-2 border-t border-slate-800/80 pt-4">
        {(drivers ?? []).filter((d) => d?.id).map((d) => (
          <span
            key={`b-${d.id}`}
            className="rounded-full border border-slate-700/50 bg-slate-900/50 px-2.5 py-1 text-[0.65rem] font-medium text-slate-400"
          >
            {d.label ?? '—'}
          </span>
        ))}
        {liveRm && footerPair ? (
          <span className="rounded-full border border-slate-600/50 bg-slate-900/50 px-2.5 py-1 text-[0.62rem] font-medium text-slate-400">
            {footerPair} · weekly bundle
          </span>
        ) : null}
      </div>
    </section>
  )
}
