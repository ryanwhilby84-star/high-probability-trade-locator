import React from 'react'
import { buildMacroHealthSummary } from '../macroRelationshipMapData.js'

const TONE = {
  emerald: 'border-emerald-500/30 bg-emerald-950/30 text-emerald-200',
  sky: 'border-sky-500/30 bg-sky-950/30 text-sky-200',
  amber: 'border-amber-500/30 bg-amber-950/30 text-amber-200',
  rose: 'border-rose-500/30 bg-rose-950/30 text-rose-200',
  slate: 'border-slate-600/40 bg-slate-900/40 text-slate-300',
}

function fmtTs(ts) {
  if (!ts) return '—'
  const d = new Date(ts)
  if (Number.isNaN(d.getTime())) return String(ts)
  return d.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function Stat({ label, value, tone = 'slate' }) {
  return (
    <div className={`rounded-xl border px-3 py-2.5 ${TONE[tone] || TONE.slate}`}>
      <div className="text-[1.35rem] font-semibold leading-none tabular-nums">{value}</div>
      <div className="mt-1 text-[0.62rem] font-semibold uppercase tracking-[0.14em] opacity-80">{label}</div>
    </div>
  )
}

/**
 * Dashboard-wide macro data health panel.
 * @param {{ maps?: Record<string, unknown>, summary?: Record<string, unknown> }} props
 */
export function MacroHealthPanel({ maps = null, summary = null }) {
  const s = React.useMemo(() => summary || buildMacroHealthSummary(maps), [summary, maps])
  if (!s || !s.total) return null

  return (
    <section
      className="rounded-2xl border border-slate-700/40 bg-gradient-to-b from-slate-950/90 to-slate-900/60 p-4 sm:p-5"
      aria-label="Macro data health"
    >
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 className="text-sm font-semibold tracking-tight text-slate-100">Macro Health</h2>
        <p className="text-[0.72rem] text-slate-400">
          Coverage{' '}
          <span className="font-semibold text-slate-200 tabular-nums">
            {s.available}/{s.total}
          </span>{' '}
          assets available
        </p>
      </div>

      <div className="mt-3 grid grid-cols-2 gap-2.5 sm:grid-cols-3 lg:grid-cols-6">
        <Stat label="Coverage" value={`${s.available}/${s.total}`} tone="slate" />
        <Stat label="Live" value={s.live} tone="emerald" />
        <Stat label="Cached" value={s.cached} tone="sky" />
        <Stat label="Stale" value={s.stale} tone="amber" />
        <Stat label="Warning" value={s.warning} tone="rose" />
        <Stat label="Missing" value={s.missing} tone="slate" />
      </div>

      <div className="mt-3 flex flex-wrap gap-x-6 gap-y-1 border-t border-slate-800/80 pt-3 text-[0.72rem] text-slate-400">
        <span>
          Last successful refresh:{' '}
          <span className="font-medium text-slate-200">{fmtTs(s.lastSuccessfulRefresh)}</span>
        </span>
        <span>
          Last failed refresh:{' '}
          <span className="font-medium text-slate-300">{fmtTs(s.lastFailedRefresh)}</span>
        </span>
      </div>
    </section>
  )
}

export default MacroHealthPanel
