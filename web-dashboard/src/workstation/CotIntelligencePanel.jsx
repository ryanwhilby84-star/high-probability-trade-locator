import React from 'react'
import './cotIntelligence.css'

const fmt = (v) => {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  const n = Number(v)
  const sign = n > 0 ? '+' : ''
  return `${sign}${n.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
}

const pct = (v) =>
  v == null || !Number.isFinite(Number(v)) ? '—' : `${Number(v).toFixed(0)}th`

const LAYER_OPTIONS = [
  { id: 'commercial_extremes', label: 'Commercial Extremes' },
  { id: 'noncommercial_extremes', label: 'Non-Commercial Extremes' },
  { id: 'nonreportable_extremes', label: 'Non-Reportable Extremes' },
  { id: 'multi_group', label: 'Multi-Group Events' },
]

function qualityClass(q) {
  if (!q) return ''
  if (q.includes('INSUFFICIENT') || q.includes('LOW')) return 'cwi-quality--warn'
  if (q.includes('STRONGER')) return 'cwi-quality--ok'
  return 'cwi-quality--mid'
}

function HorizonCard({ stats }) {
  if (!stats || !stats.n) {
    return (
      <div className="cwi-horizon">
        <strong>{stats?.horizon_weeks ?? '—'}W</strong>
        <p>No measurable outcomes</p>
      </div>
    )
  }
  return (
    <div className="cwi-horizon">
      <strong>{stats.horizon_weeks}W</strong>
      <p className="cwi-horizon-main">
        {stats.higher_count}/{stats.n} higher
        {stats.pct_higher != null ? ` · ${stats.pct_higher}%` : ''}
      </p>
      <p>Median {stats.median_return_pct != null ? `${stats.median_return_pct}%` : '—'}</p>
      <p>Avg {stats.avg_return_pct != null ? `${stats.avg_return_pct}%` : '—'}</p>
      <p className={`cwi-quality ${qualityClass(stats.sample_quality)}`}>{stats.sample_quality}</p>
      {stats.note ? <p className="cwi-horizon-note">{stats.note}</p> : null}
    </div>
  )
}

/**
 * COT Workstation Intelligence V2 side panel.
 */
export function CotIntelligencePanel({
  intel,
  layers,
  onLayersChange,
  selectedEvent,
  onSelectEvent,
  onJumpToDate,
  onBackToCurrent,
  inspectingHistorical,
}) {
  if (!intel?.available) {
    return (
      <aside className="cwi-panel" aria-label="COT intelligence">
        <h2 className="cwi-title">Positioning Intelligence</h2>
        <p className="cwi-muted">
          {intel?.reason ||
            'Run python scripts/run_workstation_intelligence.py to precompute research evidence.'}
        </p>
      </aside>
    )
  }

  const panel = intel.intelligence_panel || {}
  const setup = panel.setup_summary || {}
  const analogues = intel.analogues || {}
  const outcomes = analogues.outcomes_by_horizon || {}
  const cases = analogues.cases || []

  return (
    <aside className="cwi-panel" aria-label="COT intelligence">
      <header className="cwi-head">
        <h2 className="cwi-title">Current Positioning Setup</h2>
        <p className="cwi-week">COT week {intel.source_week}</p>
      </header>

      <section className="cwi-setup">
        <div className="cwi-setup-row">
          <span>Commercials</span>
          <strong>{setup.commercial || '—'}</strong>
        </div>
        <p className="cwi-setup-detail">{setup.commercial_change}</p>
        <div className="cwi-setup-row">
          <span>Non-Commercials</span>
          <strong>{setup.noncommercial || '—'}</strong>
        </div>
        <div className="cwi-setup-row">
          <span>Non-Reportables</span>
          <strong>{setup.nonreportable || '—'}</strong>
        </div>
        <div className="cwi-setup-row">
          <span>Historical analogues</span>
          <strong>{setup.analogue_cases ?? 0} independent</strong>
        </div>
        <div className="cwi-setup-row">
          <span>Sample quality</span>
          <strong className={qualityClass(setup.sample_quality)}>{setup.sample_quality || '—'}</strong>
        </div>
        {setup.best_horizon_weeks != null ? (
          <div className="cwi-setup-row">
            <span>Best-supported horizon</span>
            <strong>
              {setup.best_horizon_weeks}W · {setup.best_horizon_note}
            </strong>
          </div>
        ) : null}
        <p className="cwi-disclaimer">{setup.disclaimer}</p>
      </section>

      <section className="cwi-block">
        <h3>State</h3>
        <p>
          <strong>Commercials — </strong>
          {panel.state?.commercial}
        </p>
        <p>
          <strong>Non-Commercials — </strong>
          {panel.state?.noncommercial}
        </p>
        <p>
          <strong>Non-Reportables — </strong>
          {panel.state?.nonreportable}
        </p>
      </section>

      <section className="cwi-block">
        <h3>Change</h3>
        <p>{panel.change?.commercial_narrative}</p>
        {(panel.change?.commercial_metrics || []).length ? (
          <ul className="cwi-metrics">
            {panel.change.commercial_metrics.map((m) => (
              <li key={m}>{m}</li>
            ))}
          </ul>
        ) : null}
        <p className="cwi-muted">NC: {panel.change?.noncommercial}</p>
        <p className="cwi-muted">NR: {panel.change?.nonreportable}</p>
      </section>

      <section className="cwi-block">
        <h3>Interpretation</h3>
        <p>{panel.interpretation}</p>
      </section>

      <section className="cwi-block">
        <h3>What to watch</h3>
        <p>{panel.what_to_watch}</p>
      </section>

      <section className="cwi-block">
        <h3>Event layers</h3>
        <div className="cwi-layers">
          {LAYER_OPTIONS.map((opt) => (
            <label key={opt.id} className="cwi-layer">
              <input
                type="checkbox"
                checked={Boolean(layers?.[opt.id])}
                onChange={(e) =>
                  onLayersChange?.({ ...layers, [opt.id]: e.target.checked })
                }
              />
              {opt.label}
            </label>
          ))}
        </div>
      </section>

      {selectedEvent ? (
        <section className="cwi-block cwi-selected">
          <h3>Selected event</h3>
          <p className="cwi-event-label">{selectedEvent.label}</p>
          <ul className="cwi-metrics">
            <li>Date: {selectedEvent.date}</li>
            <li>Group: {selectedEvent.group_label || selectedEvent.group}</li>
            <li>Net: {fmt(selectedEvent.net)}</li>
            <li>Percentile: {pct(selectedEvent.percentile)}</li>
            <li>1W: {fmt(selectedEvent.change_1w)}</li>
            <li>4W: {fmt(selectedEvent.change_4w)}</li>
            <li>12W: {fmt(selectedEvent.change_12w)}</li>
            <li>Class: {selectedEvent.classification}</li>
          </ul>
          <button
            type="button"
            className="cwi-btn"
            onClick={() => onJumpToDate?.(selectedEvent.date)}
          >
            Jump charts to event
          </button>
        </section>
      ) : null}

      <section className="cwi-block">
        <div className="cwi-block-head">
          <h3>Historical analogues</h3>
          {inspectingHistorical ? (
            <button type="button" className="cwi-btn cwi-btn-primary" onClick={onBackToCurrent}>
              Back to current
            </button>
          ) : null}
        </div>
        <p className="cwi-muted">{analogues.matching_method}</p>
        <p>
          <strong>{analogues.independent_case_count ?? 0}</strong> independent cases
          {analogues.raw_match_count_before_dedup != null
            ? ` (${analogues.raw_match_count_before_dedup} raw before cooldown)`
            : ''}
        </p>
        <div className="cwi-horizons">
          {['4', '8', '12', '26'].map((h) => (
            <HorizonCard key={h} stats={outcomes[h]} />
          ))}
        </div>
        <h4 className="cwi-subh">Previous cases</h4>
        {!cases.length ? (
          <p className="cwi-muted">NO RELIABLE HISTORICAL CONCLUSION — no independent analogues.</p>
        ) : (
          <ul className="cwi-cases">
            {cases.map((c) => (
              <li key={c.date}>
                <button
                  type="button"
                  className="cwi-case-btn"
                  onClick={() => {
                    onSelectEvent?.({
                      date: c.date,
                      label: 'HISTORICAL ANALOGUE',
                      group: 'analogue',
                      group_label: 'Analogue',
                      net: null,
                      percentile: c.commercial_percentile,
                      change_1w: null,
                      change_4w: null,
                      change_12w: null,
                      classification: (c.matched_rules || []).join('; '),
                    })
                    onJumpToDate?.(c.date)
                  }}
                >
                  {c.date}
                  <span>
                    {c.outcomes?.['12']
                      ? `12W ${c.outcomes['12'].return_pct > 0 ? '+' : ''}${c.outcomes['12'].return_pct}%`
                      : ''}
                  </span>
                </button>
              </li>
            ))}
          </ul>
        )}
      </section>
    </aside>
  )
}

export { LAYER_OPTIONS }
