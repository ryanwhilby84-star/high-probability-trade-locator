import React from 'react'

import { useCommercialStrengthResearch } from '../hooks/useCommercialStrengthResearch.js'
import { navigateToCommercialStrengthResearch } from '../routing.js'
import { CommercialStrengthResearchTable } from './CommercialStrengthResearchTable.jsx'

function ResearchErrorBanner({ missingPaths, errors }) {
  if (!missingPaths?.length) return null
  return (
    <div className="comm-research-error" role="alert">
      <strong>Commercial strength research data unavailable</strong>
      <p className="comm-research-error-lede">
        Expected JSON exports under <code>public/data/</code>. Regenerate with:{' '}
        <code>python -m hptl.fx.run_commercial_strength_research</code>
      </p>
      <ul className="comm-research-error-list">
        {missingPaths.map((path) => (
          <li key={path}>
            <code>{path}</code>
            {errors?.find((e) => e.includes(path)) ? (
              <span className="comm-research-error-detail">
                {' '}
                — {errors.find((e) => e.includes(path))}
              </span>
            ) : null}
          </li>
        ))}
      </ul>
    </div>
  )
}

/** Research-only panel — spec vs commercial strength. Not scored in scanner or RS. */
export function CommercialStrengthResearchPanel({ compact = false }) {
  const { rows, loading, missingPaths, errors, ready, calendarWeek } = useCommercialStrengthResearch()

  return (
    <section className="comm-research-panel" aria-label="Commercial strength research">
      <header className="comm-research-header">
        <div>
          <h2 className="comm-research-title">Commercial strength research</h2>
          <p className="comm-research-subtitle">
            Spec vs commercial positioning strength · Week {calendarWeek} · Research only — not ranked or scored
          </p>
        </div>
        {!compact ? (
          <button type="button" className="ws-btn" onClick={navigateToCommercialStrengthResearch}>
            Full page
          </button>
        ) : null}
      </header>

      {loading ? <p className="comm-research-loading">Loading research exports…</p> : null}

      {!loading && missingPaths.length ? (
        <ResearchErrorBanner missingPaths={missingPaths} errors={errors} />
      ) : null}

      {!loading && ready ? (
        <>
          <p className="comm-research-meta">{rows.length} currencies · sorted by |divergence|</p>
          <CommercialStrengthResearchTable rows={rows} />
        </>
      ) : null}

      {!loading && !missingPaths.length && !ready ? (
        <p className="comm-research-loading">No research rows — check commercial strength exports.</p>
      ) : null}
    </section>
  )
}
