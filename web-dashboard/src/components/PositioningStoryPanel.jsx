import React from 'react'

import { usePositioningStory } from '../hooks/usePositioningStory.js'
import { navigateToPositioningStory } from '../routing.js'
import { PositioningStoryTable } from './PositioningStoryTable.jsx'

function StoryErrorBanner({ missingPaths, errors }) {
  if (!missingPaths?.length) return null
  return (
    <div className="comm-research-error" role="alert">
      <strong>Positioning story data unavailable</strong>
      <p className="comm-research-error-lede">
        Expected JSON export under <code>public/data/</code>. Regenerate with:{' '}
        <code>python -m hptl.fx.run_positioning_story_score</code>
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

/** Commercial vs non-commercial story layer — attention only, no trade signals. */
export function PositioningStoryPanel({ showNavLink = false }) {
  const { rows, loading, missingPaths, errors, ready, calendarWeek } = usePositioningStory()

  return (
    <section className="comm-research-panel positioning-story-panel" aria-label="FX positioning story">
      <header className="comm-research-header">
        <div>
          <h2 className="comm-research-title">Positioning story</h2>
          <p className="comm-research-subtitle">
            Commercial vs non-commercial relationship · Week {calendarWeek} · Attention layer only — not ranked or
            scored in scanner
          </p>
        </div>
        {showNavLink ? (
          <button type="button" className="ws-btn" onClick={navigateToPositioningStory}>
            Full page
          </button>
        ) : null}
      </header>

      {loading ? <p className="comm-research-loading">Loading positioning story export…</p> : null}

      {!loading && missingPaths.length ? <StoryErrorBanner missingPaths={missingPaths} errors={errors} /> : null}

      {!loading && ready ? (
        <>
          <p className="comm-research-meta">{rows.length} currencies · sorted by |story score|</p>
          <PositioningStoryTable rows={rows} />
        </>
      ) : null}

      {!loading && !missingPaths.length && !ready ? (
        <p className="comm-research-loading">No positioning story rows — check export.</p>
      ) : null}
    </section>
  )
}
