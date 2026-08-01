import React from 'react'
import './weeklyAnalysis.css'

const WORKFLOW_SEQUENCE = [
  'NORMAL',
  'OPPOSITION_BUILDING',
  'OPPOSITION_MATURE',
  'ROTATION_WATCH',
  'EARLY_ROTATION',
  'CONFIRMED_ROTATION',
  'POST_ROTATION',
]

function humanize(code) {
  if (!code) return '—'
  return String(code).replace(/_/g, ' ')
}

function WorkflowLadder({ stage, sequence }) {
  const steps = sequence?.length ? sequence : WORKFLOW_SEQUENCE
  const idx = steps.indexOf(stage)
  return (
    <ol className="cwa-ladder" aria-label={`Workflow: ${humanize(stage)}`}>
      {steps.map((step, i) => (
        <li
          key={step}
          className={
            i === idx
              ? 'cwa-ladder-step is-current'
              : i < idx
                ? 'cwa-ladder-step is-past'
                : 'cwa-ladder-step'
          }
        >
          {humanize(step)}
        </li>
      ))}
    </ol>
  )
}

function Section({ question, title, children }) {
  return (
    <section className="cwa-block">
      <h3 className="cwa-block-title">{title}</h3>
      {question ? <p className="cwa-block-q">{question}</p> : null}
      {children}
    </section>
  )
}

/**
 * Floating Weekly Analysis window — closed by default; charts stay primary.
 * Driven by trajectory_reasoning export (engine === 'trajectory_reasoning').
 */
export function WeeklyAnalysisPanel({ open, onClose, intel }) {
  const panelRef = React.useRef(null)
  const dragRef = React.useRef(null)
  const [pos, setPos] = React.useState({ x: 24, y: 72 })
  const [size, setSize] = React.useState({ w: 380, h: 520 })

  React.useEffect(() => {
    if (!open) return undefined
    const onKey = (e) => {
      if (e.key === 'Escape') onClose?.()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open, onClose])

  const onDragStart = (e) => {
    if (e.button !== 0) return
    if (e.target.closest('button')) return
    const startX = e.clientX
    const startY = e.clientY
    const orig = { ...pos }
    dragRef.current = { startX, startY, orig }
    const onMove = (ev) => {
      if (!dragRef.current) return
      const dx = ev.clientX - dragRef.current.startX
      const dy = ev.clientY - dragRef.current.startY
      setPos({
        x: Math.max(8, dragRef.current.orig.x + dx),
        y: Math.max(8, dragRef.current.orig.y + dy),
      })
    }
    const onUp = () => {
      dragRef.current = null
      window.removeEventListener('pointermove', onMove)
      window.removeEventListener('pointerup', onUp)
    }
    window.addEventListener('pointermove', onMove)
    window.addEventListener('pointerup', onUp)
  }

  const onResizeStart = (e) => {
    e.preventDefault()
    e.stopPropagation()
    const startX = e.clientX
    const startY = e.clientY
    const orig = { ...size }
    const onMove = (ev) => {
      setSize({
        w: Math.max(300, Math.min(640, orig.w + (ev.clientX - startX))),
        h: Math.max(280, Math.min(820, orig.h + (ev.clientY - startY))),
      })
    }
    const onUp = () => {
      window.removeEventListener('pointermove', onMove)
      window.removeEventListener('pointerup', onUp)
    }
    window.addEventListener('pointermove', onMove)
    window.addEventListener('pointerup', onUp)
  }

  if (!open) return null

  const isTrajectory = intel?.engine === 'trajectory_reasoning'
  const story = intel?.dominant_story || {}
  const workflow = intel?.workflow_state || {}
  const positioning = intel?.positioning_trajectory || {}
  const priceRel = intel?.price_relationship || {}
  const rotation = intel?.rotation_factor || {}
  const hist = intel?.historical_context || {}
  const confirmation = intel?.confirmation || []
  const invalidation = intel?.invalidation || []

  return (
    <aside
      ref={panelRef}
      className="cwa-float"
      style={{ left: pos.x, top: pos.y, width: size.w, height: size.h }}
      aria-label="Weekly analysis"
      role="dialog"
      aria-modal="false"
    >
      <header className="cwa-float-head" onPointerDown={onDragStart}>
        <div>
          <h2 className="cwa-title">Weekly Analysis</h2>
          {intel?.source_week ? (
            <p className="cwa-week">Report week {intel.source_week}</p>
          ) : null}
          {isTrajectory ? (
            <p className="cwa-week">Trajectory engine</p>
          ) : intel?.engine ? (
            <p className="cwa-week cwa-muted">Unexpected engine: {intel.engine}</p>
          ) : null}
        </div>
        <button
          type="button"
          className="cwa-close"
          onClick={onClose}
          title="Close weekly analysis"
        >
          Close
        </button>
      </header>

      <div className="cwa-float-body">
        {!intel?.available ? (
          <p className="cwa-muted">
            {intel?.reason ||
              'Run python scripts/run_analyst_intelligence.py after weekly inspector + OHLC exports.'}
          </p>
        ) : !isTrajectory ? (
          <p className="cwa-muted">
            Weekly Analysis expects trajectory_reasoning output. Rebuild with{' '}
            <code>python scripts/run_analyst_intelligence.py</code> — legacy analyst templates are
            no longer consumed.
          </p>
        ) : (
          <>
            <p className="cwa-disclaimer">{intel.disclaimer}</p>

            <Section title="Dominant Story" question="What is the read?">
              <p className="cwa-story-label">{story.label || humanize(story.code)}</p>
              <p className="cwa-narrative">{story.narrative || intel.summary}</p>
            </Section>

            <Section title="Workflow State" question="Where are we in the rotation sequence?">
              <p className="cwa-prog-state">
                {humanize(workflow.stage)}
                {workflow.structural_state && workflow.structural_state !== workflow.stage
                  ? ` · structural ${humanize(workflow.structural_state)}`
                  : ''}
              </p>
              <WorkflowLadder stage={workflow.stage} sequence={workflow.sequence} />
              {workflow.narrative ? <p className="cwa-muted">{workflow.narrative}</p> : null}
            </Section>

            <Section title="Positioning Trajectory" question="How are participant paths evolving?">
              <ul className="cwa-list">
                {(positioning.lines || []).map((line) => (
                  <li key={line}>{line}</li>
                ))}
              </ul>
            </Section>

            <Section title="Price Relationship" question="Is price agreeing, leading, or resisting?">
              <p className="cwa-narrative">{priceRel.narrative}</p>
            </Section>

            <Section title="Rotation Factor" question="How advanced is the rotation evidence?">
              <p className="cwa-rf-score">
                <strong>
                  {rotation.rotation_factor != null && Number.isFinite(Number(rotation.rotation_factor))
                    ? Number(rotation.rotation_factor).toFixed(1)
                    : '—'}
                </strong>
                <span>
                  {' '}
                  → {rotation.label || humanize(rotation.band || rotation.classification)}
                </span>
              </p>
              {rotation.narrative ? <p className="cwa-muted">{rotation.narrative}</p> : null}
            </Section>

            <Section title="Confirmation" question="What would advance the setup?">
              <ul className="cwa-list cwa-check-list--ok">
                {confirmation.length ? (
                  confirmation.map((line) => <li key={line}>✓ {line}</li>)
                ) : (
                  <li className="cwa-muted">No confirmation items from trajectory engine.</li>
                )}
              </ul>
            </Section>

            <Section title="Invalidation" question="What would kill the thesis?">
              <ul className="cwa-list cwa-check-list--miss">
                {invalidation.length ? (
                  invalidation.map((line) => <li key={line}>✗ {line}</li>)
                ) : (
                  <li className="cwa-muted">No invalidation items from trajectory engine.</li>
                )}
              </ul>
            </Section>

            <Section title="Historical Context" question="Have we seen this before?">
              <p>{hist.summary}</p>
              {hist.outcomes_note ? <p className="cwa-muted">{hist.outcomes_note}</p> : null}
            </Section>
          </>
        )}
      </div>

      <div
        className="cwa-resize"
        onPointerDown={onResizeStart}
        title="Drag to resize"
        aria-hidden="true"
      />
    </aside>
  )
}

export function resolveWeeklyAnalysisBlock(doc, marketId, matchedKey) {
  if (!doc?.markets) return null
  const markets = doc.markets
  return (
    markets[marketId] ||
    (matchedKey ? markets[matchedKey] : null) ||
    Object.entries(markets).find(
      ([k]) => String(k).toLowerCase() === String(marketId || '').toLowerCase(),
    )?.[1] ||
    null
  )
}

/** @deprecated use resolveWeeklyAnalysisBlock */
export const resolveAnalystIntelligenceBlock = resolveWeeklyAnalysisBlock
