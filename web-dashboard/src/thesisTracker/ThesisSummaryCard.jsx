import React from 'react'

import { getOpportunity } from '../thesisTracker/opportunityModel.js'



function PillarRow({ label, state, scoreDisplay, pending }) {

  return (

    <div className={`toe-pillar${pending ? ' toe-pillar--pending' : ''}`}>

      <span className="toe-pillar-k">{label}</span>

      <span className="toe-pillar-v">{state || '—'}</span>

      {scoreDisplay && scoreDisplay !== '—' ? <span className="toe-pillar-sub">{scoreDisplay}</span> : null}

    </div>

  )

}



export function ThesisSummaryCard({ thesis, compact }) {

  const opp = getOpportunity(thesis)

  const s = opp.summary || {}

  const align = opp.alignment || { label: '—' }

  const actionKey = opp.action_key || 'no_edge'



  return (

    <article className={`toe-summary${compact ? ' toe-summary--compact' : ''}`} aria-label="Thesis summary">

      <h2 className="toe-summary-title">{s.instrument_display || thesis.market}</h2>

      <div className="toe-align-row">

        <span className="toe-align-label">Alignment</span>

        <span className="toe-align-num">{align.label}</span>

      </div>

      <div className="toe-pillars">

        <PillarRow

          label="Valuation"

          state={s.valuation?.state}

          scoreDisplay={s.valuation?.score_display}

          pending={!opp.alignment?.pillars?.find((p) => p.pillar === 'valuation')?.wired}

        />

        <PillarRow

          label="Institutions"

          state={s.institutions?.state}

          scoreDisplay={s.institutions?.score_display}

        />

        <PillarRow label="Retail" state={s.retail?.state} scoreDisplay={s.retail?.score_display} />

        <PillarRow

          label="Seasonality"

          state={s.seasonality?.state}

          scoreDisplay={s.seasonality?.score_display}

          pending={!opp.alignment?.pillars?.find((p) => p.pillar === 'seasonality')?.wired}

        />

        <PillarRow label="Location" state={s.location?.state} scoreDisplay={s.location?.score_display} />

      </div>

      <div className={`toe-action toe-action--${actionKey}`}>{opp.action}</div>

    </article>

  )

}

