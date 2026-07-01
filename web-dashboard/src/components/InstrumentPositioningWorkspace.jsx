import React from 'react'

import { CotWorkstation } from '../workstation/CotWorkstation.jsx'

export function InstrumentPositioningWorkspace({ marketId }) {
  return (
    <section
      id="instrument-positioning-workspace"
      className="instrument-positioning-workspace instrument-positioning-workspace--dark instrument-positioning-workspace--canvas"
      aria-label="COT positioning workspace"
    >
      <div className="instrument-positioning-head">
        <div className="instrument-positioning-head-row">
          <div>
            <h3 className="wo-cot-title">Positioning</h3>
            <p className="wo-cot-sub">
              Weekly price and COT net positioning — hover any panel to compare the same week.
            </p>
          </div>
        </div>
      </div>

      <CotWorkstation marketId={marketId} />
    </section>
  )
}
