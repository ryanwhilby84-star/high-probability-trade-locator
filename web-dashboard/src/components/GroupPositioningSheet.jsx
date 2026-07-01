import React from 'react'
import { CotRawDataTable } from './CotRawDataTable.jsx'
import { ThreeYearContextBlock } from '../legacy/dashboardLegacy.jsx'

/** Raw spreadsheet + 3Y context for one participant group (tabbed below charts). */
export function GroupPositioningSheet({ rawRows, rolling3y, multiyear, groupLabel }) {
  return (
    <div className="group-positioning-sheet">
      <h4 className="wo-cot-section-title" style={{ marginTop: '8px' }}>
        Raw positioning data
      </h4>
      <CotRawDataTable rows={rawRows} groupLabel={groupLabel} />

      <ThreeYearContextBlock ctx={rolling3y} multiyear={multiyear} hideDeepAudit />
    </div>
  )
}
