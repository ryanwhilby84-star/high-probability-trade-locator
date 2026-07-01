import React from 'react'

import { CotWorkstation } from '../workstation/CotWorkstation.jsx'

/** @deprecated use CotWorkstation directly */
export function PositioningChartStack({ marketId }) {
  return <CotWorkstation marketId={marketId} />
}
