/**
 * Workstation module — institutional research UI foundation.
 *
 * Layers:
 *   data/       — normalization & alignment (no model logic)
 *   hooks/      — data retrieval
 *   charts/     — rendering (lightweight-charts)
 *   context/    — shared weekly timeline state for panel sync
 *   components/ — sync indicators & future panel adapters
 *
 * Future valuation models plug in via instrument_valuation_history export +
 * fairValuePoints overlay — no chart rewrite required.
 */

export { InstrumentWorkstationLayout } from './InstrumentWorkstationLayout.jsx'
export { InstrumentResearchWorkstation, InstrumentResearchWorkstationPanel } from './InstrumentResearchWorkstation.jsx'
export { WeeklyTimelineProvider, useWeeklyTimeline, useWeeklyTimelineOptional } from './context/WeeklyTimelineContext.jsx'
export { useWorkstationData } from './hooks/useWorkstationData.js'
export { useInstrumentValuationHistory } from './hooks/useInstrumentValuationHistory.js'
export { WeeklyCandlestickChart } from './charts/WeeklyCandlestickChart.jsx'
export { TimelineSyncIndicator } from './components/TimelineSyncIndicator.jsx'

