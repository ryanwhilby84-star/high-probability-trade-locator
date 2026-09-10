import React from 'react'

import { filterMarketsBySidebar } from '../components/AppShell.jsx'
import { CotWorkstation } from '../workstation/CotWorkstation.jsx'
import {
  WorkstationIntegrityPanel,
  WorkstationRenderErrorPanel,
} from '../workstation/WorkstationIntegrityPanel.jsx'
import {
  navigateToCotWorkstation,
  navigateToInstrument,
  navigateToScanner,
} from '../routing.js'

import '../workstation/cotWorkstationPage.css'
import '../workstation/cotWorkstationSoft.css'
import '../workstation/cotWorkstationFinal.css'

/*
 * Keep the fullscreen research layout attached to the rendered route itself.
 * This intentionally wins over the legacy fitted/flex workstation rules: the
 * four panels are analytical charts, not strips that must all fit in one screen.
 */
const FORCED_RESEARCH_LAYOUT = `
.cot-ws-page .cot-workstation--fullscreen .cot-ws-research-stage {
  min-height: 0 !important;
  overflow: hidden !important;
}
.cot-ws-page .cot-workstation--fullscreen .cot-ws-canvas-scroll {
  display: block !important;
  height: 100% !important;
  overflow-y: auto !important;
  overflow-x: hidden !important;
  scrollbar-gutter: stable !important;
}
.cot-ws-page .cot-workstation--fullscreen .cot-ws-panels {
  display: block !important;
  height: auto !important;
  min-height: 0 !important;
  overflow: visible !important;
  padding-bottom: 28px !important;
}
.cot-ws-page .cot-workstation--fullscreen .cot-ws-panel,
.cot-ws-page .cot-workstation--fullscreen .cot-ws-cot-group,
.cot-ws-page .cot-workstation--fullscreen .cot-ws-cot-group .cot-ws-panel--cot {
  display: block !important;
  flex: none !important;
  height: auto !important;
  min-height: 0 !important;
}
.cot-ws-page .cot-workstation--fullscreen .cot-ws-panel--price .cot-ws-panel-body {
  display: block !important;
  flex: none !important;
  height: 430px !important;
  min-height: 430px !important;
  max-height: none !important;
}
.cot-ws-page .cot-workstation--fullscreen .cot-ws-panel--cot .cot-ws-panel-body {
  display: block !important;
  flex: none !important;
  height: 350px !important;
  min-height: 350px !important;
  max-height: none !important;
}
.cot-ws-page .cot-workstation--fullscreen .cot-ws-cot-group {
  border-top: 6px solid #dfe5e2 !important;
}
.cot-ws-page .cot-workstation--fullscreen .cot-ws-panel--cot {
  border-bottom: 6px solid #e9eeec !important;
}
.cot-ws-page .cot-workstation--fullscreen .cot-ws-panel-resize-handle {
  display: none !important;
}
.cot-ws-page .cot-workstation--fullscreen .cot-ws-chart-plot,
.cot-ws-page .cot-workstation--fullscreen .cot-ws-chart-pane,
.cot-ws-page .cot-workstation--fullscreen .cot-ws-chart-canvas {
  min-height: 100% !important;
}
`

class CotWorkstationErrorBoundary extends React.Component {
  constructor(props) { super(props); this.state = { error: null, retryToken: 0 } }
  static getDerivedStateFromError(error) { return { error } }
  componentDidCatch(error, info) { console.error('[cot-workstation] WORKSTATION RENDERING ERROR', this.props.marketId, error, info) }
  handleRetry = () => { this.setState((s) => ({ error: null, retryToken: s.retryToken + 1 })) }
  render() {
    if (this.state.error) return <WorkstationRenderErrorPanel instrumentId={this.props.marketId} error={this.state.error} onRetry={this.handleRetry} />
    return <React.Fragment key={this.state.retryToken}>{this.props.children}</React.Fragment>
  }
}

export function CotWorkstationPage({ marketId, trackedMarkets, sidebarClass }) {
  const navMarkets = React.useMemo(() => filterMarketsBySidebar(trackedMarkets, sidebarClass), [trackedMarkets, sidebarClass])
  const navIndex = navMarkets.indexOf(marketId)
  const prevMarket = navIndex > 0 ? navMarkets[navIndex - 1] : null
  const nextMarket = navIndex >= 0 && navIndex < navMarkets.length - 1 ? navMarkets[navIndex + 1] : null
  const [routePayload, setRoutePayload] = React.useState(null)
  const [retryNonce, setRetryNonce] = React.useState(0)

  React.useEffect(() => {
    let cancelled = false
    setRoutePayload(null)
    fetch(`/api/workstation/${encodeURIComponent(marketId)}`, { cache: 'no-store' })
      .then(async (r) => { let body=null; try { body=await r.json() } catch { body=null }; if (!cancelled && body && typeof body === 'object' && body.status) setRoutePayload(body) })
      .catch(() => {})
    return () => { cancelled = true }
  }, [marketId, retryNonce])

  const integrityFailed = routePayload?.status === 'integrity_error'

  return (
    <div className="cot-ws-page">
      <style>{FORCED_RESEARCH_LAYOUT}</style>
      <header className="cot-ws-page-topbar">
        <div className="cot-ws-page-topbar-left">
          <button type="button" className="cot-ws-page-btn" onClick={navigateToScanner}>← Scanner</button>
          <button type="button" className="cot-ws-page-btn" onClick={() => navigateToInstrument(marketId)}>Instrument</button>
        </div>
        <div className="cot-ws-page-topbar-center">
          <span className="cot-ws-page-title">{marketId}</span>
          <span className="cot-ws-page-subtitle">COT Workstation · Institutional Edge</span>
        </div>
        <div className="cot-ws-page-topbar-right">
          <label className="cot-ws-page-market-select"><span className="sr-only">Market</span><select className="cot-ws-page-select" value={marketId} onChange={(e) => navigateToCotWorkstation(e.target.value)}>{navMarkets.map((m) => <option key={m} value={m}>{m}</option>)}</select></label>
          <button type="button" className="cot-ws-page-btn" disabled={!prevMarket} onClick={() => prevMarket && navigateToCotWorkstation(prevMarket)}>Prev</button>
          <button type="button" className="cot-ws-page-btn" disabled={!nextMarket} onClick={() => nextMarket && navigateToCotWorkstation(nextMarket)}>Next</button>
        </div>
      </header>
      <main className="cot-ws-page-body">
        {integrityFailed ? <WorkstationIntegrityPanel instrumentId={routePayload.instrument_id || marketId} reportDate={routePayload.report_date} stage={routePayload.stage || 'Derived COT'} missingFields={routePayload.missing_fields || []} message={routePayload.message || 'Derived COT statistics are incomplete for this instrument.'} onRetry={() => setRetryNonce((n) => n + 1)} /> : <CotWorkstationErrorBoundary key={marketId} marketId={marketId}><CotWorkstation marketId={marketId} variant="fullscreen" /></CotWorkstationErrorBoundary>}
      </main>
    </div>
  )
}
