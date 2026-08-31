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
