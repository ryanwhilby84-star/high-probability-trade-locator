import React from 'react'

import { filterMarketsBySidebar } from '../components/AppShell.jsx'
import { CotWorkstation } from '../workstation/CotWorkstation.jsx'
import {
  navigateToCotWorkstation,
  navigateToInstrument,
  navigateToScanner,
} from '../routing.js'

import '../workstation/cotWorkstationPage.css'

class CotWorkstationErrorBoundary extends React.Component {
  constructor(props) {
    super(props)
    this.state = { error: null }
  }

  static getDerivedStateFromError(error) {
    return { error }
  }

  componentDidCatch(error) {
    console.error('[cot-workstation] route failed', this.props.marketId, error)
  }

  render() {
    if (this.state.error) {
      return (
        <div className="cot-ws-status cot-ws-status--error">
          <p>
            COT workstation could not render for <strong>{this.props.marketId}</strong>.
          </p>
          <p className="cot-ws-status-detail">
            Diagnostic: {String(this.state.error?.message || this.state.error)}
          </p>
          <p className="cot-ws-status-detail">
            The app shell is still stable. Use Scanner or the instrument page while this workstation issue is
            investigated.
          </p>
        </div>
      )
    }

    return this.props.children
  }
}

export function CotWorkstationPage({ marketId, trackedMarkets, sidebarClass, onSidebarClass }) {
  const navMarkets = React.useMemo(
    () => filterMarketsBySidebar(trackedMarkets, sidebarClass),
    [trackedMarkets, sidebarClass],
  )

  const navIndex = navMarkets.indexOf(marketId)
  const prevMarket = navIndex > 0 ? navMarkets[navIndex - 1] : null
  const nextMarket = navIndex >= 0 && navIndex < navMarkets.length - 1 ? navMarkets[navIndex + 1] : null

  return (
    <div className="cot-ws-page">
      <header className="cot-ws-page-topbar">
        <div className="cot-ws-page-topbar-left">
          <button type="button" className="cot-ws-page-btn" onClick={navigateToScanner}>
            Scanner
          </button>
          <button type="button" className="cot-ws-page-btn" onClick={() => navigateToInstrument(marketId)}>
            ← {marketId}
          </button>
        </div>

        <div className="cot-ws-page-topbar-center">
          <span className="cot-ws-page-title">{marketId}</span>
          <span className="cot-ws-page-subtitle">COT Workstation</span>
        </div>

        <div className="cot-ws-page-topbar-right">
          <label className="cot-ws-page-market-select">
            <span className="sr-only">Market</span>
            <select
              className="cot-ws-page-select"
              value={marketId}
              onChange={(e) => navigateToCotWorkstation(e.target.value)}
            >
              {navMarkets.map((m) => (
                <option key={m} value={m}>
                  {m}
                </option>
              ))}
            </select>
          </label>
          <button
            type="button"
            className="cot-ws-page-btn"
            disabled={!prevMarket}
            onClick={() => prevMarket && navigateToCotWorkstation(prevMarket)}
          >
            Prev
          </button>
          <button
            type="button"
            className="cot-ws-page-btn"
            disabled={!nextMarket}
            onClick={() => nextMarket && navigateToCotWorkstation(nextMarket)}
          >
            Next
          </button>
        </div>
      </header>

      <main className="cot-ws-page-body">
        <CotWorkstationErrorBoundary key={marketId} marketId={marketId}>
          <CotWorkstation marketId={marketId} variant="fullscreen" />
        </CotWorkstationErrorBoundary>
      </main>
    </div>
  )
}
