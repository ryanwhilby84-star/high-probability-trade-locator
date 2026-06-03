import React from 'react'
import { useHashRoute } from './routing.js'
import { useConfluenceData } from './hooks/useConfluenceData.js'
import { ScannerPage } from './pages/ScannerPage.jsx'
import { InstrumentPage } from './pages/InstrumentPage.jsx'
import { CotPositioningPage } from './pages/CotPositioningPage.jsx'
import { TradeJournalPage } from './pages/TradeJournalPage.jsx'
import { ThesisTrackerPage } from './pages/ThesisTrackerPage.jsx'
import { OandaCoveragePage } from './pages/OandaCoveragePage.jsx'
import { PriceCoveragePage } from './pages/PriceCoveragePage.jsx'
import { CotProofPage } from './pages/CotProofPage.jsx'
import { CotSourceTruthPage } from './pages/CotSourceTruthPage.jsx'
import { DataLineagePage } from './pages/DataLineagePage.jsx'
import { canonicalMarketId } from './marketResolution.js'
import { allInstrumentIds } from './instrumentRegistry.js'
import { navigateToScanner } from './routing.js'

export default function App() {
  const route = useHashRoute()
  const confluence = useConfluenceData()
  const [sidebarClass, setSidebarClass] = React.useState('all')

  const allowedMarkets = React.useMemo(() => {
    const ids = allInstrumentIds()
    return ids.length ? ids : confluence.trackedMarkets
  }, [confluence.trackedMarkets])

  if (confluence.loading && !confluence.data.length) {
    return (
      <div className="ws-root ws-loading">
        Loading confluence data…
      </div>
    )
  }

  if (route.view === 'journal') {
    return (
      <TradeJournalPage
        sidebarClass={sidebarClass}
        onSidebarClass={setSidebarClass}
      />
    )
  }

  if (route.view === 'thesis') {
    return (
      <ThesisTrackerPage
        sidebarClass={sidebarClass}
        onSidebarClass={setSidebarClass}
      />
    )
  }

  if (route.view === 'oanda') {
    return (
      <OandaCoveragePage
        sidebarClass={sidebarClass}
        onSidebarClass={setSidebarClass}
      />
    )
  }

  if (route.view === 'price-coverage') {
    return (
      <PriceCoveragePage
        sidebarClass={sidebarClass}
        onSidebarClass={setSidebarClass}
      />
    )
  }

  if (route.view === 'cot-proof') {
    return <CotProofPage sidebarClass={sidebarClass} onSidebarClass={setSidebarClass} />
  }

  if (route.view === 'cot-source-truth') {
    return <CotSourceTruthPage sidebarClass={sidebarClass} onSidebarClass={setSidebarClass} />
  }

  if (route.view === 'data-lineage') {
    return <DataLineagePage sidebarClass={sidebarClass} onSidebarClass={setSidebarClass} />
  }

  if (route.view === 'cot-positioning' && route.market) {
    const marketId = canonicalMarketId(route.market)
    if (!allowedMarkets.includes(marketId)) {
      navigateToScanner()
      return null
    }
    return (
      <CotPositioningPage
        marketId={marketId}
        groupSlug={route.group || 'managed-money'}
        confluence={confluence}
        sidebarClass={sidebarClass}
        onSidebarClass={setSidebarClass}
      />
    )
  }

  if (route.view === 'instrument' && route.market) {
    const marketId = canonicalMarketId(route.market)
    if (!allowedMarkets.includes(marketId)) {
      navigateToScanner()
      return null
    }
    return (
      <InstrumentPage
        marketId={marketId}
        confluence={confluence}
        sidebarClass={sidebarClass}
        onSidebarClass={setSidebarClass}
      />
    )
  }

  return (
    <ScannerPage
      {...confluence}
      sidebarClass={sidebarClass}
      onSidebarClass={setSidebarClass}
    />
  )
}
