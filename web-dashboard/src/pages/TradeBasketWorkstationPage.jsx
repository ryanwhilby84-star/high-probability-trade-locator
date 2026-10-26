import { AppShell } from '../components/AppShell.jsx'
import React from 'react'

import { TradeBasketWorkstation } from '../trade_basket/TradeBasketWorkstation.jsx'
import {
  navigateToScanner,
  navigateToCorrelationMatrix,
} from '../routing.js'
import '../trade_basket/tradeBasketWorkstation.css'

/**
 * Production Trade Basket Workstation (Phase 2B–4).
 * Presentation only — mathematics via POST /api/trade-basket.
 */
export function TradeBasketWorkstationPage({ sidebarClass, onSidebarClass }) {
  return <AppShell title="Trade Basket" subtitle="Portfolio exposure and trade basket analysis" sidebarClass={sidebarClass} onSidebarClass={onSidebarClass}>
    <div className="tbw-page"><TradeBasketWorkstation /></div>
  </AppShell>
}
