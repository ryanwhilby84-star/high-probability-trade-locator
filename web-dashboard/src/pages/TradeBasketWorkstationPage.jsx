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
export function TradeBasketWorkstationPage() {
  return (
    <div className="tbw-page">
      <header className="tbw-topbar">
        <div className="tbw-topbar-left">
          <button type="button" className="tbw-btn" onClick={navigateToScanner}>
            Scanner
          </button>
          <button type="button" className="tbw-btn" onClick={navigateToCorrelationMatrix}>
            Correlation Matrix
          </button>
          <div>
            <h1 className="tbw-title">Trade Basket Workstation</h1>
            <p className="tbw-sub">
              Phase 4 — FX pair trades, currency exposure, portfolio intelligence ·
              display only
            </p>
          </div>
        </div>
        <div />
      </header>
      <TradeBasketWorkstation />
    </div>
  )
}
