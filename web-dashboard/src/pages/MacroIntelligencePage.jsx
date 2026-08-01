import React from 'react'

import { MacroIntelligenceWorkstation } from '../macro_intelligence/MacroIntelligenceWorkstation.jsx'
import {
  navigateToScanner,
  navigateToCorrelationMatrix,
  navigateToTradeBasket,
} from '../routing.js'
import '../macro_intelligence/macroIntelligenceWorkstation.css'

/**
 * Macro Intelligence page — Phase 5 architecture.
 */
export function MacroIntelligencePage() {
  return (
    <div className="mi-page">
      <header className="mi-topbar">
        <div className="mi-topbar-left">
          <button type="button" className="mi-btn" onClick={navigateToScanner}>
            Scanner
          </button>
          <button type="button" className="mi-btn" onClick={navigateToCorrelationMatrix}>
            Correlation Matrix
          </button>
          <button type="button" className="mi-btn" onClick={navigateToTradeBasket}>
            Trade Basket
          </button>
          <div>
            <h1 className="mi-title">Macro Intelligence</h1>
            <p className="mi-sub">
              Phase 5 — deterministic macro backdrop framework · architecture only
            </p>
          </div>
        </div>
        <div />
      </header>
      <MacroIntelligenceWorkstation />
    </div>
  )
}
