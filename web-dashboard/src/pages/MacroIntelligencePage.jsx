import { AppShell } from '../components/AppShell.jsx'
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
export function MacroIntelligencePage({ sidebarClass, onSidebarClass }) {
  return <AppShell title="Macro Intelligence" subtitle="Macro backdrop and cross-asset context" sidebarClass={sidebarClass} onSidebarClass={onSidebarClass}>
    <div className="mi-page"><MacroIntelligenceWorkstation /></div>
  </AppShell>
}
