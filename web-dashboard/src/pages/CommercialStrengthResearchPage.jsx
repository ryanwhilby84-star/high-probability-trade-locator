import React from 'react'

import { AppShell } from '../components/AppShell.jsx'
import { CommercialStrengthResearchPanel } from '../components/CommercialStrengthResearchPanel.jsx'
import { navigateToScanner } from '../routing.js'

/** Dedicated route for commercial strength research table. */
export function CommercialStrengthResearchPage({ sidebarClass, onSidebarClass }) {
  return (
    <AppShell
      title="Commercial Strength Research"
      subtitle="Research only · No signals · Not used in scanner or relative strength rankings"
      sidebarClass={sidebarClass}
      onSidebarClass={onSidebarClass}
      topActions={
        <button type="button" className="ws-btn" onClick={navigateToScanner}>
          Scanner
        </button>
      }
    >
      <CommercialStrengthResearchPanel />
    </AppShell>
  )
}
