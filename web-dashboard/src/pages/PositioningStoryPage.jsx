import React from 'react'

import { AppShell } from '../components/AppShell.jsx'
import { PositioningStoryPanel } from '../components/PositioningStoryPanel.jsx'
import { navigateToScanner } from '../routing.js'

export function PositioningStoryPage({ sidebarClass, onSidebarClass }) {
  return (
    <AppShell
      title="Positioning Story"
      subtitle="Attention layer · No trade signals · Not used in scanner or relative strength"
      sidebarClass={sidebarClass}
      onSidebarClass={onSidebarClass}
      topActions={
        <button type="button" className="ws-btn" onClick={navigateToScanner}>
          Scanner
        </button>
      }
    >
      <PositioningStoryPanel />
    </AppShell>
  )
}
