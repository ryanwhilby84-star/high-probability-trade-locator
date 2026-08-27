import React from 'react'
import { AppShell } from '../components/AppShell.jsx'
import {
  navigateToCotProof,
  navigateToCotSourceTruth,
  navigateToDataLineage,
  navigateToOandaCoverage,
  navigateToPriceCoverage,
  navigateToScanner,
} from '../routing.js'

const LINKS = [
  { label: 'Data Lineage', desc: 'COT and confluence pipeline lineage.', go: navigateToDataLineage },
  { label: 'COT Source Truth', desc: 'Official CFTC mapping verification.', go: navigateToCotSourceTruth },
  { label: 'COT Proof (HTPL)', desc: 'Legacy COT proof harness.', go: navigateToCotProof },
  { label: 'Price Coverage Audit', desc: 'Price store coverage and gaps.', go: navigateToPriceCoverage },
  { label: 'OANDA Coverage', desc: 'OANDA instrument coverage audit.', go: navigateToOandaCoverage },
]

export function DiagnosticsPage({ sidebarClass, onSidebarClass }) {
  return (
    <AppShell
      title="Diagnostics"
      subtitle="Developer and data-quality tools — not part of the trading workflow."
      sidebarClass={sidebarClass}
      onSidebarClass={onSidebarClass}
      topActions={
        <button type="button" className="ws-btn" onClick={navigateToScanner}>
          ← Scanner
        </button>
      }
    >
      <p className="ws-topbar-meta diag-intro">
        These pages validate exports, mappings, and coverage. Traders should use Scanner, Instrument detail, and Thesis
        Tracker for decisions.
      </p>
      <ul className="diag-link-list">
        {LINKS.map(({ label, desc, go }) => (
          <li key={label}>
            <button type="button" className="diag-link-card" onClick={go}>
              <span className="diag-link-title">{label}</span>
              <span className="diag-link-desc">{desc}</span>
            </button>
          </li>
        ))}
      </ul>
    </AppShell>
  )
}
