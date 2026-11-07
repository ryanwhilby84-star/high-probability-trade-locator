import React from 'react'
import { AppShell } from '../components/AppShell.jsx'
import { CommercialAttentionPanel } from '../components/CommercialAttentionPanel.jsx'

// Market Radar is the single attention surface. Legacy priority tables,
// duplicate watchlists and outdated homepage tiles are deliberately retired.
export function ScannerPage({
  date, dates, setDate, latestCotReportDate, cotFeedStatus,
  loading, error, sidebarClass, onSidebarClass,
}) {
  const [radarEligibleOnly, setRadarEligibleOnly] = React.useState(true)
  return <AppShell
    title="Market Radar"
    subtitle="Institutional positioning · extreme context · NC rotation intelligence"
    date={date}
    dates={dates}
    onDateChange={setDate}
    latestCotReportDate={latestCotReportDate}
    sidebarClass={sidebarClass}
    onSidebarClass={onSidebarClass}
  >
    {loading ? <p className="ws-topbar-meta">Loading market data…</p> : null}
    {error ? <p className="ws-error-banner">Data error: {error}</p> : null}
    {cotFeedStatus?.is_stale ? <p className="ws-error-banner" role="status">
      COT export is stale: {cotFeedStatus.latest_export_cot_week || latestCotReportDate || 'unknown'}.
      Rotation alerts only reflect the latest published report.
    </p> : null}
    <label style={{ display: 'inline-flex', alignItems: 'center', gap: 8, marginBottom: 12, color: '#cbd6df', fontSize: 12 }}>
      <input type="checkbox" checked={radarEligibleOnly} onChange={e => setRadarEligibleOnly(e.target.checked)} />
      Primary markets only
    </label>
    <CommercialAttentionPanel radarEligibleOnly={radarEligibleOnly} />
  </AppShell>
}
