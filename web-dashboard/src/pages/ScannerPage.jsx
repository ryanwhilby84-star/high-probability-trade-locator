import React from 'react'
import { AppShell } from '../components/AppShell.jsx'
import { CommercialAttentionPanel } from '../components/CommercialAttentionPanel.jsx'
import { inspectorDates, loadWeeklyInspector } from '../data/weeklyInspectorStore.js'

// Header, historical selection and signals all belong to the same COT snapshot.
export function ScannerPage({ sidebarClass, onSidebarClass }) {
  const [radarEligibleOnly, setRadarEligibleOnly] = React.useState(true)
  const [state, setState] = React.useState({ inspector: null, loading: true, error: null })
  const [selectedDate, setSelectedDate] = React.useState(null)
  React.useEffect(() => {
    let cancelled = false
    loadWeeklyInspector().then(inspector => {
      if (!cancelled) setState({ inspector, loading: false, error: null })
    }).catch(error => {
      if (!cancelled) setState({ inspector: null, loading: false, error })
    })
    return () => { cancelled = true }
  }, [])
  const dates = React.useMemo(() => inspectorDates(state.inspector), [state.inspector])
  const latest = dates.at(-1)
  const date = dates.includes(selectedDate) ? selectedDate : latest
  return <AppShell
    title="Market Radar"
    subtitle="Institutional positioning · extreme context · NC rotation intelligence"
    date={date}
    dates={dates}
    onDateChange={setSelectedDate}
    latestCotReportDate={latest}
    sidebarClass={sidebarClass}
    onSidebarClass={onSidebarClass}
  >
    <label style={{ display: 'inline-flex', alignItems: 'center', gap: 8, marginBottom: 12, color: '#cbd6df', fontSize: 12 }}>
      <input type="checkbox" checked={radarEligibleOnly} onChange={e => setRadarEligibleOnly(e.target.checked)} />
      Primary markets only
    </label>
    <CommercialAttentionPanel radarEligibleOnly={radarEligibleOnly} inspectorState={state} selectedDate={date} latestDate={latest} />
  </AppShell>
}
