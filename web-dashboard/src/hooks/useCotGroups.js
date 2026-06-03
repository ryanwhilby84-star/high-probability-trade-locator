import React from 'react'
import { getCotAuditForInstrument, getCotGroupsForInstrument, loadCotGroupAudit, loadCotGroupsStore } from '../cotGroupsData.js'

export function useCotGroups(instrumentId) {
  const [groupsStore, setGroupsStore] = React.useState(null)
  const [auditStore, setAuditStore] = React.useState(null)
  const [loading, setLoading] = React.useState(true)
  const [error, setError] = React.useState(null)

  React.useEffect(() => {
    let cancelled = false
    setLoading(true)
    Promise.all([loadCotGroupsStore(), loadCotGroupAudit()])
      .then(([groups, audit]) => {
        if (!cancelled) {
          setGroupsStore(groups)
          setAuditStore(audit)
          setError(null)
        }
      })
      .catch((e) => {
        if (!cancelled) {
          setGroupsStore({ instruments: {} })
          setAuditStore({ instruments: {} })
          setError(e?.message || 'Failed to load COT groups')
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [])

  const instrumentGroups = React.useMemo(
    () => (groupsStore && instrumentId ? getCotGroupsForInstrument(groupsStore, instrumentId) : null),
    [groupsStore, instrumentId],
  )

  const instrumentAudit = React.useMemo(
    () => (auditStore && instrumentId ? getCotAuditForInstrument(auditStore, instrumentId) : null),
    [auditStore, instrumentId],
  )

  return { instrumentGroups, instrumentAudit, loading, error, groupsStore }
}
