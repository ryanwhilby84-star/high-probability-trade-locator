import React from 'react'
import {
  getLegacyAuditForInstrument,
  getLegacyCotForInstrument,
  isLegacyScoringEligible,
  loadLegacyCotAudit,
  loadLegacyCotLatest,
} from '../legacyCotData.js'

export function useLegacyCot(instrumentId) {
  const [latestStore, setLatestStore] = React.useState(null)
  const [auditStore, setAuditStore] = React.useState(null)
  const [loading, setLoading] = React.useState(true)
  const [error, setError] = React.useState(null)

  React.useEffect(() => {
    let cancelled = false
    setLoading(true)
    Promise.all([loadLegacyCotLatest(), loadLegacyCotAudit()])
      .then(([latest, audit]) => {
        if (!cancelled) {
          setLatestStore(latest)
          setAuditStore(audit)
          setError(null)
        }
      })
      .catch((e) => {
        if (!cancelled) {
          setLatestStore({ instruments: {} })
          setAuditStore({ instruments: {} })
          setError(e?.message || 'Failed to load Legacy COT')
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [])

  const instrumentData = React.useMemo(
    () => (latestStore && instrumentId ? getLegacyCotForInstrument(latestStore, instrumentId) : null),
    [latestStore, instrumentId],
  )

  const instrumentAudit = React.useMemo(
    () => (auditStore && instrumentId ? getLegacyAuditForInstrument(auditStore, instrumentId) : null),
    [auditStore, instrumentId],
  )

  const scoringEligible = React.useMemo(
    () => (latestStore && instrumentId ? isLegacyScoringEligible(latestStore, instrumentId) : false),
    [latestStore, instrumentId],
  )

  return { instrumentData, instrumentAudit, scoringEligible, loading, error, latestStore }
}
