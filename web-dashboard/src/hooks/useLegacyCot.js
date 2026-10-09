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
  const [reloadToken, setReloadToken] = React.useState(0)
  const [error, setError] = React.useState(null)

  React.useEffect(() => {
    let cancelled = false
    setLoading(true)
    Promise.all([loadLegacyCotLatest(reloadToken > 0), loadLegacyCotAudit().catch(() => ({ instruments: {} }))])
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
  }, [reloadToken])

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

  return { retry: () => setReloadToken((v) => v + 1), instrumentData, instrumentAudit, scoringEligible, loading, error, latestStore }
}
