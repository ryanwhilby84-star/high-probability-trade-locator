import React from 'react'

const AUDIT_URL = '/data/oanda_coverage_audit.json'

export function useOandaCoverageData() {
  const [data, setData] = React.useState(null)
  const [loading, setLoading] = React.useState(true)
  const [error, setError] = React.useState(null)

  React.useEffect(() => {
    let cancelled = false
    setLoading(true)
    fetch(AUDIT_URL)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((doc) => {
        if (!cancelled) {
          setData(doc)
          setError(null)
        }
      })
      .catch((e) => {
        if (!cancelled) {
          setData(null)
          setError(e.message || String(e))
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [])

  return { data, loading, error }
}
