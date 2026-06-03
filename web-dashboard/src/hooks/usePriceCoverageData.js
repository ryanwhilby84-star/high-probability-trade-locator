import React from 'react'

const AUDIT_URL = '/data/price_coverage_audit.json'

const EMPTY_COVERAGE = {
  oanda_supported: [],
  alpha_supported: [],
  supported_by_both: [],
  unsupported: [],
  instruments: [],
  summary: {},
}

/** Normalize audit JSON — safe defaults for all list fields. */
export function normalizeCoverageData(loaded) {
  if (!loaded || typeof loaded !== 'object') {
    return { ...EMPTY_COVERAGE }
  }

  let instruments = loaded.instruments
  if (!Array.isArray(instruments)) {
    if (instruments && typeof instruments === 'object') {
      instruments = Object.values(instruments)
    } else {
      instruments = []
    }
  }

  return {
    ...EMPTY_COVERAGE,
    ...loaded,
    oanda_supported: Array.isArray(loaded.oanda_supported) ? loaded.oanda_supported : [],
    alpha_supported: Array.isArray(loaded.alpha_supported) ? loaded.alpha_supported : [],
    supported_by_both: Array.isArray(loaded.supported_by_both) ? loaded.supported_by_both : [],
    unsupported: Array.isArray(loaded.unsupported) ? loaded.unsupported : [],
    instruments,
    summary: loaded.summary && typeof loaded.summary === 'object' ? loaded.summary : {},
  }
}

export function usePriceCoverageData() {
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
        const coverageData = normalizeCoverageData(doc)
        if (import.meta.env.DEV) {
          console.log('coverage data', coverageData)
        }
        if (!cancelled) {
          setData(coverageData)
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
