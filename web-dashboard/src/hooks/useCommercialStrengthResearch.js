import React from 'react'

/** G10 COT currencies in the commercial strength research layer. */
export const G10_RESEARCH_CURRENCIES = ['EUR', 'GBP', 'JPY', 'CHF', 'AUD', 'CAD', 'NZD']

export const COMMERCIAL_STRENGTH_DATA_URLS = {
  commercial: '/data/commercial_strength_latest.json',
  divergence: '/data/commercial_spec_divergence_latest.json',
  relativeStrength: '/data/relative_strength_latest.json',
}

async function fetchResearchJson(url) {
  try {
    const response = await fetch(url)
    if (!response.ok) {
      return {
        data: null,
        error: `HTTP ${response.status} — ${url}`,
        path: url,
      }
    }
    const data = await response.json()
    return { data, error: null, path: url }
  } catch (err) {
    return {
      data: null,
      error: `${err?.message || 'Fetch failed'} — ${url}`,
      path: url,
    }
  }
}

export function buildCommercialResearchRows(commercialDoc, divergenceDoc) {
  return G10_RESEARCH_CURRENCIES.map((currency) => {
    const div = divergenceDoc?.currencies?.[currency] || {}
    const comm = commercialDoc?.currencies?.[currency] || {}
    const divergence = div.divergence ?? null
    const spec = div.spec_score ?? null
    const commercial = div.commercial_score ?? comm.commercial_score ?? null
    return {
      currency,
      spec_strength: spec,
      commercial_strength: commercial,
      divergence,
      abs_divergence: Number.isFinite(Number(divergence)) ? Math.abs(Number(divergence)) : -1,
    }
  }).sort((a, b) => b.abs_divergence - a.abs_divergence)
}

export function useCommercialStrengthResearch() {
  const [commercialDoc, setCommercialDoc] = React.useState(null)
  const [divergenceDoc, setDivergenceDoc] = React.useState(null)
  const [relativeStrengthDoc, setRelativeStrengthDoc] = React.useState(null)
  const [loading, setLoading] = React.useState(true)
  const [missingPaths, setMissingPaths] = React.useState([])
  const [errors, setErrors] = React.useState([])

  React.useEffect(() => {
    let active = true
    setLoading(true)
    setMissingPaths([])
    setErrors([])

    Promise.all([
      fetchResearchJson(COMMERCIAL_STRENGTH_DATA_URLS.commercial),
      fetchResearchJson(COMMERCIAL_STRENGTH_DATA_URLS.divergence),
      fetchResearchJson(COMMERCIAL_STRENGTH_DATA_URLS.relativeStrength),
    ])
      .then(([commercial, divergence, relativeStrength]) => {
        if (!active) return

        const failed = [commercial, divergence, relativeStrength].filter((r) => !r.data)
        setCommercialDoc(commercial.data)
        setDivergenceDoc(divergence.data)
        setRelativeStrengthDoc(relativeStrength.data)
        setMissingPaths(failed.map((r) => r.path))
        setErrors(failed.map((r) => r.error).filter(Boolean))
      })
      .finally(() => {
        if (active) setLoading(false)
      })

    return () => {
      active = false
    }
  }, [])

  const rows = React.useMemo(() => {
    if (!commercialDoc || !divergenceDoc) return []
    return buildCommercialResearchRows(commercialDoc, divergenceDoc)
  }, [commercialDoc, divergenceDoc])

  const ready = !loading && missingPaths.length === 0 && rows.length > 0

  const calendarWeek =
    commercialDoc?.calendar_week ||
    divergenceDoc?.calendar_week ||
    relativeStrengthDoc?.calendar_week ||
    '—'

  return {
    commercialDoc,
    divergenceDoc,
    relativeStrengthDoc,
    rows,
    loading,
    missingPaths,
    errors,
    ready,
    calendarWeek,
  }
}
