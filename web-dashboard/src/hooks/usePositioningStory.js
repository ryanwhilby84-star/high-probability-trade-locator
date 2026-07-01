import React from 'react'

export const FX_STORY_CURRENCIES = ['EUR', 'GBP', 'JPY', 'CHF', 'AUD', 'CAD', 'NZD', 'USD']

export const POSITIONING_STORY_DATA_URL = '/data/fx_positioning_story_latest.json'

async function fetchStoryJson(url) {
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

export function buildPositioningStoryRows(storyDoc) {
  if (!storyDoc?.currencies) return []
  return FX_STORY_CURRENCIES.map((currency) => {
    const row = storyDoc.currencies[currency] || {}
    const score = row.story_score ?? null
    return {
      currency,
      story_state: row.story_state || 'Mixed',
      story_score: score,
      commercial_current_score: row.commercial_current_score ?? null,
      commercial_change_13w: row.commercial_change_13w ?? null,
      noncommercial_current_score: row.noncommercial_current_score ?? null,
      noncommercial_change_13w: row.noncommercial_change_13w ?? null,
      explanation: row.explanation || '',
      available: row.available !== false,
      abs_story_score: Number.isFinite(Number(score)) ? Math.abs(Number(score)) : -1,
    }
  }).sort((a, b) => b.abs_story_score - a.abs_story_score)
}

export function usePositioningStory() {
  const [storyDoc, setStoryDoc] = React.useState(null)
  const [loading, setLoading] = React.useState(true)
  const [missingPaths, setMissingPaths] = React.useState([])
  const [errors, setErrors] = React.useState([])

  React.useEffect(() => {
    let active = true
    setLoading(true)
    setMissingPaths([])
    setErrors([])

    fetchStoryJson(POSITIONING_STORY_DATA_URL)
      .then((result) => {
        if (!active) return
        setStoryDoc(result.data)
        if (!result.data) {
          setMissingPaths([result.path])
          setErrors([result.error].filter(Boolean))
        }
      })
      .finally(() => {
        if (active) setLoading(false)
      })

    return () => {
      active = false
    }
  }, [])

  const rows = React.useMemo(() => buildPositioningStoryRows(storyDoc), [storyDoc])

  const ready = !loading && missingPaths.length === 0 && rows.length > 0

  return {
    storyDoc,
    rows,
    loading,
    missingPaths,
    errors,
    ready,
    calendarWeek: storyDoc?.calendar_week || '—',
  }
}
