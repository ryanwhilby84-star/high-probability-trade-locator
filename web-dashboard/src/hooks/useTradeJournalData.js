import React from 'react'

export function useTradeJournalData() {
  const [doc, setDoc] = React.useState(null)
  const [loading, setLoading] = React.useState(true)
  const [error, setError] = React.useState(null)

  const reload = React.useCallback(() => {
    setLoading(true)
    fetch('/data/trade_journal_latest.json')
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((data) => {
        setDoc(data && typeof data === 'object' ? data : { entries: [] })
        setError(null)
      })
      .catch((e) => {
        setDoc({ entries: [], disclaimer: 'No journal export yet.' })
        setError(e?.message || 'Failed to load journal')
      })
      .finally(() => setLoading(false))
  }, [])

  React.useEffect(() => {
    reload()
  }, [reload])

  const entries = React.useMemo(() => {
    const list = Array.isArray(doc?.entries) ? doc.entries : []
    return [...list].sort((a, b) =>
      String(b.updated_at || b.created_at || '').localeCompare(String(a.updated_at || a.created_at || '')),
    )
  }, [doc])

  return { doc, entries, loading, error, reload }
}
