import React from 'react'

import {

  addNote as localAddNote,

  addThesisFromRow,

  loadOverlay,

  mergeWithOverlay,

  removeThesis as localRemoveThesis,

  setStatus as localSetStatus,

} from '../thesisTracker/thesisLocal.js'

import { hydrateThesesFromConfluence } from '../thesisTracker/confluenceOverlay.js'

import { sortByOpportunity } from '../thesisTracker/opportunityModel.js'



export function useThesisTrackerData() {

  const [doc, setDoc] = React.useState(null)

  const [confluenceDoc, setConfluenceDoc] = React.useState(null)

  const [loading, setLoading] = React.useState(true)

  const [error, setError] = React.useState(null)

  const [overlayTick, setOverlayTick] = React.useState(0)



  const reload = React.useCallback(() => {

    setLoading(true)

    fetch('/data/confluence_history_latest.json')

      .then((r) => (r.ok ? r.json() : { records: [] }))

      .catch(() => ({ records: [] }))

      .then((confluenceData) => {

        setConfluenceDoc(confluenceData && typeof confluenceData === 'object' ? confluenceData : { records: [] })

        return fetch('/data/thesis_tracker_latest.json').then((r) => {

          if (!r.ok) throw new Error(`thesis HTTP ${r.status}`)

          return r.json()

        })

      })

      .then((thesisData) => {

        setDoc(thesisData && typeof thesisData === 'object' ? thesisData : { theses: [] })

        setError(null)

      })

      .catch((e) => {

        setDoc({ theses: [], disclaimer: 'No thesis export yet — run python -m hptl.thesis_tracker.run_thesis_seed' })

        setConfluenceDoc((prev) => prev ?? { records: [] })

        setError(e?.message || 'Failed to load thesis tracker')

      })

      .finally(() => setLoading(false))

  }, [])



  React.useEffect(() => {

    reload()

  }, [reload])



  const seeded = React.useMemo(() => (Array.isArray(doc?.theses) ? doc.theses : []), [doc])



  const theses = React.useMemo(() => {

    void overlayTick

    const merged = mergeWithOverlay(seeded)

    return sortByOpportunity(hydrateThesesFromConfluence(merged, confluenceDoc))

  }, [seeded, confluenceDoc, overlayTick])



  const bump = React.useCallback(() => setOverlayTick((n) => n + 1), [])



  const actions = React.useMemo(

    () => ({

      track: (payload) => {

        const t = addThesisFromRow(payload)

        bump()

        return t

      },

      remove: (id) => {

        localRemoveThesis(id)

        bump()

      },

      setStatus: (id, status) => {

        localSetStatus(id, status)

        bump()

      },

      addNote: (id, text) => {

        localAddNote(id, text)

        bump()

      },

      isTracked: (market) => {

        const overlay = loadOverlay()

        if (overlay.added.some((t) => t.market === market)) return true

        return seeded.some((t) => t.market === market && !overlay.patches[t.thesis_id]?.removed)

      },

    }),

    [bump, seeded],

  )



  return { doc, confluenceDoc, theses, loading, error, reload, actions }

}

