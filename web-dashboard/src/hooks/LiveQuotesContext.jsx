import React from 'react'

import { useLiveQuotesInternal } from './useLiveQuotes.js'

const LiveQuotesContext = React.createContext(null)

/** One poll + refresh pipeline per instrument page. */
export function LiveQuotesProvider({ marketId, children }) {
  const value = useLiveQuotesInternal(marketId)
  return <LiveQuotesContext.Provider value={value}>{children}</LiveQuotesContext.Provider>
}

export function useLiveQuotes(marketId) {
  const ctx = React.useContext(LiveQuotesContext)
  if (!ctx) {
    throw new Error(`useLiveQuotes(${marketId || ''}) must be used inside LiveQuotesProvider`)
  }
  return ctx
}
