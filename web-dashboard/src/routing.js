import React from 'react'
import { canonicalMarketId } from './marketResolution.js'

export function parseRoute() {
  const hash = typeof window !== 'undefined' ? window.location.hash.replace(/^#/, '') : ''
  const path = hash.startsWith('/') ? hash : hash ? `/${hash}` : '/'
  const parts = path.split('/').filter(Boolean)
  if (parts[0] === 'instrument' && parts[1]) {
    const market = decodeURIComponent(parts[1])
    if (parts[2] === 'positioning' && parts[3]) {
      return { view: 'cot-positioning', market, group: parts[3] }
    }
    return { view: 'instrument', market }
  }
  if (parts[0] === 'journal') {
    return { view: 'journal', market: null }
  }
  if (parts[0] === 'thesis' || parts[0] === 'tracker') {
    return { view: 'thesis', market: null }
  }
  if (parts[0] === 'oanda') {
    return { view: 'oanda', market: null }
  }
  if (parts[0] === 'price-coverage' || parts[0] === 'price') {
    return { view: 'price-coverage', market: null }
  }
  if (parts[0] === 'cot-proof') {
    return { view: 'cot-proof', market: null }
  }
  if (parts[0] === 'cot-source-truth') {
    return { view: 'cot-source-truth', market: null }
  }
  if (parts[0] === 'data-lineage' || parts[0] === 'cot-lineage') {
    return { view: 'data-lineage', market: null }
  }
  return { view: 'scanner', market: null }
}

export function navigateToScanner() {
  window.location.hash = '#/scanner'
}

export function navigateToInstrument(market) {
  const id = canonicalMarketId(market)
  window.location.hash = `#/instrument/${encodeURIComponent(id)}`
}

export function navigateToCotPositioning(market, groupSlug = 'managed-money') {
  const id = canonicalMarketId(market)
  window.location.hash = `#/instrument/${encodeURIComponent(id)}/positioning/${groupSlug}`
}

export function navigateToJournal() {
  window.location.hash = '#/journal'
}

export function navigateToThesisTracker() {
  window.location.hash = '#/thesis'
}

export function navigateToOandaCoverage() {
  window.location.hash = '#/oanda'
}

export function navigateToPriceCoverage() {
  window.location.hash = '#/price-coverage'
}

export function navigateToCotProof() {
  window.location.hash = '#/cot-proof'
}

export function navigateToCotSourceTruth() {
  window.location.hash = '#/cot-source-truth'
}

export function navigateToDataLineage() {
  window.location.hash = '#/data-lineage'
}

export function useHashRoute() {
  const [route, setRoute] = React.useState(parseRoute)
  React.useEffect(() => {
    const onHash = () => setRoute(parseRoute())
    window.addEventListener('hashchange', onHash)
    return () => window.removeEventListener('hashchange', onHash)
  }, [])
  return route
}
