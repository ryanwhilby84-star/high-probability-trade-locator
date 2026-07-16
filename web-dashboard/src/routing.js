import React from 'react'

import { canonicalMarketId } from './marketResolution.js'



export function parseRoute() {

  const hash = typeof window !== 'undefined' ? window.location.hash.replace(/^#/, '') : ''

  const path = hash.startsWith('/') ? hash : hash ? `/${hash}` : '/'

  const parts = path.split('/').filter(Boolean)

  if (parts[0] === 'instrument') {

    if (parts.length >= 3 && parts[parts.length - 1] === 'cot-workstation') {

      const market = decodeURIComponent(parts.slice(1, -1).join('/')).trim()

      return { view: 'cot-workstation', market }

    }

    if (parts.length >= 2) {

      const market = decodeURIComponent(parts.slice(1).join('/')).trim()

      return { view: 'instrument', market }

    }

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

  if (parts[0] === 'diagnostics') {

    return { view: 'diagnostics', market: null }

  }

  if (parts[0] === 'macro-hub' || parts[0] === 'macro') {

    return { view: 'macro-hub', market: null }

  }

  // Natural Gas institutional valuation — accept several URL shapes.
  // Preferred: #/valuation/Natural%20Gas%20%2F%20NG
  // Also: #/valuation/natural-gas , #/Valuation/...
  if (String(parts[0] || '').toLowerCase() === 'valuation') {
    const rest = parts
      .slice(1)
      .map((p) => {
        try {
          return decodeURIComponent(p)
        } catch {
          return p
        }
      })
      .join('/')
      .trim()
    const key = rest.toLowerCase().replace(/\s+/g, ' ')
    if (
      !rest ||
      key === 'natural-gas' ||
      key === 'ng' ||
      key === 'natural gas / ng' ||
      key === 'natural gas' ||
      key.includes('natural gas')
    ) {
      return { view: 'natural-gas-valuation', market: 'Natural Gas / NG' }
    }
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



export function navigateToCotWorkstation(market) {

  const id = canonicalMarketId(market)

  window.location.hash = `#/instrument/${encodeURIComponent(id)}/cot-workstation`

}

/** Open instrument detail and scroll to valuation evidence workstation. */
export function navigateToInstrumentValuation(market) {
  try {
    sessionStorage.setItem('scrollToValuation', '1')
  } catch {
    /* ignore */
  }
  navigateToInstrument(market)
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



export function navigateToDiagnostics() {

  window.location.hash = '#/diagnostics'

}



export function navigateToMacroHub() {

  window.location.hash = '#/macro-hub'

}



export function navigateToNaturalGasValuation() {

  window.location.hash = `#/valuation/${encodeURIComponent('Natural Gas / NG')}`

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

