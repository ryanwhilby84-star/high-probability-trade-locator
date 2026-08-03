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

    if (parts.length >= 3 && parts[parts.length - 1] === 'seasonality-workstation') {

      const market = decodeURIComponent(parts.slice(1, -1).join('/')).trim()

      return { view: 'seasonality-workstation', market }

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

  if (parts[0] === 'correlation-matrix' || parts[0] === 'correlation') {

    return { view: 'correlation-matrix', market: null }

  }

  if (parts[0] === 'trade-basket-verify' || parts[0] === 'trade-basket') {

    return { view: 'trade-basket', market: null }

  }

  if (parts[0] === 'macro-intelligence' || parts[0] === 'macro-intel') {

    return { view: 'macro-intelligence', market: null }

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
      key === 'dxy' ||
      key === 'dx' ||
      key === 'usd index' ||
      key === 'us dollar index' ||
      key === 'us dollar index / dx' ||
      key.includes('dollar index')
    ) {
      return { view: 'dxy-macro-bias', market: 'US Dollar Index / DX' }
    }
    if (key === 'gold' || key === 'xau' || key === 'xauusd' || key.includes('gold')) {
      return { view: 'gold-valuation', market: 'Gold' }
    }
    if (
      !rest ||
      key === 'natural-gas' ||
      key === 'ng' ||
      key === 'natural gas / ng' ||
      key === 'natural gas' ||
      key.includes('natural gas') ||
      (key.includes('workstation') && (key.includes('ng') || key.includes('natural')))
    ) {
      // Live tip card remains available; default Natural Gas valuation is the research workstation.
      if (
        key.includes('/live') ||
        key.endsWith('live') ||
        key === 'natural-gas/live' ||
        key === 'ng/live'
      ) {
        return { view: 'natural-gas-valuation', market: 'Natural Gas / NG' }
      }
      return { view: 'natural-gas-valuation-workstation', market: 'Natural Gas / NG' }
    }
  }

  if (String(parts[0] || '').toLowerCase() === 'dxy' || String(parts[0] || '').toLowerCase() === 'dixie') {
    return { view: 'dxy-macro-bias', market: 'US Dollar Index / DX' }
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



export function navigateToSeasonalityWorkstation(market) {

  const id = canonicalMarketId(market || 'Gold')

  window.location.hash = `#/instrument/${encodeURIComponent(id)}/seasonality-workstation`

}



export function navigateToCorrelationMatrix() {

  window.location.hash = '#/correlation-matrix'

}



export function navigateToTradeBasketVerify() {

  window.location.hash = '#/trade-basket'

}



export function navigateToTradeBasket() {

  window.location.hash = '#/trade-basket'

}



export function navigateToMacroIntelligence() {

  window.location.hash = '#/macro-intelligence'

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
  // Default Natural Gas valuation entry → historical research workstation.
  window.location.hash = `#/valuation/${encodeURIComponent('Natural Gas / NG')}`
}

export function navigateToNaturalGasValuationLive() {
  window.location.hash = `#/valuation/${encodeURIComponent('Natural Gas / NG')}/live`
}

export function navigateToNaturalGasValuationWorkstation() {
  window.location.hash = `#/valuation/${encodeURIComponent('Natural Gas / NG')}/workstation`
}



export function navigateToDxyMacroBias() {

  window.location.hash = `#/valuation/${encodeURIComponent('US Dollar Index / DX')}`

}



export function navigateToGoldValuation() {

  window.location.hash = `#/valuation/${encodeURIComponent('Gold')}`

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

