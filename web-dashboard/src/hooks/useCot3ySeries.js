import React from 'react'
import { resolveMarketBlock } from '../charts/marketBlockResolve.js'
import {
  getCot3ySnapshot,
  prefetchCot3ySeries,
  subscribeCot3ySeries,
  invalidateCot3ySeriesCache,
} from '../data/cot3ySeriesStore.js'

export { invalidateCot3ySeriesCache }

export function useCot3ySeries() {
  const [snap, setSnap] = React.useState(() => getCot3ySnapshot())

  React.useEffect(() => {
    prefetchCot3ySeries()
    return subscribeCot3ySeries(() => setSnap(getCot3ySnapshot()))
  }, [])

  return snap
}

export function resolveCot3yBlock(doc, market) {
  return resolveMarketBlock(doc, market)
}
