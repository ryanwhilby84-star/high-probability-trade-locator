import React from 'react'

/**
 * COT panels are ready once the COT export resolves for the instrument.
 * OHLC is optional and must not block Commercial / Non-Commercial / Non-Reportable charts.
 */
export function useCotWorkstationReady({
  marketId,
  cotLoading,
  cotDoc,
  cotBlock,
  modelAvailable,
  visibleRowCount,
  ohlcExportLoaded,
}) {
  const [readyMarket, setReadyMarket] = React.useState(null)

  const cotDataReady =
    Boolean(marketId) &&
    !cotLoading &&
    Boolean(cotDoc) &&
    Boolean(cotBlock) &&
    modelAvailable &&
    visibleRowCount > 0

  React.useEffect(() => {
    setReadyMarket((prev) => (prev === null ? prev : null))
  }, [marketId])

  React.useEffect(() => {
    if (!marketId || !cotDataReady) return
    setReadyMarket((prev) => (prev === marketId ? prev : marketId))
  }, [marketId, cotDataReady])

  const chartsReady = readyMarket === marketId && cotDataReady
  const cotSettled = !cotLoading && Boolean(cotDoc)
  const ohlcSettled = Boolean(ohlcExportLoaded)

  return { chartsReady, cotSettled, ohlcSettled, cotDataReady }
}
