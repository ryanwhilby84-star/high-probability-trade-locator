import React from 'react'
import { SeasonalWindowFinder } from '../seasonality/SeasonalWindowFinder.jsx'

import { SeasonalityWorkstation } from '../seasonality_workstation/SeasonalityWorkstation.jsx'
import {
  navigateToInstrument,
  navigateToScanner,
  navigateToSeasonalityWorkstation,
} from '../routing.js'
import { canonicalMarketId } from '../marketResolution.js'

import '../seasonality_workstation/seasonalityWorkstation.css'

/**
 * Seasonality Workstation page — navigates the full canonical tracked universe
 * (same order as HPTL registry / LEGACY_COT list), not a sidebar-filtered subset.
 */
export function SeasonalityWorkstationPage({
  marketId,
  trackedMarkets,
}) {
  const navMarkets = React.useMemo(() => {
    const ids = (trackedMarkets || []).map((m) => canonicalMarketId(m)).filter(Boolean)
    // de-dupe, preserve order
    const seen = new Set()
    return ids.filter((id) => {
      if (seen.has(id)) return false
      seen.add(id)
      return true
    })
  }, [trackedMarkets])

  const navIndex = navMarkets.indexOf(marketId)
  const prevMarket = navIndex > 0 ? navMarkets[navIndex - 1] : null
  const nextMarket =
    navIndex >= 0 && navIndex < navMarkets.length - 1 ? navMarkets[navIndex + 1] : null

  const [lookback, setLookback] = React.useState('15Y')
  const [payload, setPayload] = React.useState(null)
  const [loading, setLoading] = React.useState(true)
  const [error, setError] = React.useState(null)

  React.useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    setPayload(null)
    const load = async () => {
      try {
        // Static Vercel hosting has no live Python seasonality API. Load the
        // validated, regularly regenerated export directly instead.
        const response = await fetch('/data/seasonality_workstation_latest.json', { cache: 'no-store' })
        if (!response.ok) throw new Error(`Seasonality export HTTP ${response.status}`)
        const archive = await response.json()
        const market = archive?.markets?.[marketId]
        const selection = market?.lookbacks?.[lookback]
        if (market?.status !== 'ok' || market?.integrity?.status === 'FAIL' || !selection) throw new Error('No verified seasonality for selected market and lookback')
        if (!cancelled) setPayload({
          ...market,
          selected_lookback: lookback,
          sample_size: selection.sample_size ?? selection.sample_years ?? market.sample_size,
          seasonality: { ...market.seasonality, week_stats: selection.week_stats, index_paths: selection.index_paths, price_aligned: selection.price_aligned, historical_year_paths: selection.historical_year_paths, projection: selection.projection },
          archived_generated_at: archive.generated_at,
        })
      } catch (archiveError) {
        if (!cancelled) setError(`Seasonality data unavailable: ${archiveError.message}`)
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    load()
    return () => {
      cancelled = true
    }
  }, [marketId, lookback])

  return (
    <div className="sws-page">
      <header className="sws-topbar">
        <div className="sws-topbar-left">
          <button type="button" className="sws-btn" onClick={navigateToScanner}>
            Scanner
          </button>
          <button type="button" className="sws-btn" onClick={() => navigateToInstrument(marketId)}>
            ← Instrument
          </button>
          <div>
            <h1 className="sws-title">Seasonality Workstation</h1>
            <p className="sws-sub">
              {marketId}
              {navIndex >= 0 ? ` · ${navIndex + 1}/${navMarkets.length}` : ''}
            </p>
          </div>
          <label className="sws-instrument-select">
            <span className="sws-muted">Instrument</span>
            <select
              value={marketId}
              onChange={(e) => navigateToSeasonalityWorkstation(e.target.value)}
            >
              {navMarkets.map((id) => (
                <option key={id} value={id}>
                  {id}
                </option>
              ))}
            </select>
          </label>
        </div>
        <div className="sws-topbar-right">
          <button
            type="button"
            className="sws-btn"
            disabled={!prevMarket}
            onClick={() => prevMarket && navigateToSeasonalityWorkstation(prevMarket)}
          >
            ← Prev
          </button>
          <button
            type="button"
            className="sws-btn"
            disabled={!nextMarket}
            onClick={() => nextMarket && navigateToSeasonalityWorkstation(nextMarket)}
          >
            Next →
          </button>
        </div>
      </header>

      <SeasonalWindowFinder marketId={marketId} />
      {payload?.archived_generated_at ? <p role="status" style={{ margin: 16, padding: 12, color: "#cbd5e1" }}>Seasonality data updated {payload.archived_generated_at.slice(0, 10)} · historical seasonal tendencies, not trading signals.</p> : null}
      <SeasonalityWorkstation
        marketId={marketId}
        payload={payload}
        lookback={lookback}
        onLookback={setLookback}
        loading={loading}
        error={error}
      />
    </div>
  )
}
