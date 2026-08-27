import React from 'react'

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
    const url = `/api/seasonality-workstation/${encodeURIComponent(marketId)}?lookback=${encodeURIComponent(lookback)}`
    fetch(url, { cache: 'no-store' })
      .then(async (r) => {
        let body = null
        try {
          body = await r.json()
        } catch {
          body = null
        }
        if (cancelled) return
        if (!body) {
          setError('Invalid response from seasonality workstation API.')
          return
        }
        setPayload(body)
        if (body.status !== 'ok') {
          setError(body.message || body.error || 'Seasonality research failed.')
        }
      })
      .catch((err) => {
        if (!cancelled) setError(err?.message || 'Fetch failed')
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
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
