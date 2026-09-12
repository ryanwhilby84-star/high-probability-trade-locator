import React from 'react'

import { SeasonalityWorkstation } from '../seasonality_workstation/SeasonalityWorkstation.jsx'
import { CrossMarketSeasonalityPanel } from '../seasonality_workstation/CrossMarketSeasonalityPanel.jsx'
import { SeasonalEdgePanel } from '../components/SeasonalEdgePanel.jsx'
import { AnnualTurningPointPanel } from '../components/AnnualTurningPointPanel.jsx'
import {
  DXY_COMPONENTS,
  DXY_MARKET_ID,
} from '../seasonality_workstation/crossMarketSeasonality.js'
import {
  navigateToInstrument,
  navigateToScanner,
  navigateToSeasonalityWorkstation,
} from '../routing.js'
import { canonicalMarketId, TRACKED_MARKET_IDS } from '../marketResolution.js'

import '../seasonality_workstation/seasonalityWorkstation.css'
import '../seasonality_workstation/crossMarketSeasonality.css'

const DASHBOARD_MARKETS = new Set(TRACKED_MARKET_IDS)
const BOND_MARKET_RE = /\b(bond|treasury|t-note|t-bond|bund|gilt)\b/i

/**
 * Seasonality Workstation navigation is intentionally kept compact: the normal
 * dashboard/COT universe plus any bond/rates market present in the registry.
 * The larger instrument registry contains many aliases and auxiliary markets
 * that are useful elsewhere but only add noise to this dropdown.
 */
export function SeasonalityWorkstationPage({
  marketId,
  trackedMarkets,
}) {
  const navMarkets = React.useMemo(() => {
    const ids = (trackedMarkets || [])
      .map((m) => canonicalMarketId(m))
      .filter(Boolean)
      .filter((id) => DASHBOARD_MARKETS.has(id) || BOND_MARKET_RE.test(id))

    // de-dupe, preserve registry/dashboard order
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
  const dollarComplexIds = React.useMemo(
    () => new Set([DXY_MARKET_ID, ...DXY_COMPONENTS.map((row) => row.id)]),
    [],
  )
  const showCrossMarketConfirmation = dollarComplexIds.has(marketId)

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

      <div style={{ padding: '0.75rem 0.75rem 0' }}>
        <SeasonalEdgePanel instrumentId={marketId} />
      </div>

      <AnnualTurningPointPanel instrumentId={marketId} />

      <SeasonalityWorkstation
        marketId={marketId}
        payload={payload}
        lookback={lookback}
        onLookback={setLookback}
        loading={loading}
        error={error}
      />

      {showCrossMarketConfirmation ? (
        <div className="sws-cross-shell">
          <CrossMarketSeasonalityPanel activeLookback={lookback} />
        </div>
      ) : null}
    </div>
  )
}
