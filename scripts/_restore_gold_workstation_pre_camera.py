"""Restore GoldValuationWorkstationPage to pre-camera known-good from HEAD NG page."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "web-dashboard/src/pages/NaturalGasValuationWorkstationPage.jsx"
DST = ROOT / "web-dashboard/src/pages/GoldValuationWorkstationPage.jsx"
EXPORT = ROOT / "web-dashboard/src/pages/GoldValuationPage.jsx"


def must_replace(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        raise SystemExit(f"missing block: {label}")
    return text.replace(old, new)


def main() -> None:
    t = SRC.read_text(encoding="utf-8")

    t = must_replace(
        t,
        """import { useLivePrice } from '../prices/usePriceStores.js'
import {
  navigateToCotWorkstation,
  navigateToInstrument,
  navigateToNaturalGasValuationLive,
  navigateToScanner,
} from '../routing.js'
""",
        """import { useCanonicalCurrentPrice } from '../prices/canonicalCurrentPrice.js'
import {
  navigateToCotWorkstation,
  navigateToInstrument,
  navigateToScanner,
} from '../routing.js'
""",
        "imports-top",
    )

    t = must_replace(
        t,
        """import {
  INTERACTION_MODE,
  alignPointsToTimeline,
  assertSharedVisibleRange,
  buildLiveValuationState,
  extractPhysicalFairValueTip,
  formatClock,
  historicalSeriesFingerprint,
  resolveCurrentPriceSource,
  resolveInteractionMode,
} from './naturalGasValuationWorkstationLive.js'
import './naturalGasValuationWorkstation.css'
""",
        """import {
  INTERACTION_MODE,
  UPDATE_MODE,
  alignPointsToTimeline,
  assertSharedVisibleRange,
  formatClock,
  historicalSeriesFingerprint,
  resolveInteractionMode,
} from './naturalGasValuationWorkstationLive.js'
import {
  buildGoldWorkstationHistory,
  goldLiveState,
} from './goldValuationWorkstationModel.js'
import './naturalGasValuationWorkstation.css'
""",
        "imports-live",
    )

    t = must_replace(t, "const MARKET = 'Natural Gas / NG'", "const MARKET = 'Gold'", "market")
    t = must_replace(
        t,
        """const HISTORY_URL = '/data/ng_valuation_workstation_latest.json'
const VALUATION_URL = '/data/natural_gas_valuation_latest.json'

function fmt(v, digits = 3) {""",
        """const GOLD_URL = '/data/gold_valuation_latest.json'

function fmt(v, digits = 2) {""",
        "urls-fmt",
    )
    t = must_replace(
        t,
        "export function NaturalGasValuationWorkstationPage()",
        "export function GoldValuationWorkstationPage()",
        "export-fn",
    )
    t = must_replace(
        t,
        "export default NaturalGasValuationWorkstationPage",
        "export default GoldValuationWorkstationPage",
        "export-default",
    )

    t = must_replace(
        t,
        """  const [historyDoc, setHistoryDoc] = React.useState(null)
  const [valuationDoc, setValuationDoc] = React.useState(null)
  const [error, setError] = React.useState(null)
  const [lockedTime, setLockedTime] = React.useState(null)
  const [hoverTime, setHoverTime] = React.useState(null)
  const lockedTimeRef = React.useRef(null)
  lockedTimeRef.current = lockedTime
  const hoverRafRef = React.useRef(null)
  const pendingHoverRef = React.useRef(null)
  const rangeInitRef = React.useRef(false)

  const [rangeId, setRangeId] = React.useState(POSITIONING_DEFAULT_RANGE_ID)
  const [bottomSeries, setBottomSeries] = React.useState(BOTTOM_SERIES.deviation)
  const [modelMode, setModelMode] = React.useState('walkforward')
  const [scaleMode, setScaleMode] = React.useState(DEFAULT_SCALE_MODE)
  const [pricesLatestSnapshot, setPricesLatestSnapshot] = React.useState(null)
  const sharedVisibleRangeRef = React.useRef(null)
  const lockedTimeStableRef = React.useRef(null)

  const liveHook = useLivePrice(MARKET)

  React.useEffect(() => {
    let cancelled = false
    const loadCore = () =>
      Promise.all([
        fetchPublicJson(HISTORY_URL),
        fetchPublicJson(VALUATION_URL).catch(() => null),
        fetchPublicJson('/data/prices_latest.json').catch(() => null),
      ]).then(([hist, val, prices]) => {
        if (cancelled) return
        setHistoryDoc(hist)
        setValuationDoc(val)
        const snap = prices?.instruments?.[MARKET]?.price || null
        setPricesLatestSnapshot(snap)
        setError(null)
      })

    loadCore().catch((err) => {
      if (!cancelled) setError(err?.message || String(err))
    })

    // Reactive snapshot path when WebSocket is down — never requires weekly rebuild.
    const pollId = window.setInterval(() => {
      Promise.all([
        fetchPublicJson(VALUATION_URL).catch(() => null),
        fetchPublicJson('/data/prices_latest.json').catch(() => null),
      ]).then(([val, prices]) => {
        if (cancelled) return
        if (val) setValuationDoc(val)
        const snap = prices?.instruments?.[MARKET]?.price || null
        if (snap) setPricesLatestSnapshot(snap)
      })
    }, 15_000)

    return () => {
      cancelled = true
      window.clearInterval(pollId)
      if (hoverRafRef.current != null) cancelAnimationFrame(hoverRafRef.current)
    }
  }, [])

  const weeks = historyDoc?.weeks || []
""",
        """  const [historyDoc, setHistoryDoc] = React.useState(null)
  const [error, setError] = React.useState(null)
  const [lockedTime, setLockedTime] = React.useState(null)
  const [hoverTime, setHoverTime] = React.useState(null)
  const lockedTimeRef = React.useRef(null)
  lockedTimeRef.current = lockedTime
  const hoverRafRef = React.useRef(null)
  const pendingHoverRef = React.useRef(null)
  const rangeInitRef = React.useRef(false)
  const canonical = useCanonicalCurrentPrice(MARKET)

  const [rangeId, setRangeId] = React.useState(POSITIONING_DEFAULT_RANGE_ID)
  const [bottomSeries, setBottomSeries] = React.useState(BOTTOM_SERIES.deviation)
  const [modelMode, setModelMode] = React.useState('walkforward')
  const [scaleMode, setScaleMode] = React.useState(DEFAULT_SCALE_MODE)
  const sharedVisibleRangeRef = React.useRef(null)
  const lockedTimeStableRef = React.useRef(null)

  React.useEffect(() => {
    let cancelled = false
    fetchPublicJson(GOLD_URL)
      .then((goldDoc) => {
        if (cancelled) return
        setHistoryDoc(buildGoldWorkstationHistory(goldDoc))
        setError(null)
      })
      .catch((err) => {
        if (!cancelled) setError(err?.message || String(err))
      })

    return () => {
      cancelled = true
      if (hoverRafRef.current != null) cancelAnimationFrame(hoverRafRef.current)
    }
  }, [])

  const weeks = historyDoc?.weeks || []
  const tip = historyDoc?.tip || {}
""",
        "state-load",
    )

    t = must_replace(
        t,
        """  const physicalTip = React.useMemo(
    () => extractPhysicalFairValueTip(valuationDoc, weeks, modelMode),
    [valuationDoc, weeks, modelMode],
  )

  const priceSource = React.useMemo(
    () =>
      resolveCurrentPriceSource({
        connected: Boolean(liveHook?.connected),
        streamPrice: liveHook?.streamPrice || null,
        quote: liveHook?.quote || null,
        status: liveHook?.status || null,
        freshness: liveHook?.freshness || null,
        valuationPriceFreshness: physicalTip.price_freshness,
        pricesLatestSnapshot,
      }),
    [
      liveHook?.connected,
      liveHook?.streamPrice,
      liveHook?.quote,
      liveHook?.status,
      liveHook?.freshness,
      physicalTip.price_freshness,
      pricesLatestSnapshot,
    ],
  )

  const liveState = React.useMemo(
    () =>
      buildLiveValuationState({
        physicalTip,
        priceSource,
        historicalSeriesFingerprint: fingerprint,
        researchVerdict: historyDoc?.verdict?.verdict || physicalTip.model_verdict,
      }),
    [physicalTip, priceSource, fingerprint, historyDoc?.verdict?.verdict],
  )

  const interactionMode = resolveInteractionMode({ lockedTime, hoverTime })
  // Preserve lock across live quote ticks (price updates must not clear selection).
  lockedTimeStableRef.current = lockedTime
""",
        """  const interactionMode = resolveInteractionMode({ lockedTime, hoverTime })
  lockedTimeStableRef.current = lockedTime

  const liveState = React.useMemo(() => {
    const base = goldLiveState({
      marketPrice: canonical.price,
      fairValue: tip.fair_value,
      priceStatus: canonical.status,
      priceLabel: canonical.label,
      priceSource: canonical.providerSymbol
        ? `${canonical.label || canonical.status} · ${canonical.provider || 'OANDA'} · ${canonical.providerSymbol}`
        : canonical.label || 'OANDA / canonical',
      asOf: canonical.asOf,
      tip,
    })
    return {
      ...base,
      price_label: canonical.label || base.update_mode || 'STALE',
      model_verdict: historyDoc?.verdict?.verdict || base.model_verdict,
      historical_series_fingerprint: fingerprint,
      fair_value_stable: true,
      last_state_update: canonical.asOf || null,
    }
  }, [canonical, tip, historyDoc?.verdict?.verdict, fingerprint])

  const updateMode = liveState.update_mode || UPDATE_MODE.STALE
""",
        "live-state",
    )

    # Sync probe rename
    t = t.replace("window.__NGVW_SYNC__", "window.__GOLDVW_SYNC__")
    t = t.replace("[NGVW]", "[GOLDVW]")

    # Topbar / labels / testids
    t = must_replace(
        t,
        """          <button type="button" className="cot-ws-page-btn" onClick={navigateToNaturalGasValuationLive}>
            Live tip card
          </button>
""",
        "",
        "live-tip-btn",
    )

    simple = [
        ('data-testid="ngvw-page"', 'data-testid="goldvw-page"'),
        ('data-testid="ngvw-price-badge"', 'data-testid="goldvw-price-badge"'),
        ('data-testid="ngvw-return-live"', 'data-testid="goldvw-return-live"'),
        ("Weekly Natural Gas Price", "Weekly Gold Price"),
        ('key="ngvw-price-pane"', 'key="goldvw-price-pane"'),
        ('data-testid="ngvw-live-card"', 'data-testid="goldvw-live-card"'),
        ("CURRENT NATURAL GAS VALUATION", "CURRENT GOLD VALUATION"),
        ("Physical fair value", "Market-clearing fair value"),
        ('data-testid="ngvw-market-price"', 'data-testid="goldvw-market-price"'),
        ('data-testid="ngvw-fair-value"', 'data-testid="goldvw-fair-value"'),
        ('data-testid="ngvw-live-deviation"', 'data-testid="goldvw-live-deviation"'),
        ('data-testid="ngvw-state-headline"', 'data-testid="goldvw-state-headline"'),
        ('data-testid="ngvw-price-status"', 'data-testid="goldvw-price-status"'),
        ('data-testid="ngvw-comparison-status"', 'data-testid="goldvw-comparison-status"'),
        ('data-testid="ngvw-model-verdict"', 'data-testid="goldvw-model-verdict"'),
        ('data-testid="ngvw-live-diag"', 'data-testid="goldvw-live-diag"'),
        ('key="ngvw-valuation-pane"', 'key="goldvw-valuation-pane"'),
        ("                  <dt>Storage as of</dt>\n", "                  <dt>Valid FV quarter</dt>\n"),
        ("                  <dt>Production as of</dt>\n", "                  <dt>Latest model quarter</dt>\n"),
    ]
    for a, b in simple:
        t = must_replace(t, a, b, a[:40])

    t = must_replace(
        t,
        "                  <dd>{liveState.storage_as_of || '—'}</dd>\n",
        "                  <dd>{liveState.storage_as_of || tip.latest_valid_quarter || '—'}</dd>\n",
        "storage-dd",
    )
    t = must_replace(
        t,
        "                  <dd>{liveState.production_as_of || '—'}</dd>\n",
        """                  <dd>
                    {tip.market_quarter || '—'}
                    {tip.solver_status ? ` · ${tip.solver_status}` : ''}
                  </dd>
""",
        "production-dd",
    )

    t = must_replace(
        t,
        """                  <article className="ngvw-card">
                    <h3>Why the model said this</h3>
                    <dl>
                      <div>
                        <dt>Storage surplus/deficit</dt>
                        <dd>{fmt(inspector.storage_surplus_bcf, 1)} Bcf</dd>
                      </div>
                      <div>
                        <dt>Storage contribution (log)</dt>
                        <dd>{fmtSigned(inspector.storage_log_contribution, 4)}</dd>
                      </div>
                      <div>
                        <dt>Production YoY</dt>
                        <dd>{fmtSigned(inspector.production_yoy_pct)}%</dd>
                      </div>
                      <div>
                        <dt>Production contribution (log)</dt>
                        <dd>{fmtSigned(inspector.production_log_contribution, 4)}</dd>
                      </div>
                      <div>
                        <dt>Intercept / baseline</dt>
                        <dd>{fmt(inspector.intercept, 4)}</dd>
                      </div>
                      <div>
                        <dt>Contribution reconciliation (log P)</dt>
                        <dd>{fmt(inspector.log_price_recon, 4)}</dd>
                      </div>
                    </dl>
                  </article>""",
        """                  <article className="ngvw-card">
                    <h3>Why the model said this</h3>
                    <dl>
                      <div>
                        <dt>Fair-value quarter</dt>
                        <dd>{historyHit?.week?.fair_value_quarter || tip.latest_valid_quarter || '—'}</dd>
                      </div>
                      <div>
                        <dt>Publication date</dt>
                        <dd>
                          {historyHit?.week?.fair_value_publication_date ||
                            tip.latest_valid_publication_date ||
                            '—'}
                        </dd>
                      </div>
                      <div>
                        <dt>Solver status</dt>
                        <dd>{historyHit?.week?.solver_status || tip.solver_status || '—'}</dd>
                      </div>
                      <div>
                        <dt>Carried forward</dt>
                        <dd>{historyHit?.week?.is_carried_forward ? 'Yes' : 'Observation week'}</dd>
                      </div>
                      <div>
                        <dt>Total demand</dt>
                        <dd>
                          {tip.total_demand == null ? '—' : `${fmt(tip.total_demand, 1)} t`}
                        </dd>
                      </div>
                      <div>
                        <dt>Net imbalance</dt>
                        <dd>
                          {tip.net_imbalance_tonnes == null
                            ? '—'
                            : `${fmtSigned(tip.net_imbalance_tonnes, 1)} t`}
                        </dd>
                      </div>
                    </dl>
                  </article>""",
        "why-card",
    )

    leftovers = [
        line
        for line in t.splitlines()
        if any(
            tok in line
            for tok in (
                "navigateToNaturalGas",
                "extractPhysical",
                "buildLiveValuationState",
                "resolveCurrentPriceSource",
                "useLivePrice",
                "valuationDoc",
                "HISTORY_URL",
                "VALUATION_URL",
                "pricesLatestSnapshot",
                "liveHook",
                "NaturalGasValuation",
            )
        )
    ]
    if leftovers:
        print("LEFTOVERS:")
        for line in leftovers[:40]:
            print(" ", line.strip())
        raise SystemExit(f"{len(leftovers)} leftover NG refs")

    DST.write_text(t, encoding="utf-8")
    EXPORT.write_text(
        """/** Gold Valuation route → Natural Gas Valuation Workstation template (pre-camera). */
export {
  GoldValuationWorkstationPage as GoldValuationPage,
  GoldValuationWorkstationPage as default,
} from './GoldValuationWorkstationPage.jsx'
""",
        encoding="utf-8",
    )
    print("wrote", DST.relative_to(ROOT))
    print("wrote", EXPORT.relative_to(ROOT))


if __name__ == "__main__":
    main()
