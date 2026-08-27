"""Probe dense daily sources for Coffee seasonality foundation."""
from __future__ import annotations

from datetime import date, timedelta

from hptl.config import get_fmp_api_key
from hptl.data_sources.fmp_client import FmpClient, FmpApiError
from hptl.prices.canonical_timeline import resample_weekly_closes
from hptl.prices.fx_oanda_backfill_feasibility_audit import OANDA_MAX_COUNT, _iso_from, _probe_candles
from hptl.seasonality.seasonality_price_bars import history_quality
from hptl.seasonality.seasonality_v2 import normalize_daily_bars, parse_fmp_historical_payload, years_spanned

OANDA_CANDIDATES = [
    "COFFEE_USD",
    "COFFEEUSD",
    "KC_USD",
    "KCUSD",
    "ARABICA_USD",
]

FMP_CANDIDATES = [
    "KCUSD",
    "KC=F",
    "KCUSX",
    "JO",
    "CAFE",
    "KC1!",
]


def _report(label: str, daily: list[dict]) -> None:
    weekly = resample_weekly_closes(daily)
    yrs, avg, min3 = history_quality(weekly)
    if not daily:
        print(f"  {label}: empty")
        return
    print(
        f"  {label}: daily={len(daily)} yrs={years_spanned(daily):.1f} "
        f"weekly={len(weekly)} hist_yrs={yrs} avg_wpy={avg:.1f} min3={min3} "
        f"range={daily[0]['date']}..{daily[-1]['date']}"
    )


def main() -> None:
    start = date.today() - timedelta(days=20 * 366)
    print("=== OANDA ===")
    for sym in OANDA_CANDIDATES:
        bars, meta = _probe_candles(sym, from_time=_iso_from(start), count=OANDA_MAX_COUNT)
        if bars:
            daily = normalize_daily_bars(bars)
            _report(sym, daily)
        else:
            print(f"  {sym}: FAIL {meta.get('error')}")

    print("=== FMP ===")
    if not get_fmp_api_key():
        print("  FMP_API_KEY not set")
        return
    client = FmpClient()
    for sym in FMP_CANDIDATES:
        try:
            payload = client.get(f"api/v3/historical-price-full/{sym}")
            daily = normalize_daily_bars(parse_fmp_historical_payload(payload))
            _report(sym, daily)
        except FmpApiError as exc:
            print(f"  {sym}: FAIL {exc.status_code} {str(exc)[:100]}")


if __name__ == "__main__":
    main()
