"""Probe FMP historical depth for commodity seasonality foundation."""
from __future__ import annotations

from hptl.data_sources.fmp_client import FmpClient
from hptl.prices.canonical_timeline import resample_weekly_closes
from hptl.seasonality.seasonality_price_bars import history_quality
from hptl.seasonality.seasonality_v2 import normalize_daily_bars, parse_fmp_historical_payload, years_spanned

SYMBOLS = {
    "Copper / HG": ["HGUSD", "CPER", "COPX"],
    "Corn": ["ZCUSD", "CORN", "ZOUSX"],
    "Cotton": ["CTUSD", "BAL"],
    "Coffee": ["KCUSD", "JO", "CAFE"],
}


def main() -> None:
    client = FmpClient()
    for market, syms in SYMBOLS.items():
        print(f"--- {market} ---")
        for sym in syms:
            try:
                payload = client.get(f"api/v3/historical-price-full/{sym}")
                daily = normalize_daily_bars(parse_fmp_historical_payload(payload))
                weekly = resample_weekly_closes(daily)
                yrs, avg, _ = history_quality(weekly)
                if daily:
                    print(
                        f"  {sym}: daily={len(daily)} yrs={years_spanned(daily):.1f} "
                        f"weekly={len(weekly)} hist_yrs={yrs} avg_wpy={avg:.1f} "
                        f"range={daily[0]['date']}..{daily[-1]['date']}"
                    )
                else:
                    print(f"  {sym}: empty")
            except Exception as exc:
                print(f"  {sym}: ERROR {str(exc)[:100]}")


if __name__ == "__main__":
    main()
