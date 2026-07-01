"""Probe OANDA commodity symbols for seasonality foundation."""
from __future__ import annotations

from datetime import date, timedelta

from hptl.prices.fx_oanda_backfill_feasibility_audit import OANDA_MAX_COUNT, _iso_from, _probe_candles

CANDIDATES = [
    ("Copper", "XCU_USD"),
    ("Copper2", "XCUUSD"),
    ("Corn", "CORN_USD"),
    ("Coffee", "COFFEE_USD"),
    ("Cotton", "COTTON_USD"),
    ("Wheat-ref", "WHEAT_USD"),
]


def main() -> None:
    start = date.today() - timedelta(days=10 * 366)
    for label, sym in CANDIDATES:
        bars, meta = _probe_candles(sym, from_time=_iso_from(start), count=OANDA_MAX_COUNT)
        err = meta.get("error")
        if bars:
            print(f"{label} {sym}: {len(bars)} bars {bars[0]['date']}..{bars[-1]['date']}")
        else:
            print(f"{label} {sym}: FAIL {err}")


if __name__ == "__main__":
    main()
