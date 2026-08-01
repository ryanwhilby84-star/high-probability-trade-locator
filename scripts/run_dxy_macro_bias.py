"""Refresh DXY FRED proxy prices, workstation OHLC, and macro bias export."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.prices.fred_prices import fetch_fred_instrument
from hptl.prices.price_store import write_price_store_merged
from hptl.prices.run_price_refresh import refresh_instrument_record
from hptl.prices.workstation_ohlc_export import write_workstation_ohlc_exports
from hptl.valuation.dxy_macro_bias_export import write_dxy_macro_bias_exports

DX = "US Dollar Index / DX"


def main() -> int:
    fetched = fetch_fred_instrument(DX)
    rec = refresh_instrument_record(DX, fetched, fetched_via="fred")
    write_price_store_merged({DX: rec})
    daily_n = len(rec.get("daily") or [])
    weekly_n = len(rec.get("weekly") or [])
    print(f"DX price store: daily={daily_n} weekly={weekly_n} as_of={(rec.get('price') or {}).get('as_of')}")

    ohlc_path = write_workstation_ohlc_exports()
    print(f"Workstation OHLC: {ohlc_path}")

    paths = write_dxy_macro_bias_exports()
    print(f"DXY macro bias: {paths.get('public')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
