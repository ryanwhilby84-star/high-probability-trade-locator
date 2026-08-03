"""Refresh Natural Gas market price + republish valuation price freshness.

One command:
  1) OANDA OHLC + live snapshot → canonical price store (+ public/dist)
  2) live_quotes_latest export
  3) NG valuation re-export (reads refreshed canonical price; no COT/confluence)

Usage:
  python scripts/refresh_natural_gas_market_price.py

Does not modify weekly COT workflow or valuation formulas.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

MARKET = "Natural Gas / NG"


def main() -> int:
    t0 = time.perf_counter()
    from hptl.prices.coverage import load_price_coverage
    from hptl.prices.live_quotes_export import write_live_quotes_exports
    from hptl.prices.price_freshness import build_instrument_price_freshness
    from hptl.prices.price_store import load_instrument_record_internal
    from hptl.prices.run_price_refresh import refresh_instrument_record
    from hptl.prices.unified_adapter import UnifiedPriceAdapter
    from hptl.prices.price_store import write_price_store_merged
    from hptl.valuation.energy_ng_valuation_export import write_natural_gas_valuation_exports

    print("=== 1) Refresh Natural Gas OANDA price ===")
    coverage = load_price_coverage()
    adapter = UnifiedPriceAdapter(coverage)
    fetched = adapter.fetch(MARKET)
    via = str(fetched.get("_fetched_via") or "oanda")
    rec = refresh_instrument_record(MARKET, fetched, fetched_via=via)
    path = write_price_store_merged({MARKET: rec}, coverage_generated_at=coverage.get("generated_at"))
    print(f"  fetched_via={via} err={rec.get('error')}")
    print(f"  snapshot={rec.get('price')}")
    daily = rec.get("daily") or []
    print(f"  completed_daily_tip={(daily[-1] if daily else None)}")
    print(f"  forming_daily={rec.get('forming_daily')}")
    print(f"  store={path}")

    print("=== 2) Live quotes export ===")
    lq = write_live_quotes_exports()
    print(f"  wrote {lq}")

    print("=== 3) NG valuation re-export (price freshness only; formulas unchanged) ===")
    paths = write_natural_gas_valuation_exports()
    doc = json.loads(paths["data"].read_text(encoding="utf-8"))
    inst = doc.get("instrument") or {}

    internal = load_instrument_record_internal(MARKET) or {}
    freshness = build_instrument_price_freshness(
        internal,
        provider=((internal.get("price_scale") or {}).get("source") or "oanda"),
        symbol=((internal.get("price_scale") or {}).get("symbol") or "NATGAS_USD"),
    )

    elapsed = round(time.perf_counter() - t0, 2)
    print("=== RESULT ===")
    print(f"  runtime_sec={elapsed}")
    print(f"  live={freshness.get('live_quote')}")
    print(f"  completed_daily={freshness.get('latest_completed_daily')}")
    print(f"  weekly={freshness.get('latest_completed_weekly')}")
    print(f"  forming_daily={freshness.get('forming_daily')}")
    print(f"  market_comparison={freshness.get('market_comparison')}")
    print(
        f"  valuation fair={inst.get('fair_value')} "
        f"model_anchor={inst.get('model_anchor_price') or inst.get('spot_price')} "
        f"trusted_dev={inst.get('deviation_pct')} "
        f"trusted={inst.get('deviation_pct_trusted')} "
        f"warnings={inst.get('freshness_warnings')}"
    )
    return 0 if not rec.get("error") else 1


if __name__ == "__main__":
    raise SystemExit(main())
