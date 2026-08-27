"""CLI: refresh FX futures data sources and export currency futures IVE."""
from __future__ import annotations

import json
import sys

from hptl.valuation.currency_futures_ive_v1 import write_currency_futures_ive_export
from hptl.valuation.fx_futures_data_refresh import refresh_fx_futures_data


def main() -> int:
    refresh = refresh_fx_futures_data()
    print("Data refresh:", json.dumps(refresh, indent=2), flush=True)

    paths = write_currency_futures_ive_export()
    print(f"Wrote {paths['public_json']}")

    from hptl.valuation.currency_futures_ive_v1 import build_currency_futures_ive_export

    doc = build_currency_futures_ive_export()
    print("\n=== FX Futures Valuation Status ===")
    for sym in ("DX", "6E", "6B", "6A", "6C", "6J", "6S", "6N"):
        block = doc["by_symbol"].get(sym) or {}
        if block.get("wired"):
            print(
                f"{sym}: VALIDATED  {block.get('valuation_pct'):+.2f}%  "
                f"{block.get('valuation_label')}"
            )
        else:
            print(f"{sym}: {block.get('blocker_reason')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

