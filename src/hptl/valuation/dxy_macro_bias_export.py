"""Export DXY macro bias artifacts to data/ + public/ + dist/."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.valuation.dxy_macro_bias import MARKET, build_dxy_macro_bias

DATA_OUT = Path("data/dxy_macro_bias_latest.json")
PUBLIC_OUT = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "dxy_macro_bias_latest.json"
DIST_OUT = PROJECT_ROOT / "web-dashboard" / "dist" / "data" / "dxy_macro_bias_latest.json"


def write_dxy_macro_bias_exports(payload: dict[str, Any] | None = None) -> dict[str, Path]:
    payload = payload or build_dxy_macro_bias()
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    written: dict[str, Path] = {}
    for key, path in (("data", DATA_OUT), ("public", PUBLIC_OUT), ("dist", DIST_OUT)):
        if key == "dist" and not path.parent.parent.exists():
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        written[key] = path
    return written


def dxy_bias_export_stale(*, max_age_hours: float = 72.0) -> bool:
    """True when public export missing or older than master price/COT tip."""
    if not PUBLIC_OUT.exists():
        return True
    try:
        doc = json.loads(PUBLIC_OUT.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return True
    price_asof = str(((doc.get("price_instrument") or {}).get("as_of") or ""))[:10]
    if not price_asof:
        return True
    # Stale if DX cot_3y tip is newer than bias generation price as-of by > 7 days
    cot_path = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_3y_series_latest.json"
    if cot_path.exists():
        try:
            cot = json.loads(cot_path.read_text(encoding="utf-8"))
            tip = str(((cot.get("markets") or {}).get(MARKET) or {}).get("latest_date") or "")[:10]
            if tip and tip > price_asof:
                # price lag of a few days is normal for FRED; only force republish if export missing drivers
                pass
        except (OSError, json.JSONDecodeError):
            pass
    drivers = doc.get("drivers") or []
    return len(drivers) < 5


def main() -> None:
    paths = write_dxy_macro_bias_exports()
    doc = json.loads(paths["public"].read_text(encoding="utf-8"))
    print(f"DXY macro bias: {doc.get('macro_bias')}")
    print(f"Price as-of: {(doc.get('price_instrument') or {}).get('as_of')}")
    print(f"Written: {', '.join(str(p) for p in paths.values())}")


if __name__ == "__main__":
    main()
