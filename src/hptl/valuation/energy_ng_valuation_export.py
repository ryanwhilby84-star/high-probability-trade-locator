"""Export Natural Gas valuation for the dashboard (standalone; not weekly COT)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.valuation.ng_storage_production_v2 import build_natural_gas_valuation_document

DATA_OUT = Path("data/natural_gas_valuation_latest.json")
PUBLIC_OUT = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "natural_gas_valuation_latest.json"
DIST_OUT = PROJECT_ROOT / "web-dashboard" / "dist" / "data" / "natural_gas_valuation_latest.json"


def write_natural_gas_valuation_exports(*, as_of_week: str | None = None) -> dict[str, Path]:
    payload = build_natural_gas_valuation_document(as_of_week=as_of_week)
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    written: dict[str, Path] = {}
    for key, path in (("data", DATA_OUT), ("public", PUBLIC_OUT), ("dist", DIST_OUT)):
        if path == DIST_OUT and not path.parent.parent.exists():
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        written[key] = path
    return written


def main() -> int:
    paths = write_natural_gas_valuation_exports()
    doc = json.loads(paths["data"].read_text(encoding="utf-8"))
    inst = doc.get("instrument") or {}
    print(f"Wrote {paths['data']}")
    print(
        f"NG active={inst.get('active_model')} fallback={inst.get('fallback_to_v1')} "
        f"spot={inst.get('spot_price')} fair={inst.get('fair_value')} "
        f"dev={inst.get('deviation_pct')} confidence={inst.get('confidence')}"
    )
    print(
        f"v1_fair={inst.get('v1_fair_value')} v2_fair={inst.get('v2_fair_value')} "
        f"drivers={inst.get('validated_drivers')} "
        f"prod_yoy={inst.get('production_yoy_value')} "
        f"prod_obs={inst.get('production_observation_date')}"
    )
    print(f"warnings={inst.get('freshness_warnings')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
