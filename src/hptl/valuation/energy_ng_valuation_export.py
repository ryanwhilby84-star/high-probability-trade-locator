"""Export Natural Gas institutional valuation for the dashboard."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.valuation.energy_natural_gas_valuation_v1 import build_natural_gas_valuation_document

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
        f"NG V1 validated: wired={inst.get('wired')} spot={inst.get('spot_price')} "
        f"fair={inst.get('fair_value')} dev={inst.get('deviation_pct')} "
        f"bias={inst.get('institutional_bias')} features={inst.get('active_features')}"
    )
    print(f"experimental={inst.get('experimental_features')}")
    print(f"informational={inst.get('informational_features')}")
    print(f"Awaiting: {inst.get('awaiting_drivers')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
