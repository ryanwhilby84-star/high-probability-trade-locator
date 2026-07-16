"""Refresh Natural Gas institutional drivers and rebuild valuation export.

Usage (repo root):
  python scripts/refresh_natural_gas_drivers.py

Requires EIA_API_KEY for full EIA coverage. FRED_API_KEY enables DXY + FRED fallbacks.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from hptl.data_sources.energy_ng_driver_ingest import ingest_all_ng_drivers  # noqa: E402
from hptl.valuation.energy_ng_valuation_export import write_natural_gas_valuation_exports  # noqa: E402


def main() -> int:
    summary = ingest_all_ng_drivers()
    audit = Path("data/audits/energy_ng_driver_ingest_audit.json")
    audit.parent.mkdir(parents=True, exist_ok=True)
    audit.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("=== NG driver ingest ===")
    print(f"required_keys: {summary.get('required_keys')}")
    for key, row in (summary.get("drivers") or {}).items():
        print(
            f"  {key}: {row.get('status')} n={row.get('n_observations')} "
            f"latest={row.get('latest_date')} value={row.get('latest_value')} "
            f"src={row.get('source')}"
        )
        if row.get("error"):
            print(f"    error: {row['error']}")

    paths = write_natural_gas_valuation_exports()
    doc = json.loads(paths["data"].read_text(encoding="utf-8"))
    inst = doc.get("instrument") or {}
    print("=== NG valuation ===")
    print(f"Wrote {paths['data']}")
    print(
        f"spot={inst.get('spot_price')} fair={inst.get('fair_value')} "
        f"dev={inst.get('deviation_pct')} confidence={inst.get('confidence')} "
        f"features={inst.get('active_features')}"
    )
    cards = inst.get("driver_cards") or []
    if isinstance(cards, dict):
        cards = list(cards.values())
    for c in cards:
        if not isinstance(c, dict):
            continue
        print(
            f"  card {c.get('id')}: available={c.get('available')} "
            f"current={c.get('current')} effect={c.get('institutional_effect')} "
            f"freshness={c.get('freshness')} source={c.get('source')}"
        )
    print(f"Awaiting: {inst.get('awaiting_drivers')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
