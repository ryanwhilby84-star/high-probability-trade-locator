"""Phase 3A quick valuation check."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.agri_fundamental_valuation import compute_agri_valuation
from hptl.valuation.metals_valuation_select import compute_metals_valuation_v2

OLD = {
    "Corn": 27.09,
    "Gold": 76.81,
    "Silver": 143.06,
    "Sugar": -27.07,
    "Copper / HG": 59.68,
    "Wheat": -2.83,
    "Soybeans": -3.19,
    "Cotton": 11.45,
    "Platinum": 49.93,
    "Palladium": 26.15,
}

rows = []
for m, old in OLD.items():
    if m in ("Corn", "Sugar", "Wheat", "Soybeans", "Cotton"):
        v = compute_agri_valuation(market=m)
    else:
        v = compute_metals_valuation_v2(market=m)
    aud = v.get("institutional_audit") or {}
    rows.append(
        {
            "market": m,
            "old_valuation_pct": old,
            "new_valuation_pct": v.get("deviation_pct"),
            "model": v.get("model_id"),
            "r_squared": aud.get("r_squared") or (v.get("regression") or {}).get("r_squared"),
            "status": aud.get("status") or ("WITHHELD" if not v.get("publish") else "PRODUCTION_READY"),
            "publish": bool(v.get("publish")),
            "blocker": v.get("withheld_reason") or "",
            "fair_value": v.get("fair_value"),
            "spot": v.get("spot_price"),
            "reversion_60d": aud.get("reversion_60d_pct"),
        }
    )

print(json.dumps(rows, indent=2))
