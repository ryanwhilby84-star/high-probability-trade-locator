import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.gold_market_clearing_valuation import (  # noqa: E402
    HISTORY_CSV,
    RECON_MD,
    _render_reconciliation,
)

rows = list(csv.DictReader(HISTORY_CSV.open(encoding="utf-8")))
for r in rows:
    for k, v in list(r.items()):
        if v in ("True", "False"):
            r[k] = v == "True"
        elif v in ("", None):
            r[k] = None
        else:
            try:
                if k not in {
                    "date",
                    "usable_date",
                    "publication_date",
                    "bucket",
                    "solver_status",
                    "demand_parts",
                    "supply_parts",
                    "premium_discount",
                }:
                    r[k] = float(v)
            except ValueError:
                pass
RECON_MD.write_text(_render_reconciliation(rows), encoding="utf-8")
print("wrote", RECON_MD)
