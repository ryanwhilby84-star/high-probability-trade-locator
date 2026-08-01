"""Export COT Positioning Research for the full supported COT series universe."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.cot.positioning_research_engine import build_positioning_research_doc

COT3Y_PATHS = (
    PROCESSED_DIR / "cot_3y_series_latest.json",
    PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_3y_series_latest.json",
    PROJECT_ROOT / "data" / "cot_3y_series_latest.json",
)

CANONICAL_PATH = PROCESSED_DIR / "cot_positioning_research_latest.json"
PUBLIC_PATH = (
    PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_positioning_research_latest.json"
)
DIST_PATH = PROJECT_ROOT / "web-dashboard" / "dist" / "data" / "cot_positioning_research_latest.json"
DATA_PATH = PROJECT_ROOT / "data" / "cot_positioning_research_latest.json"


def _load_cot3y() -> dict[str, Any]:
    for p in COT3Y_PATHS:
        if p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))
    return {"markets": {}}


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def run_positioning_research_export(
    *,
    cot3y: dict[str, Any] | None = None,
    markets: list[str] | None = None,
) -> dict[str, Any]:
    """Export research for all cot3y markets unless ``markets`` narrows the set."""
    doc = cot3y if cot3y is not None else _load_cot3y()
    payload = build_positioning_research_doc(doc, markets=markets)
    payload["generated_at"] = datetime.now(timezone.utc).isoformat()

    for path in (CANONICAL_PATH, PUBLIC_PATH, DATA_PATH):
        _write(path, payload)
    if DIST_PATH.parent.is_dir():
        try:
            shutil.copy2(PUBLIC_PATH, DIST_PATH)
        except OSError:
            _write(DIST_PATH, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run COT positioning research export for the full cot3y universe "
            "(or an optional debug subset via --market)."
        )
    )
    parser.add_argument(
        "--market",
        action="append",
        dest="markets",
        help="Optional market id to narrow export (debug/tests). Repeatable. "
        "Default: entire cot3y universe.",
    )
    args = parser.parse_args(argv)
    # None ⇒ full universe; explicit list ⇒ subset only for debug.
    markets = args.markets if args.markets else None
    payload = run_positioning_research_export(markets=markets)
    s = payload.get("summary") or {}
    print(
        "Positioning Research — "
        f"available={s.get('markets_available')}/{s.get('markets_requested')} "
        f"(source={s.get('markets_in_source')})"
    )
    for mid, m in sorted((payload.get("markets") or {}).items()):
        if not m.get("available"):
            print(f"  {mid}: unavailable ({m.get('reason')})")
            continue
        ps = m.get("primary_band_summary") or {}
        hi = ps.get("high_spread_12w") or {}
        lo = ps.get("low_spread_12w") or {}
        print(
            f"  {mid}: week={m.get('source_week')} weeks={m.get('weeks')} "
            f"markers={len(m.get('markers') or [])} "
            f"events={((m.get('configuration_events') or {}).get('total'))}"
        )
        print(
            f"    primary {ps.get('band')}: high_n={ps.get('high_spread_independent_cases')} "
            f"12W {hi.get('higher_count')}/{hi.get('n')} med={hi.get('median_return_pct')} "
            f"tend={ps.get('high_tendency_12w')}"
        )
        print(
            f"    primary {ps.get('band')}: low_n={ps.get('low_spread_independent_cases')} "
            f"12W {lo.get('higher_count')}/{lo.get('n')} med={lo.get('median_return_pct')} "
            f"tend={ps.get('low_tendency_12w')}"
        )
        cur = m.get("current_interpretation") or {}
        print(
            f"    current: spread_pct={(cur.get('spread') or {}).get('percentile')} "
            f"analogues={(cur.get('analogues') or {}).get('independent_cases')} "
            f"interp={cur.get('interpretation')}"
        )
    print(f"wrote {PUBLIC_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
