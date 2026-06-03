"""Seed the Thesis Tracker with REAL multi-week snapshots from the latest export.

This is the Phase 1 data provider for the UI. It does not fabricate numbers:
every snapshot is copied from ``confluence_history_latest.json``. Statuses /
direction are assigned for demonstration and can be edited in-app later.

Usage:
    python -m hptl.thesis_tracker.run_thesis_seed
    python -m hptl.thesis_tracker.run_thesis_seed --markets "Sugar,Gold" --weeks 6 --reset
"""

from __future__ import annotations

import argparse

from hptl.thesis_tracker import store
from hptl.thesis_tracker.conviction import annotate_conviction
from hptl.thesis_tracker.models import (
    new_thesis_id,
    norm_status,
    normalize_log_entry,
    normalize_thesis,
    now_iso,
)
from hptl.thesis_tracker.narrative import build_evolution_note
from hptl.cot.cot_quarantine import is_quarantined
from hptl.markets.instrument_registry import cot_mapped_ids
from hptl.thesis_tracker.snapshot import load_records, market_history

# Demo lifecycle assignment (market -> status, direction hint is read from COT).
DEFAULT_PLAN: list[tuple[str, str]] = [
    ("Sugar", "DEVELOPING"),
    ("Gold", "READY"),
    ("Crude Oil / CL", "DISCOVERED"),
    ("Wheat", "DEVELOPING"),
    ("NASDAQ / NQ", "READY"),
    ("Copper / HG", "ACTIVE"),
]


def _direction_from(snapshots: list[dict]) -> str:
    """Thesis direction = the *emerging* move (accumulation/distribution), not the
    static net side. A market that is still net short but accumulating (shorts
    covering, longs building) is an emerging long thesis."""
    nets = [s.get("net_value") for s in snapshots if isinstance(s.get("net_value"), (int, float))]
    if len(nets) >= 2:
        first, last = nets[0], nets[-1]
        change = last - first
        floor = max(5000.0, 0.05 * abs(first)) if first else 5000.0
        if change >= floor:
            return "long"
        if change <= -floor:
            return "short"
    for snap in reversed(snapshots):
        bias = (snap.get("cot_bias") or "").lower()
        if "bull" in bias:
            return "long"
        if "bear" in bias:
            return "short"
    return "neutral"


def _build_thesis(market: str, status: str, snapshots: list[dict]) -> dict:
    annotate_conviction(snapshots)
    created_week = snapshots[0].get("week") if snapshots else None

    log: list[dict] = []
    prev = None
    for snap in snapshots:
        log.append(
            normalize_log_entry(
                {
                    "week": snap.get("week"),
                    "auto": True,
                    "text": build_evolution_note(prev, snap),
                    "created_at": snap.get("captured_at") or now_iso(),
                }
            )
        )
        prev = snap
    if status and norm_status(status) != "DISCOVERED":
        log.append(
            normalize_log_entry(
                {
                    "week": snapshots[-1].get("week") if snapshots else None,
                    "auto": False,
                    "text": f"Status set to {norm_status(status)} (seed).",
                }
            )
        )

    return normalize_thesis(
        {
            "thesis_id": new_thesis_id(),
            "market": market,
            "status": status,
            "direction_bias": _direction_from(snapshots),
            "created_at": now_iso(),
            "created_week": created_week,
            "source": "seed",
            "snapshots": snapshots,
            "evolution_log": log,
        },
        source="seed",
    )


def seed_all_cot_instruments(*, weeks: int = 13, reset: bool = True) -> int:
    """Seed every COT-mapped instrument — used by the weekly integrity gate."""
    records = load_records()
    if not records:
        return 1
    doc = {"version": store.SCHEMA_VERSION, "theses": []} if reset else store.load_tracker()
    existing_markets = {str(t.get("market")) for t in doc.get("theses") or []}
    built = 0
    for market in cot_mapped_ids():
        if is_quarantined(market):
            continue
        if market in existing_markets and not reset:
            continue
        snaps = market_history(records, market, limit=weeks)
        if not snaps:
            continue
        doc["theses"].append(_build_thesis(market, "DISCOVERED", snaps))
        built += 1
    store.save_and_export(doc)
    return 0 if built > 0 or doc.get("theses") else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed the Thesis Tracker with real multi-week snapshots.")
    parser.add_argument("--markets", default="", help="Comma-separated market names (defaults to demo set).")
    parser.add_argument("--weeks", type=int, default=6, help="Max weeks of history per thesis (default 6).")
    parser.add_argument("--reset", action="store_true", help="Replace any existing tracker contents.")
    parser.add_argument(
        "--all-cot",
        action="store_true",
        help="Seed every COT-mapped instrument (default when --reset).",
    )
    args = parser.parse_args(argv)

    records = load_records()
    if not records:
        print("No confluence_history_latest.json records found — run the decision-table build first.")
        return 1

    if args.markets.strip():
        plan = [(m.strip(), "DISCOVERED") for m in args.markets.split(",") if m.strip()]
    elif args.reset or args.all_cot:
        plan = [(m, "DISCOVERED") for m in cot_mapped_ids()]
    else:
        plan = list(DEFAULT_PLAN)

    doc = store.load_tracker() if not args.reset else {"version": store.SCHEMA_VERSION, "theses": []}
    existing_markets = {str(t.get("market")) for t in doc.get("theses") or []}

    built = 0
    skipped: list[str] = []
    for market, status in plan:
        if is_quarantined(market):
            skipped.append(f"{market} (COT integrity quarantine)")
            continue
        snaps = market_history(records, market, limit=args.weeks)
        if not snaps:
            skipped.append(f"{market} (no records)")
            continue
        if market in existing_markets and not args.reset:
            skipped.append(f"{market} (already tracked)")
            continue
        doc["theses"].append(_build_thesis(market, status, snaps))
        built += 1

    paths = store.save_and_export(doc)

    print(f"Seeded {built} thesis(es); {len(doc['theses'])} total.")
    if skipped:
        print("Skipped: " + ", ".join(skipped))
    print("Canonical:", store.TRACKER_PATH)
    for p in paths:
        print("Exported :", p)
    for t in doc["theses"]:
        print(
            f"  - {t['market']:<18} {t['status']:<16} "
            f"weeks={t['age_weeks']} conviction={t['conviction_current']} trend={t['conviction_trend']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
