"""Standalone macro relationship refresh (decoupled from the COT weekly job).

Refreshes the macro relationship maps from live FRED (with retry/backoff + cache
fallback), writes the maps + audit, and prints a coverage summary. Safe to run on
its own schedule. Never blanks existing maps: a failed refresh falls back to the
persistent series cache, and the non-destructive merge preserves prior valid maps.

Usage:
    python -m hptl.macro.run_macro_refresh            # live refresh (default)
    python -m hptl.macro.run_macro_refresh --cache-only  # rebuild from cache only
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Refresh macro relationship maps (Stage B resilient pipeline).")
    parser.add_argument(
        "--cache-only",
        action="store_true",
        help="Rebuild from the persistent cache only (no network).",
    )
    args = parser.parse_args(argv)

    # Control the feed gate for THIS process only.
    if args.cache_only:
        os.environ["HPTL_SKIP_LIVE_FEEDS"] = "1"
    else:
        os.environ["HPTL_SKIP_LIVE_FEEDS"] = "0"

    from hptl.confluence.dashboard_export import write_macro_maps_export
    from hptl.macro import fred_client
    from hptl.macro.macro_audit import build_macro_audit, write_macro_audit
    from hptl.macro.macro_relationship_maps import (
        _load_previous_maps,
        build_all_macro_relationship_maps,
    )

    mode = "cache-only" if args.cache_only else "live"
    print(f"=== Macro relationship refresh ({mode}) ===")
    print(f"HPTL_SKIP_LIVE_FEEDS={os.environ.get('HPTL_SKIP_LIVE_FEEDS')}")
    print(f"Cache dir: {fred_client.cache_dir()}")

    previous = _load_previous_maps()
    generated_at = datetime.now(timezone.utc).isoformat()
    maps = build_all_macro_relationship_maps(previous_maps=previous)

    maps_path = write_macro_maps_export(maps, generated_at=generated_at)
    audit = build_macro_audit(maps, generated_at=generated_at)
    write_macro_audit(audit)

    s = audit["summary"]
    print(f"Wrote {maps_path}")
    print(
        f"Coverage: {s['available']}/{s['total']} available — "
        f"live={s['live']} cached={s['cached']} stale={s['stale']} "
        f"warning={s['warning']} missing={s['missing']}"
    )
    print(f"Last successful refresh: {s['last_successful_refresh']}")
    print(f"Last failed refresh: {s['last_failed_refresh']}")
    for row in audit["assets"]:
        ids = ", ".join(row.get("source_series_ids") or []) or "—"
        fail = f"  ! {row['failure_reason']}" if row.get("failure_reason") else ""
        print(
            f"  [{row['data_status']:<7}] {row['asset']:<16} "
            f"obs={row.get('latest_observation_date') or '—'} "
            f"lat={row.get('latency_days')} src=({ids}){fail}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
