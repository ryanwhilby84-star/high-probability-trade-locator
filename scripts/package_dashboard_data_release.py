#!/usr/bin/env python3
"""Package a complete, validated dashboard data snapshot for deployment.

This does NOT publish or overwrite live data. It refuses incomplete snapshots.
Usage: python scripts/package_dashboard_data_release.py --data-dir web-dashboard/public/data --output /tmp/dashboard-data.zip
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

REQUIRED = {
    "confluence_history_latest.json": ("records",),
    "cot_weekly_inspector_latest.json": ("markets",),
    "cot_commercial_attention_latest.json": ("attention_board",),
    "instrument_registry.json": ("markets",),
    "prices_latest.json": (),
}

def validate(data_dir: Path) -> tuple[list[str], dict]:
    errors = []
    files = {}
    for name, required_keys in REQUIRED.items():
        path = data_dir / name
        if not path.is_file():
            errors.append(f"{name}: missing")
            continue
        try:
            raw = path.read_bytes()
            doc = json.loads(raw)
            if not isinstance(doc, dict):
                raise ValueError("expected JSON object")
            for key in required_keys:
                if not isinstance(doc.get(key), (list, dict)) or not doc[key]:
                    errors.append(f"{name}: {key} missing or empty")
            if name == "confluence_history_latest.json" and not doc.get("latest_cot_report_date"):
                errors.append(f"{name}: missing latest_cot_report_date")
            files[name] = {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
        except (OSError, ValueError) as exc:
            errors.append(f"{name}: {exc}")
    return errors, files

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("web-dashboard/public/data"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    errors, files = validate(args.data_dir)
    if errors:
        print("Refusing incomplete dashboard data release:", file=sys.stderr)
        for error in errors:
            print(f" - {error}", file=sys.stderr)
        return 1
    manifest = {
        "schema": "institutional-edge-data-release-v1",
        "packaged_at": datetime.now(timezone.utc).isoformat(),
        "files": files,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(args.output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in sorted(files):
            archive.write(args.data_dir / name, arcname=f"data/{name}")
        archive.writestr("manifest.json", json.dumps(manifest, indent=2) + "\n")
    print(f"Validated release package: {args.output} ({len(files)} files)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
