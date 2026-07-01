#!/usr/bin/env python3
"""CFTC freshness audit — trace report_date through each pipeline stage."""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

PUBLIC = ROOT / "web-dashboard" / "public" / "data"
DATA = ROOT / "data"
PROCESSED = DATA / "processed"
RAW = DATA / "raw"


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _legacy_max(doc: dict) -> str:
    maxd = ""
    for block in (doc.get("instruments") or {}).values():
        weeks = ((block.get("groups") or {}).get("noncommercials") or {}).get("weeks") or []
        if weeks:
            d = str(weeks[-1].get("report_date") or "")[:10]
            if d and d > maxd:
                maxd = d
    return maxd or "—"


def _master_max() -> str:
    path = PROCESSED / "cot_tracked_master_normalized.csv"
    if not path.exists():
        return "—"
    df = pd.read_csv(path, usecols=["cot_report_date"], low_memory=False)
    return str(df["cot_report_date"].astype(str).str[:10].max())


def _parsed_max() -> tuple[str, str]:
    """Latest report_date from most recent cot_cleaned CSV."""
    files = sorted(PROCESSED.glob("cot_cleaned_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return "—", "—"
    path = files[0]
    df = pd.read_csv(path, low_memory=False)
    col = next((c for c in ("report_date", "cot_report_date", "date") if c in df.columns), None)
    if not col:
        return "—", path.name
    return str(pd.to_datetime(df[col], errors="coerce").max().date()), path.name


def _raw_latest() -> tuple[str, str]:
    zips = sorted(RAW.glob("cot_*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not zips:
        return "—", "—"
    return "—", zips[0].name


def _instrument_sample() -> str:
    conf = _read_json(PUBLIC / "confluence_history_latest.json")
    by_mkt = conf.get("latest_cot_report_date_by_market") or {}
    if not by_mkt:
        return "—"
    return str(max(str(v)[:10] for v in by_mkt.values()))


def main() -> None:
    source_date = "—"
    source_note = ""
    try:
        from hptl.cot.report_dates import probe_cftc_latest_report_date

        probe = probe_cftc_latest_report_date()
        if probe.latest_report_date is not None and not pd.isna(probe.latest_report_date):
            source_date = str(probe.latest_report_date)[:10]
        source_note = f"rows={probe.rows_fetched}"
    except Exception as exc:
        source_note = f"probe failed: {exc}"

    probe_cache = _read_json(PROCESSED / "cot_probe_cache.json")
    cached = str(probe_cache.get("latest_cftc_report_date") or "—")[:10]
    raw_date, raw_file = _raw_latest()
    parsed_date, parsed_file = _parsed_max()
    processed_date = _master_max()
    legacy_data = _legacy_max(_read_json(DATA / "legacy_cot_latest.json"))
    legacy_public = _legacy_max(_read_json(PUBLIC / "legacy_cot_latest.json"))
    instrument_date = _instrument_sample()
    dashboard_date = str(_read_json(PUBLIC / "confluence_history_latest.json").get("latest_cot_report_date") or "—")[:10]
    ui_date = dashboard_date

    stages = [
        ("Source latest CFTC report date", source_date, "OK" if source_date != "—" else "FAIL", "CFTC live probe", source_note),
        ("Raw downloaded file", raw_date, "OK" if raw_file != "—" else "MISSING", raw_file, "mtime-ordered ZIP"),
        ("Parsed COT table", parsed_date, "OK" if parsed_date != "—" else "MISSING", parsed_file, "cot_cleaned_*.csv"),
        ("Processed/cache", processed_date, "OK", "cot_tracked_master_normalized.csv", f"legacy JSON max {legacy_data}"),
        ("Instrument-level", instrument_date, "OK" if instrument_date != "—" else "MISSING", "confluence latest_cot_report_date_by_market", ""),
        ("Dashboard export", dashboard_date, "OK" if dashboard_date != "—" else "MISSING", "confluence_history_latest.json", ""),
        ("UI data file", ui_date, "OK" if ui_date != "—" else "MISSING", "web-dashboard/public/data/confluence_history_latest.json", ""),
    ]

    break_point = "—"
    fix = "—"
    dates = [s[1] for s in stages if s[1] not in ("—", "")]
    if source_date != "—" and dashboard_date != "—" and source_date > dashboard_date:
        if processed_date != "—" and processed_date < source_date:
            break_point = "Legacy refresh / master rebuild not advancing past processed cache"
            fix = "Run python -m hptl.cot.run_update (live probe always; legacy ZIP refresh)"
        elif processed_date == source_date and dashboard_date < source_date:
            break_point = "Confluence export not rebuilt after master refresh"
            fix = "Rebuild confluence JSON from current master"
        else:
            break_point = f"Pipeline stalled before dashboard ({dashboard_date} < {source_date})"
            fix = "Run full COT update pipeline"
    elif source_date == dashboard_date:
        break_point = "None — pipeline current"
        fix = "None required"
    elif source_date == "—":
        break_point = "CFTC source probe unavailable"
        fix = "Check network / CFTC endpoints"

    print("| Stage | Latest report_date | Status | Source/file | Notes |")
    print("| --- | --- | --- | --- | --- |")
    for stage, dt, status, src, notes in stages:
        print(f"| {stage} | {dt} | {status} | {src} | {notes} |")

    print()
    print(f"CFTC freshness status: {'CURRENT' if source_date == dashboard_date and source_date != '—' else 'STALE'}")
    print(f"Engine updated?: {'Yes' if processed_date == source_date else 'No'}")
    print(f"Dashboard updated?: {'Yes' if dashboard_date == source_date else 'No'}")
    print(f"Break point: {break_point}")
    print(f"Fix applied / Fix required: {fix}")
    if cached != "—" and cached != source_date:
        print(f"Probe cache note: cached={cached} (live={source_date})")


if __name__ == "__main__":
    main()
