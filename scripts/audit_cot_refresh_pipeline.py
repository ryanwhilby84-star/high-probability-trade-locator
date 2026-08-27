#!/usr/bin/env python3
"""End-to-end COT refresh pipeline audit — dates at each stage."""
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


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _mtime(path: Path) -> str:
    if not path.exists():
        return "—"
    return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")


def main() -> None:
    rows: list[tuple[str, str, str, str]] = []

    # 1. Source (CFTC probe)
    source_date = "—"
    source_note = ""
    try:
        from hptl.cot.report_dates import probe_cftc_latest_report_date

        probe = probe_cftc_latest_report_date()
        if probe.latest_report_date is not None and not pd.isna(probe.latest_report_date):
            source_date = str(probe.latest_report_date)[:10]
        source_note = f"probe rows={probe.rows_fetched}"
    except Exception as exc:
        source_note = f"probe failed: {exc}"
    rows.append(("1. CFTC source (probe)", source_date, source_note, _mtime(DATA / "processed" / "cot_probe_cache.json")))

    # 2. Latest fetched locally (raw + probe cache)
    probe_cache = _read_json(DATA / "processed" / "cot_probe_cache.json")
    fetched = str(probe_cache.get("latest_report_date") or probe_cache.get("latest_cftc_report_date") or "—")[:10]
    raw_dir = DATA / "raw"
    latest_zip = "—"
    if raw_dir.exists():
        zips = sorted(raw_dir.glob("*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
        if zips:
            latest_zip = f"{zips[0].name} ({_mtime(zips[0])})"
    rows.append(("2. Local fetch (probe cache)", fetched, latest_zip, _mtime(raw_dir)))

    # 3. Normalized master
    master_path = PROCESSED / "cot_tracked_master_normalized.csv"
    master_date = "—"
    if master_path.exists():
        df = pd.read_csv(master_path)
        col = "cot_report_date" if "cot_report_date" in df.columns else "report_date"
        if col in df.columns:
            master_date = str(df[col].astype(str).str[:10].max())
    rows.append(("3. Master CSV", master_date, str(master_path.relative_to(ROOT)), _mtime(master_path)))

    def _legacy_max_date(doc: dict) -> str:
        maxd = "—"
        for block in (doc.get("instruments") or {}).values():
            weeks = ((block.get("groups") or {}).get("noncommercials") or {}).get("weeks") or []
            if weeks:
                d = str(weeks[-1].get("report_date") or "")[:10]
                if d and (maxd == "—" or d > maxd):
                    maxd = d
        return maxd

    # 4. legacy_cot_latest
    for label, path in [
        ("3b. legacy_cot (data)", DATA / "legacy_cot_latest.json"),
        ("3c. legacy_cot (public)", PUBLIC / "legacy_cot_latest.json"),
    ]:
        doc = _read_json(path)
        maxd = _legacy_max_date(doc)
        summ = doc.get("generated_at") or doc.get("as_of_week")
        rows.append((label, maxd, f"generated={str(summ)[:19]}", _mtime(path)))

    # 5. cot_3y_series
    for label, path in [
        ("4. cot_3y (processed)", PROCESSED / "cot_3y_series_latest.json"),
        ("5. cot_3y (public)", PUBLIC / "cot_3y_series_latest.json"),
    ]:
        doc = _read_json(path)
        maxd = "—"
        for block in (doc.get("markets") or {}).values():
            d = block.get("latest_date")
            if d and (maxd == "—" or str(d)[:10] > maxd):
                maxd = str(d)[:10]
            ser = block.get("series") or []
            if ser:
                sd = str(ser[-1].get("date", ""))[:10]
                if sd and (maxd == "—" or sd > maxd):
                    maxd = sd
        rows.append((label, maxd, f"generated={str(doc.get('generated_at',''))[:19]}", _mtime(path)))

    # 6. confluence
    conf = _read_json(PUBLIC / "confluence_history_latest.json")
    conf_date = str(conf.get("latest_cot_report_date") or "—")[:10]
    feed = conf.get("cot_feed_status") or {}
    rows.append(
        (
            "6. confluence (public)",
            conf_date,
            f"stale={feed.get('is_stale')} cftc={feed.get('latest_cftc_report_date')}",
            _mtime(PUBLIC / "confluence_history_latest.json"),
        )
    )

    dist_conf = ROOT / "web-dashboard" / "dist" / "data" / "confluence_history_latest.json"
    if dist_conf.exists():
        ddoc = _read_json(dist_conf)
        rows.append(
            (
                "6b. confluence (dist)",
                str(ddoc.get("latest_cot_report_date") or "—")[:10],
                "vite build copy",
                _mtime(dist_conf),
            )
        )

    # 7. UI reads confluence latest_cot_report_date + cot_3y for charts
    rows.append(("7. UI displayed date", conf_date, "from confluence_history_latest.json latest_cot_report_date", "—"))

    print("# COT refresh pipeline audit\n")
    print("| Stage | Latest date | Detail | File mtime |")
    print("|-------|-------------|--------|------------|")
    for stage, dt, detail, mt in rows:
        print(f"| {stage} | **{dt}** | {detail} | {mt} |")

    dates = [r[1] for r in rows if r[1] not in ("—", "")]
    if source_date not in ("—", "") and conf_date not in ("—", "") and source_date > conf_date:
        print(f"\n**Pipeline stop:** confluence/public stuck at {conf_date}; source has {source_date}.")
        if master_date not in ("—", "") and master_date < source_date:
            print(f"  → Master CSV also behind ({master_date}) — run legacy_cot + cot_tracked_backfill.")
        elif master_date == source_date and conf_date < source_date:
            print("  → Master current but confluence not rebuilt — run build_decision_table / cot_tracked_backfill.")
        elif master_date == conf_date and conf_date < source_date:
            print("  → Master and confluence aligned but behind source — run legacy_cot + full refresh chain.")
        cot3 = next((r[1] for r in rows if r[0] == "5. cot_3y (public)"), "—")
        if cot3 not in ("—", "") and cot3 < source_date:
            print(f"  → cot_3y_series also behind ({cot3}) — run run_cot_3y_series after master refresh.")


if __name__ == "__main__":
    main()
