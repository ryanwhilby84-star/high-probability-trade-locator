"""Browser-friendly dashboard JSON exports (slim history + split macro maps)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

OUT_PATH = Path("web-dashboard/public/data/confluence_history_latest.json")
MACRO_MAPS_PATH = Path("web-dashboard/public/data/macro_relationship_maps_latest.json")
DIST_CONFLUENCE_PATH = Path("web-dashboard/dist/data/confluence_history_latest.json")
DIST_MACRO_MAPS_PATH = Path("web-dashboard/dist/data/macro_relationship_maps_latest.json")

# Heavy nested blobs — kept only on latest COT-week rows (per market).
ENRICHMENT_KEYS = frozenset(
    {
        "institutional_context",
        "instrument_intel_context",
        "ui_pack",
        "expanding_history_context",
        "full_loaded_history_context",
        "intermarket_impulse_context",
        "cot_positioning_groups",
        "macro_audit",
        "positioning_interpretation",
        "market_environment_feed",
        "four_week_positioning_story",
        "flow_change_summary",
        "pressure_summary",
        "global_market_regime",
        "macro_transmission",
    }
)

# Lightweight fields kept on every row for scanner filters.
ALWAYS_KEEP_KEYS = frozenset(
    {
        "instrument_meta",
        "positioning_status",
        "cot_status_label",
        "data_status",
    }
)


def _record_enriched(
    row: dict[str, Any],
    latest_cot_report_date: str,
    latest_calendar_week: str,
) -> bool:
    cal = str(row.get("date") or "").strip()
    if latest_calendar_week and cal == latest_calendar_week:
        return True
    if not latest_cot_report_date:
        return False
    cot = str(row.get("cot_report_date") or "").strip()
    return bool(cot) and cot == latest_cot_report_date


def slim_record(row: dict[str, Any], *, enrich: bool) -> dict[str, Any]:
    if enrich or not isinstance(row, dict):
        return dict(row) if isinstance(row, dict) else row
    slim = {k: v for k, v in row.items() if k not in ENRICHMENT_KEYS}
    for k in ALWAYS_KEEP_KEYS:
        if k in row:
            slim[k] = row[k]
    return slim


def split_dashboard_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return (main confluence payload, macro_relationship_maps)."""
    latest = str(payload.get("latest_cot_report_date") or "").strip()
    records_in = payload.get("records")
    latest_cal = ""
    if isinstance(records_in, list):
        dates = [str(r.get("date") or "").strip() for r in records_in if isinstance(r, dict) and r.get("date")]
        latest_cal = max(dates) if dates else ""
    records: list[dict[str, Any]] = []
    if isinstance(records_in, list):
        for row in records_in:
            if not isinstance(row, dict):
                continue
            records.append(slim_record(row, enrich=_record_enriched(row, latest, latest_cal)))

    maps = payload.get("macro_relationship_maps")
    macro_maps = maps if isinstance(maps, dict) else {}

    main = {k: v for k, v in payload.items() if k != "macro_relationship_maps"}
    main["records"] = records
    return main, macro_maps


def _json_kwargs(*, compact: bool) -> dict[str, Any]:
    if compact:
        return {"ensure_ascii": False, "separators": (",", ":")}
    return {"ensure_ascii": False, "indent": 2}


def _macro_header(macro_maps: dict[str, Any]) -> dict[str, Any]:
    """Top-level freshness header for the macro maps file (best-effort)."""
    try:
        from hptl.macro import fred_client
        from hptl.macro.macro_audit import build_macro_audit

        audit = build_macro_audit(macro_maps)
        summary = audit.get("summary", {})
        log = fred_client.refresh_log()
        return {
            "macro_last_successful_refresh": summary.get("last_successful_refresh") or log.get("last_success"),
            "macro_last_failed_refresh": summary.get("last_failed_refresh") or log.get("last_failure"),
            "macro_coverage": {
                "total": summary.get("total"),
                "available": summary.get("available"),
                "live": summary.get("live"),
                "cached": summary.get("cached"),
                "stale": summary.get("stale"),
                "warning": summary.get("warning"),
                "missing": summary.get("missing"),
            },
        }
    except Exception:
        return {}


def _write_macro_audit(macro_maps: dict[str, Any]) -> None:
    """Write the macro audit JSON/MD (best-effort; never blocks the export)."""
    try:
        from hptl.macro.macro_audit import build_macro_audit, write_macro_audit

        write_macro_audit(build_macro_audit(macro_maps))
    except Exception:
        pass


def write_macro_maps_export(
    macro_maps: dict[str, Any],
    *,
    generated_at: str | None = None,
    latest_cot_report_date: str | None = None,
    maps_path: Path | None = None,
    compact: bool = True,
) -> Path:
    """Write only the macro relationship maps file (+ dist mirror + audit).

    Used by the standalone macro refresh job. Non-destructive: callers should pass
    already-merged maps (see ``macro_relationship_maps.build_all_macro_relationship_maps``).
    """
    maps_out = Path(maps_path or MACRO_MAPS_PATH)
    maps_out.parent.mkdir(parents=True, exist_ok=True)
    gen = generated_at or datetime.now(timezone.utc).isoformat()
    doc = {
        "generated_at": gen,
        "latest_cot_report_date": latest_cot_report_date,
        **_macro_header(macro_maps),
        "macro_relationship_maps": macro_maps,
    }
    text = json.dumps(doc, **_json_kwargs(compact=compact))
    maps_out.write_text(text, encoding="utf-8")
    if DIST_MACRO_MAPS_PATH.parent.exists() and DIST_MACRO_MAPS_PATH.resolve() != maps_out.resolve():
        DIST_MACRO_MAPS_PATH.write_text(text, encoding="utf-8")
    _write_macro_audit(macro_maps)
    return maps_out


def write_dashboard_exports(
    payload: dict[str, Any],
    *,
    out_path: Path | None = None,
    maps_path: Path | None = None,
    compact: bool = True,
) -> tuple[Path, Path]:
    """Write slim confluence JSON and separate macro relationship maps."""
    main, macro_maps = split_dashboard_payload(payload)
    out = Path(out_path or OUT_PATH)
    maps_out = Path(maps_path or MACRO_MAPS_PATH)
    out.parent.mkdir(parents=True, exist_ok=True)
    maps_out.parent.mkdir(parents=True, exist_ok=True)
    kwargs = _json_kwargs(compact=compact)
    out.write_text(json.dumps(main, **kwargs), encoding="utf-8")
    maps_out.write_text(
        json.dumps(
            {
                "generated_at": main.get("generated_at"),
                "latest_cot_report_date": main.get("latest_cot_report_date"),
                **_macro_header(macro_maps),
                "macro_relationship_maps": macro_maps,
            },
            **kwargs,
        ),
        encoding="utf-8",
    )
    _write_macro_audit(macro_maps)
    return out, maps_out


def sync_dist_exports(
    *,
    confluence_source: Path | None = None,
    maps_source: Path | None = None,
) -> list[Path]:
    """Copy canonical public JSON into ``dist/data`` for preview builds."""
    import shutil

    written: list[Path] = []
    pairs = (
        (Path(confluence_source or OUT_PATH), DIST_CONFLUENCE_PATH),
        (Path(maps_source or MACRO_MAPS_PATH), DIST_MACRO_MAPS_PATH),
    )
    for src, dest in pairs:
        if not src.exists():
            continue
        if dest.resolve() == src.resolve():
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        written.append(dest.resolve())
    return written
