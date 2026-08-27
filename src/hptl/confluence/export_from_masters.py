"""Fast confluence catch-up from local master outputs (no full history rebuild)."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from hptl.confluence.dashboard_export import OUT_PATH, sync_dist_exports
from hptl.config import PROJECT_ROOT, PROCESSED_DIR
from hptl.cot.report_dates import get_latest_local_report_date


@dataclass
class DriverAuditRow:
    name: str
    path: str
    latest_date: str
    status: str
    note: str = ""


@dataclass
class CatchUpResult:
    local_latest: str
    confluence_before: str
    confluence_after: str
    weeks_built: list[str]
    drivers_included: list[str] = field(default_factory=list)
    drivers_excluded: list[str] = field(default_factory=list)
    records_exported: int = 0
    export_path: str = ""
    error: str | None = None

    @property
    def markets_exported(self) -> int:
        return self.records_exported


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _legacy_json_max(path: Path) -> str:
    doc = _read_json(path)
    maxd = ""
    for block in (doc.get("instruments") or {}).values():
        weeks = ((block.get("groups") or {}).get("noncommercials") or {}).get("weeks") or []
        if weeks:
            d = str(weeks[-1].get("report_date") or "")[:10]
            if d and d > maxd:
                maxd = d
    return maxd or "—"


def _csv_max(path: Path, col: str) -> str:
    if not path.exists():
        return "—"
    try:
        df = pd.read_csv(path, usecols=[col], low_memory=False)
        return str(df[col].astype(str).str[:10].max())
    except (OSError, ValueError, KeyError):
        return "—"


def _json_field_date(doc: dict[str, Any], *keys: str) -> str:
    cur: Any = doc
    for k in keys:
        if not isinstance(cur, dict):
            return "—"
        cur = cur.get(k)
    if cur is None:
        return "—"
    return str(cur)[:10]


def audit_local_drivers() -> list[DriverAuditRow]:
    """Audit latest available date for each local driver dataset."""
    rows: list[DriverAuditRow] = []

    def add(name: str, rel: str, latest: str, *, note: str = "") -> None:
        path = PROJECT_ROOT / rel
        status = "OK" if latest not in ("—", "") else ("MISSING" if not path.exists() else "STALE")
        rows.append(DriverAuditRow(name=name, path=rel, latest_date=latest, status=status, note=note))

    master = PROCESSED_DIR / "cot_tracked_master_normalized.csv"
    add("cot_master", str(master.relative_to(PROJECT_ROOT)), _csv_max(master, "cot_report_date"))

    legacy = PROJECT_ROOT / "data" / "legacy_cot_latest.json"
    add("legacy_cot", "data/legacy_cot_latest.json", _legacy_json_max(legacy))

    prices = _read_json(PROJECT_ROOT / "data" / "processed" / "prices_latest.json")
    px_summary = prices.get("summary") or {}
    add(
        "prices",
        "data/processed/prices_latest.json",
        str(px_summary.get("latest_daily_date") or px_summary.get("as_of") or prices.get("generated_at") or "—")[:10],
    )

    val = _read_json(PROJECT_ROOT / "data" / "valuation_latest.json")
    add("valuation_export", "data/valuation_latest.json", str(val.get("generated_at") or "—")[:10])

    loc = _read_json(PROJECT_ROOT / "data" / "location_latest.json")
    add("location_export", "data/location_latest.json", str(loc.get("generated_at") or "—")[:10])

    sea = _read_json(PROJECT_ROOT / "data" / "seasonality_latest.json")
    add("seasonality_export", "data/seasonality_latest.json", str(sea.get("generated_at") or "—")[:10])

    macro = _read_json(PROJECT_ROOT / "data" / "processed" / "macro_history_latest.json")
    add(
        "macro_history",
        "data/processed/macro_history_latest.json",
        _json_field_date(macro, "latest_snapshot_date") or str(macro.get("generated_at") or "—")[:10],
    )

    fx_v3 = _read_json(PROJECT_ROOT / "data" / "fx_valuation_v3_audit.json")
    add("fx_v3_audit", "data/fx_valuation_v3_audit.json", str(fx_v3.get("generated_at") or "—")[:10])

    inst = PROCESSED_DIR / "institutional_regime_state.json"
    add(
        "institutional_regime",
        str(inst.relative_to(PROJECT_ROOT)),
        str(_read_json(inst).get("generated_at") or "—")[:10],
    )

    conf = _read_json(OUT_PATH)
    add(
        "confluence_export",
       str(OUT_PATH.resolve().relative_to(PROJECT_ROOT.resolve())),
        str(conf.get("latest_cot_report_date") or "—")[:10],
        note=f"{len(conf.get('records') or [])} records",
    )

    return rows


def _confluence_latest(path: Path | None = None) -> str:
    doc = _read_json(path or OUT_PATH)
    return str(doc.get("latest_cot_report_date") or "—")[:10]


def _missing_cot_weeks(*, export_latest: str, local_latest: str) -> list[str]:
    from hptl.confluence.build_decision_table import _load_cot_history

    cot = _load_cot_history()
    if cot.empty or "cot_report_date" not in cot.columns:
        return []
    dates = sorted(cot["cot_report_date"].dropna().dt.strftime("%Y-%m-%d").unique())
    if not export_latest or export_latest == "—":
        return list(dates)
    return [d for d in dates if d > export_latest[:10] and d <= local_latest[:10]]


def regenerate_valuation_from_local() -> dict[str, Path]:
    """Force valuation_latest.json from local caches only (no live network)."""
    os.environ.setdefault("HPTL_SKIP_LIVE_FEEDS", "1")
    from hptl.valuation.export import write_valuation_exports

    return write_valuation_exports()


def catch_up_confluence_export(
    *,
    cot_feed_meta: dict[str, Any] | None = None,
    weeks: list[str] | None = None,
) -> CatchUpResult:
    """Append missing COT week rows to existing confluence export (no full history rebuild)."""
    os.environ.setdefault("HPTL_DISABLE_WATCHDOG", "1")
    os.environ.setdefault("HPTL_SKIP_LIVE_FEEDS", "1")
    os.environ.setdefault("HPTL_SKIP_VALUATION", "1")

    local_ts = get_latest_local_report_date()
    local_latest = str(local_ts)[:10] if local_ts is not None and not pd.isna(local_ts) else "—"
    before = _confluence_latest()

    existing: dict[str, Any] = _read_json(OUT_PATH)
    export_latest = before

    if local_latest == "—":
        return CatchUpResult(
            local_latest=local_latest,
            confluence_before=before,
            confluence_after=before,
            weeks_built=[],
            error="Local COT master has no latest report date.",
        )

    to_build = weeks or _missing_cot_weeks(export_latest=export_latest, local_latest=local_latest)
    if not to_build and export_latest >= local_latest:
        from hptl.confluence.repair_missing_markets import refresh_latest_confluence_validation_fields

        refresh_latest_confluence_validation_fields()
        sync_dist_exports()
        after = _confluence_latest()
        recs = len(_read_json(OUT_PATH).get("records") or [])
        return CatchUpResult(
            local_latest=local_latest,
            confluence_before=before,
            confluence_after=after,
            weeks_built=[],
            records_exported=recs,
            export_path=str(OUT_PATH.resolve()),
        )

    if not to_build:
        return CatchUpResult(
            local_latest=local_latest,
            confluence_before=before,
            confluence_after=before,
            weeks_built=[],
            error=f"No master weeks between export ({export_latest}) and local ({local_latest}).",
        )

    os.environ["HPTL_CONFLUENCE_ONLY_DATES"] = ",".join(to_build)
    os.environ["HPTL_CONFLUENCE_INCREMENTAL"] = "1"

    from hptl.confluence.build_decision_table import run as run_confluence_build

    out = run_confluence_build(cot_feed_meta=cot_feed_meta)
    sync_dist_exports(confluence_source=out)

    after = _confluence_latest(out)
    recs = len(_read_json(out).get("records") or [])
    return CatchUpResult(
        local_latest=local_latest,
        confluence_before=before,
        confluence_after=after,
        weeks_built=to_build,
        records_exported=recs,
        export_path=str(out.resolve()),
    )


def run_master_rebuild(*, skip_valuation: bool = False) -> CatchUpResult:
    """Audit drivers, regenerate valuation, catch up confluence export."""
    audit = audit_local_drivers()
    included: list[str] = []
    excluded: list[str] = []

    for row in audit:
        line = f"{row.name} ({row.latest_date})"
        if row.status == "OK":
            included.append(line)
        else:
            excluded.append(f"{line} — {row.status}: {row.note or row.path}")

    if not skip_valuation:
        try:
            regenerate_valuation_from_local()
            included.append("valuation_latest.json (regenerated)")
        except Exception as exc:
            excluded.append(f"valuation_latest.json — FAILED: {exc}")

    result = catch_up_confluence_export()
    result.drivers_included = included
    result.drivers_excluded = excluded
    return result


def print_summary(result: CatchUpResult) -> None:
    print(f"latest local date: {result.local_latest}")
    print(f"latest confluence date (before): {result.confluence_before}")
    print(f"latest confluence date (after): {result.confluence_after}")
    print(f"weeks built: {', '.join(result.weeks_built) or '—'}")
    print(f"drivers included: {len(result.drivers_included)}")
    for d in result.drivers_included:
        print(f"  + {d}")
    print(f"drivers excluded: {len(result.drivers_excluded)}")
    for d in result.drivers_excluded:
        print(f"  - {d}")
    print(f"records exported: {result.records_exported}")
    print(f"export path: {result.export_path or OUT_PATH}")
    if result.error:
        print(f"error: {result.error}")
    else:
        print("completion: OK")


def main() -> int:
    os.environ.setdefault("HPTL_DISABLE_WATCHDOG", "1")
    os.environ.setdefault("HPTL_SKIP_LIVE_FEEDS", "1")
    result = run_master_rebuild()
    print_summary(result)
    return 1 if result.error else 0


if __name__ == "__main__":
    raise SystemExit(main())
