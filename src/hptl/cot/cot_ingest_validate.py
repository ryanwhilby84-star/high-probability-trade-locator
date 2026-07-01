"""Post-ingest COT validation — Phase 2A."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from hptl.config import DATA_DIR
from hptl.confluence.build_decision_table import TARGET_MARKETS, tracked_master_csv_path
from hptl.cot.cot_failures import log_cot_failure

LEGACY_LATEST = DATA_DIR / "legacy_cot_latest.json"


@dataclass
class IngestValidationResult:
    ok: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    latest_stored_week: str | None = None
    previous_stored_week: str | None = None
    week_advanced: bool = False
    commercial_populated: bool = False
    noncommercial_populated: bool = False
    nonreportable_populated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "latest_stored_week": self.latest_stored_week,
            "previous_stored_week": self.previous_stored_week,
            "week_advanced": self.week_advanced,
            "commercial_populated": self.commercial_populated,
            "noncommercial_populated": self.noncommercial_populated,
            "nonreportable_populated": self.nonreportable_populated,
        }


def _max_week_in_csv(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, usecols=["date"], low_memory=False)
    except (OSError, ValueError, pd.errors.EmptyDataError):
        return None
    if df.empty:
        return None
    s = pd.to_datetime(df["date"], errors="coerce").dropna()
    if s.empty:
        return None
    return s.max().strftime("%Y-%m-%d")


def _positions_ok_on_week(path: Path, week: str) -> tuple[bool, bool, list[str]]:
    """Check commercial + noncommercial columns on a given week for TARGET_MARKETS."""
    errors: list[str] = []
    if not path.exists():
        return False, False, ["processed/master CSV missing"]
    try:
        df = pd.read_csv(path, low_memory=False)
    except (OSError, pd.errors.EmptyDataError) as exc:
        return False, False, [f"cannot read CSV: {exc}"]

    if "date" not in df.columns or "market" not in df.columns:
        return False, False, ["CSV missing date/market columns"]

    df["_d"] = pd.to_datetime(df["date"], errors="coerce")
    week_ts = pd.Timestamp(week)
    sub = df[(df["_d"] == week_ts) & (df["market"].isin(TARGET_MARKETS))]
    if sub.empty:
        return False, False, [f"no TARGET_MARKETS rows on week {week}"]

    comm_ok = "commercial_long" in sub.columns and sub["commercial_long"].notna().any()
    nc_ok = "noncommercial_long" in sub.columns and sub["noncommercial_long"].notna().any()
    if not comm_ok:
        errors.append("commercial positions not populated on latest week")
    if not nc_ok:
        errors.append("non-commercial positions not populated on latest week")
    return comm_ok, nc_ok, errors


def _nonreportable_ok(week: str) -> tuple[bool, list[str]]:
    """Legacy JSON carries nonreportable long/short for workstation groups."""
    warnings: list[str] = []
    if not LEGACY_LATEST.exists():
        warnings.append("legacy_cot_latest.json missing — nonreportable check skipped")
        return True, warnings
    try:
        doc = json.loads(LEGACY_LATEST.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        warnings.append(f"legacy JSON unreadable: {exc}")
        return False, warnings

    instruments = doc.get("instruments") or {}
    populated = 0
    checked = 0
    for market in TARGET_MARKETS[:12]:
        inst = instruments.get(market) or {}
        groups = inst.get("groups") or {}
        nr = groups.get("nonreportables") or {}
        weeks = nr.get("weeks") or []
        if not weeks:
            continue
        checked += 1
        latest = weeks[-1]
        if str(latest.get("report_date", ""))[:10] >= week[:10]:
            if latest.get("nonreportable_long") is not None or latest.get("long") is not None:
                populated += 1
    if checked == 0:
        warnings.append("no nonreportable group weeks in legacy JSON")
        return True, warnings
    if populated == 0:
        return False, ["nonreportable positions not populated in legacy JSON"]
    return True, warnings


def validate_post_ingest(
    *,
    processed_csv: Path | str | None,
    master_csv: Path | str | None = None,
    previous_week: str | None,
    expected_week: str | None,
    update_performed: bool,
    rows_added: int = 0,
) -> IngestValidationResult:
    """Verify ingestion produced a new week with populated positioning fields."""
    result = IngestValidationResult(ok=True)
    result.previous_stored_week = previous_week

    master_path = Path(master_csv) if master_csv else tracked_master_csv_path()
    proc_path = Path(processed_csv) if processed_csv else None

    result.latest_stored_week = _max_week_in_csv(master_path) or (
        _max_week_in_csv(proc_path) if proc_path else None
    )

    check_path = proc_path if proc_path and proc_path.exists() else master_path
    week = expected_week or result.latest_stored_week
    if week:
        comm, nc, pos_errors = _positions_ok_on_week(check_path, week)
        result.commercial_populated = comm
        result.noncommercial_populated = nc
        result.errors.extend(pos_errors)
        nr_ok, nr_warn = _nonreportable_ok(week)
        result.nonreportable_populated = nr_ok
        result.warnings.extend(nr_warn)
        if not nr_ok:
            result.errors.append("nonreportable positions not populated")

    if previous_week and result.latest_stored_week:
        result.week_advanced = result.latest_stored_week > previous_week
    elif result.latest_stored_week and expected_week:
        result.week_advanced = result.latest_stored_week >= expected_week[:10]

    if update_performed and rows_added == 0 and expected_week:
        if result.latest_stored_week and result.latest_stored_week < expected_week[:10]:
            result.errors.append(
                f"ingest expected new week {expected_week} but stored max is {result.latest_stored_week}"
            )
        elif not result.week_advanced and previous_week == result.latest_stored_week:
            result.errors.append("update performed but report week did not advance")

    if result.errors:
        result.ok = False
        log_cot_failure(
            failure_type="ingest_validation",
            source="post_ingest",
            error="; ".join(result.errors),
            detail=result.to_dict(),
        )
    return result
