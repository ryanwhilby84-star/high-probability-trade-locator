"""COT coverage audit — one auditable row per registry instrument.

Exports web-dashboard/public/data/cot_coverage_audit_latest.json so the dashboard can prove,
for every instrument, exactly what COT data backs it (direct / leg-derived / proxy / macro / none),
how many rows are valid vs invalid, and why anything is excluded.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from hptl.cot.canonical_entities import (
    CANONICAL_COT_ENTITIES,
    COT_STATUS_BROKEN,
    COT_STATUS_DIRECT,
    COT_STATUS_INVALID,
    COT_STATUS_LEG,
    COT_STATUS_MACRO,
    COT_STATUS_NONE,
    COT_STATUS_PROXY,
    resolve_cot_status,
)
from hptl.cot.data_integrity import frame_integrity_summary, validate_cot_frame
from hptl.markets.instrument_registry import all_instrument_ids, load_registry

COT_MASTER_PATH = Path("data/processed/cot_tracked_master_normalized.csv")
AUDIT_JSON_PATH = Path("data/cot_coverage_audit_latest.json")
PUBLIC_AUDIT_PATH = Path("web-dashboard/public/data/cot_coverage_audit_latest.json")

# data_quality_status vocabulary
DQ_CLEAN = "clean"
DQ_INCOMPLETE = "incomplete"
DQ_DUPLICATE = "duplicate"
DQ_INVALID = "invalid_rows_detected"
DQ_STALE = "stale"
DQ_MISSING = "missing"
DQ_BROKEN = "broken"


def _load_cot_master() -> pd.DataFrame:
    if not COT_MASTER_PATH.exists():
        return pd.DataFrame()
    return pd.read_csv(COT_MASTER_PATH, low_memory=False)


def build_cot_coverage_audit() -> dict[str, Any]:
    raw = _load_cot_master()
    validated = validate_cot_frame(raw)
    integ = frame_integrity_summary(validated)
    per_market = integ.get("by_market", {})

    # Canonical entities that currently have >= 1 integrity-valid row.
    valid_entities: set[str] = {
        m for m, s in per_market.items() if s.get("valid_rows", 0) > 0
    }

    # Global latest valid week (for staleness detection).
    global_latest = ""
    for s in per_market.values():
        lv = s.get("latest_valid_cot_week")
        if lv and lv > global_latest:
            global_latest = lv

    reg = load_registry()
    instruments: list[dict[str, Any]] = []

    for iid in all_instrument_ids():
        spec = reg[iid]
        res = resolve_cot_status(spec, valid_entities=valid_entities)

        # Which canonical entity (if any) owns this instrument's row stats.
        owner = res.direct_cot_market
        market_stats = per_market.get(owner or iid, {})
        valid_rows = int(market_stats.get("valid_rows", 0))
        invalid_rows = int(market_stats.get("invalid_rows", 0))
        latest_valid = market_stats.get("latest_valid_cot_week")

        duplicate_of: str | None = None
        if res.cot_status == COT_STATUS_PROXY and res.proxy_cot_markets:
            # Proxy instruments would otherwise reuse a canonical table → flag the source.
            duplicate_of = res.proxy_cot_markets[0]

        dq, exclusion = _data_quality(
            status=res.cot_status,
            valid_rows=valid_rows,
            invalid_rows=invalid_rows,
            latest_valid=latest_valid,
            global_latest=global_latest,
            owner=owner,
            raw_has_market=bool(owner) and owner in set(raw.get("market", pd.Series(dtype=object)).unique()) if not raw.empty else False,
        )

        instruments.append(
            {
                "instrument_id": spec.id,
                "display_name": spec.display_name,
                "asset_class": spec.asset_class,
                "cot_status": res.cot_status,
                "direct_cot_market": res.direct_cot_market,
                "leg_cot_markets": res.leg_cot_markets,
                "proxy_cot_markets": res.proxy_cot_markets,
                "latest_valid_cot_week": latest_valid,
                "valid_rows_count": valid_rows,
                "invalid_rows_count": invalid_rows,
                "duplicate_of": duplicate_of,
                "data_quality_status": dq,
                "exclusion_reason": exclusion,
                "note": res.note,
            }
        )

    summary = _summary(instruments, integ)
    return {
        "generated_at": pd.Timestamp.now("UTC").isoformat(),
        "global_latest_valid_cot_week": global_latest,
        "canonical_entities_total": len(CANONICAL_COT_ENTITIES),
        "canonical_entities_with_valid_rows": sorted(valid_entities),
        "integrity": integ,
        "summary": summary,
        "instruments": instruments,
    }


def _data_quality(
    *,
    status: str,
    valid_rows: int,
    invalid_rows: int,
    latest_valid: str | None,
    global_latest: str,
    owner: str | None,
    raw_has_market: bool,
) -> tuple[str, str | None]:
    if status == COT_STATUS_DIRECT:
        if valid_rows == 0:
            return DQ_BROKEN, "no_valid_canonical_cot_rows"
        if latest_valid and global_latest and latest_valid < global_latest:
            return DQ_STALE, f"latest_valid_week_{latest_valid}_behind_{global_latest}"
        if invalid_rows > 0:
            return DQ_INVALID, None  # usable, but flag that some rows were quarantined
        return DQ_CLEAN, None
    if status == COT_STATUS_LEG:
        return DQ_CLEAN, None
    if status == COT_STATUS_PROXY:
        return DQ_DUPLICATE, None  # reuses a canonical table; not its own COT
    if status == COT_STATUS_BROKEN:
        if owner and not raw_has_market:
            return DQ_MISSING, "canonical_entity_absent_from_cot_master"
        return DQ_BROKEN, "mapping_declared_but_no_valid_rows"
    if status == COT_STATUS_INVALID:
        return DQ_INVALID, "all_rows_failed_integrity"
    if status == COT_STATUS_MACRO:
        return DQ_INCOMPLETE, "no_direct_or_leg_cot_macro_only"
    # no_cot_available
    return DQ_MISSING, "no_cot_mapping"


def _summary(instruments: list[dict[str, Any]], integ: dict[str, Any]) -> dict[str, Any]:
    def count_status(s: str) -> int:
        return sum(1 for x in instruments if x["cot_status"] == s)

    def count_dq(s: str) -> int:
        return sum(1 for x in instruments if x["data_quality_status"] == s)

    duplicates = [x for x in instruments if x["duplicate_of"]]
    unresolved = [
        {"instrument_id": x["instrument_id"], "cot_status": x["cot_status"], "reason": x["exclusion_reason"]}
        for x in instruments
        if x["cot_status"] in {COT_STATUS_BROKEN, COT_STATUS_INVALID}
    ]

    return {
        "instruments_total": len(instruments),
        "direct_cot": count_status(COT_STATUS_DIRECT),
        "leg_derived_cot": count_status(COT_STATUS_LEG),
        "proxy_cot": count_status(COT_STATUS_PROXY),
        "macro_only": count_status(COT_STATUS_MACRO),
        "no_cot_available": count_status(COT_STATUS_NONE),
        "broken_mapping": count_status(COT_STATUS_BROKEN),
        "invalid_data": count_status(COT_STATUS_INVALID),
        "data_quality": {
            "clean": count_dq(DQ_CLEAN),
            "incomplete": count_dq(DQ_INCOMPLETE),
            "duplicate": count_dq(DQ_DUPLICATE),
            "invalid_rows_detected": count_dq(DQ_INVALID),
            "stale": count_dq(DQ_STALE),
            "missing": count_dq(DQ_MISSING),
            "broken": count_dq(DQ_BROKEN),
        },
        "invalid_cot_rows_detected": integ.get("invalid_rows", 0),
        "duplicate_mappings_flagged": len(duplicates),
        "top_unresolved_mapping_issues": unresolved[:20],
    }


def write_cot_coverage_audit(
    payload: dict[str, Any],
    *,
    path: Path | None = None,
    public_path: Path | None = None,
) -> Path:
    out = path or AUDIT_JSON_PATH
    pub = public_path or PUBLIC_AUDIT_PATH
    out.parent.mkdir(parents=True, exist_ok=True)
    pub.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    out.write_text(text, encoding="utf-8")
    pub.write_text(text, encoding="utf-8")
    return out
