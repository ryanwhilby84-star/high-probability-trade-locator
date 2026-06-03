"""Load positioning exclusively from ``legacy_cot_latest.json`` (Legacy Futures Only)."""
from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from hptl.markets.instrument_registry import cot_mapped_ids

PARSER_NAME = "hptl.cot.legacy_cot_loader"
DEFAULT_LATEST_PATH = Path("data/legacy_cot_latest.json")
PUBLIC_LATEST_PATH = Path("web-dashboard/public/data/legacy_cot_latest.json")

NC_LONG_COL = "Noncommercial Positions-Long (All)"
NC_SHORT_COL = "Noncommercial Positions-Short (All)"


def legacy_cot_latest_path() -> Path:
    if os.environ.get("HPTL_LEGACY_COT_LATEST_PATH", "").strip():
        return Path(os.environ["HPTL_LEGACY_COT_LATEST_PATH"].strip())
    if DEFAULT_LATEST_PATH.exists():
        return DEFAULT_LATEST_PATH
    return PUBLIC_LATEST_PATH


@lru_cache(maxsize=1)
def load_legacy_cot_document(path: str | None = None) -> dict[str, Any]:
    p = Path(path) if path else legacy_cot_latest_path()
    if not p.exists():
        return {"instruments": {}, "scoring_eligible_instruments": []}
    return json.loads(p.read_text(encoding="utf-8"))


def scoring_eligible_markets(doc: dict[str, Any] | None = None) -> list[str]:
    d = doc or load_legacy_cot_document()
    eligible = d.get("scoring_eligible_instruments") or []
    if eligible:
        return list(eligible)
    return [
        iid
        for iid in cot_mapped_ids()
        if (d.get("instruments") or {}).get(iid, {}).get("mapping_status") == "PASS"
    ]


def _week_rows_for_instrument(inst: dict[str, Any]) -> list[dict[str, Any]]:
    groups = inst.get("groups") or {}
    nc = {w["report_date"]: w for w in groups.get("noncommercials", {}).get("weeks") or []}
    comm = {w["report_date"]: w for w in groups.get("commercials", {}).get("weeks") or []}
    nrep = {w["report_date"]: w for w in groups.get("nonreportables", {}).get("weeks") or []}
    dates = sorted(set(nc) | set(comm) | set(nrep))
    rows: list[dict[str, Any]] = []
    for d in dates:
        nc_w = nc.get(d) or {}
        comm_w = comm.get(d) or {}
        nrep_w = nrep.get(d) or {}
        rows.append(
            {
                "report_date": d,
                "instrument_id": inst.get("instrument_id"),
                "mapping_status": inst.get("mapping_status"),
                "selected_cftc_code": inst.get("selected_cftc_code") or nc_w.get("cftc_market_code"),
                "selected_market_name": inst.get("selected_market_name") or nc_w.get("market_name"),
                "selected_report_type": inst.get("selected_report_type") or "legacy_futures_only",
                "nc_long": nc_w.get("long"),
                "nc_short": nc_w.get("short"),
                "nc_net": nc_w.get("net"),
                "nc_long_week_change": nc_w.get("long_week_change"),
                "nc_short_week_change": nc_w.get("short_week_change"),
                "nc_net_week_change": nc_w.get("net_week_change"),
                "comm_long": comm_w.get("long"),
                "comm_short": comm_w.get("short"),
                "comm_net": comm_w.get("net"),
                "nrept_long": nrep_w.get("long"),
                "nrept_short": nrep_w.get("short"),
                "nrept_net": nrep_w.get("net"),
                "open_interest": nc_w.get("open_interest") or comm_w.get("open_interest"),
                "raw_source_file": nc_w.get("raw_source_file"),
                "raw_source_row": nc_w.get("raw_source_row"),
            }
        )
    return rows


def load_legacy_positioning_decision_rows(
    *,
    path: str | None = None,
    eligible_only: bool = True,
) -> pd.DataFrame:
    """Decision-table COT frame: one row per (market, cot_report_date) from Legacy NC headline."""
    doc = load_legacy_cot_document(path)
    markets = scoring_eligible_markets(doc) if eligible_only else list((doc.get("instruments") or {}).keys())
    frames: list[pd.DataFrame] = []
    for iid in markets:
        inst = (doc.get("instruments") or {}).get(iid)
        if not inst or inst.get("mapping_status") != "PASS":
            continue
        weeks = _week_rows_for_instrument(inst)
        if not weeks:
            continue
        x = pd.DataFrame(weeks)
        x["market"] = iid
        x["cot_report_date"] = pd.to_datetime(x["report_date"], errors="coerce").dt.normalize()
        x["raw_cftc_market_name"] = x["selected_market_name"]
        x["long_value"] = pd.to_numeric(x["nc_long"], errors="coerce")
        x["short_value"] = pd.to_numeric(x["nc_short"], errors="coerce")
        x["net_value"] = pd.to_numeric(x["nc_net"], errors="coerce")
        x["long_col_used"] = NC_LONG_COL
        x["short_col_used"] = NC_SHORT_COL
        x["position_source_family"] = "legacy_noncommercial"
        x["positioning_source"] = "legacy_cot_latest.json"
        x["trader_group_used"] = f"{NC_LONG_COL} / {NC_SHORT_COL}"
        x["missing_reason"] = pd.NA
        zero_pair = x["long_value"].eq(0) & x["short_value"].eq(0)
        x.loc[zero_pair, "net_value"] = pd.NA
        x.loc[zero_pair, "missing_reason"] = "long and short are both 0 in legacy row"
        x["quality_score"] = (
            x["long_value"].notna().astype(int) * 5
            + x["short_value"].notna().astype(int) * 5
            + (~zero_pair).astype(int) * 5
            + x["net_value"].notna().astype(int) * 5
        )
        frames.append(x.drop(columns=["report_date"], errors="ignore"))

    if not frames:
        return pd.DataFrame()

    cot = pd.concat(frames, ignore_index=True)
    cot = cot.sort_values(["market", "cot_report_date"]).drop_duplicates(
        ["market", "cot_report_date"], keep="last"
    )
    return cot.reset_index(drop=True)


def load_legacy_trader_positioning_by_market_date(
    *,
    path: str | None = None,
) -> pd.DataFrame:
    """Trader-group columns for merge — Legacy NC / Commercial / Non-reportable only."""
    cot = load_legacy_positioning_decision_rows(path=path, eligible_only=True)
    if cot.empty:
        return pd.DataFrame()

    out = pd.DataFrame()
    out["market"] = cot["market"]
    out["cot_report_date"] = cot["cot_report_date"]
    out["raw_cftc_market_name"] = cot["raw_cftc_market_name"]
    # Aliases: mm_* = non-commercial for downstream trader_groups_payload (commodity profile).
    out["mm_long"] = pd.to_numeric(cot["nc_long"], errors="coerce")
    out["mm_short"] = pd.to_numeric(cot["nc_short"], errors="coerce")
    out["mm_net"] = pd.to_numeric(cot["nc_net"], errors="coerce")
    out["comm_long"] = pd.to_numeric(cot["comm_long"], errors="coerce")
    out["comm_short"] = pd.to_numeric(cot["comm_short"], errors="coerce")
    out["comm_net"] = pd.to_numeric(cot["comm_net"], errors="coerce")
    out["nrept_long"] = pd.to_numeric(cot["nrept_long"], errors="coerce")
    out["nrept_short"] = pd.to_numeric(cot["nrept_short"], errors="coerce")
    out["nrept_net"] = pd.to_numeric(cot["nrept_net"], errors="coerce")
    out["open_interest"] = pd.to_numeric(cot["open_interest"], errors="coerce")
    if "nc_long" in cot.columns:
        oi = out["open_interest"]
        out["mm_pct_long"] = 100.0 * out["mm_long"] / oi.where(oi > 0)
        out["mm_pct_short"] = 100.0 * out["mm_short"] / oi.where(oi > 0)
        out["comm_pct_long"] = 100.0 * out["comm_long"] / oi.where(oi > 0)
        out["comm_pct_short"] = 100.0 * out["comm_short"] / oi.where(oi > 0)
        out["nrept_pct_long"] = 100.0 * out["nrept_long"] / oi.where(oi > 0)
        out["nrept_pct_short"] = 100.0 * out["nrept_short"] / oi.where(oi > 0)
    return out


def legacy_trader_groups_payload(row: pd.Series | dict[str, Any]) -> dict[str, Any]:
    """Dashboard ``cot_positioning_groups`` — Legacy three cohorts only (commodity profile)."""
    if isinstance(row, dict):
        row = pd.Series(row)

    def _pack(prefix: str, label: str, interpretation: str) -> dict[str, Any]:
        long_v = _num(row.get(f"{prefix}_long"))
        short_v = _num(row.get(f"{prefix}_short"))
        net_v = _num(row.get(f"{prefix}_net"))
        if net_v is None and long_v is not None and short_v is not None:
            net_v = long_v - short_v
        return {
            "label": label,
            "interpretation": interpretation,
            "available": long_v is not None or short_v is not None,
            "long": long_v,
            "short": short_v,
            "net": net_v,
            "pct_long": _num(row.get(f"{prefix}_pct_long")),
            "pct_short": _num(row.get(f"{prefix}_pct_short")),
        }

    oi = _num(row.get("open_interest"))
    return {
        "profile": "legacy",
        "positioning_source": "legacy_cot_latest.json",
        "open_interest": oi,
        "managed_money": _pack("mm", "Non-Commercial", "Legacy non-commercial / speculative"),
        "commercial": _pack("comm", "Commercial", "Legacy commercial hedgers"),
        "nonreportable": _pack("nrept", "Non-Reportable", "Legacy non-reportable / retail proxy"),
    }


def _num(v: Any) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if pd.notna(f) else None
