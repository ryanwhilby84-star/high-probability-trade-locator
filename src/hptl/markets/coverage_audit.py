"""Instrument data coverage audit — COT, macro, attention eligibility."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from hptl.context.attention_engine import (
    PRIORITY_DEVELOPING,
    PRIORITY_HIGH,
    PRIORITY_LOW,
    PRIORITY_WATCHLIST,
    aggregate_priority_markets,
)
from hptl.markets.instrument_registry import (
    LEGACY_COT_MARKETS,
    InstrumentSpec,
    all_instrument_ids,
    get_instrument,
    load_registry,
)

AUDIT_JSON_PATH = Path("data/instrument_coverage_audit.json")
PUBLIC_AUDIT_PATH = Path("web-dashboard/public/data/instrument_coverage_audit.json")

# Instruments expected to have direct CFTC pipeline rows (registry + aliases).
EXPECTED_DIRECT_COT: frozenset[str] = frozenset(
    {
        "Gold",
        "Silver",
        "Copper / HG",
        "Platinum",
        "Palladium",
        "Crude Oil / CL",
        "Natural Gas / NG",
        "Wheat",
        "Corn",
        "Soybeans",
        "Sugar",
        "Coffee",
        "Cocoa",
        "NASDAQ / NQ",
        "S&P 500 / ES",
        "Dow / YM",
        "Euro FX / 6E",
        "British Pound / 6B",
        "Japanese Yen / 6J",
        "Swiss Franc / 6S",
        "Australian Dollar / 6A",
        "Canadian Dollar / 6C",
        "NZ Dollar / 6N",
    }
)

# OANDA display names → direct COT target or proxy explanation.
EXPECTED_COT_ALIASES: dict[str, str] = {
    "US Nas 100": "NASDAQ / NQ (proxy)",
    "US SPX 500": "S&P 500 / ES (proxy)",
    "US Wall St 30": "Dow / YM (proxy)",
    "West Texas Oil": "Crude Oil / CL (proxy)",
    "Copper": "Copper / HG (proxy)",
    "US Russ 2000": "no CFTC mini-Russell in current fut_fin feed",
    "Brent Crude Oil": "ICE Brent disaggregated exists but not mapped in pipeline",
}


def _cot_resolved(rec: dict[str, Any]) -> bool:
    bias = str(rec.get("cot_bias") or "").strip().upper()
    if not bias or bias == "N/A":
        return False
    reason = str(rec.get("missing_reason") or "")
    if "no mapped raw COT" in reason:
        return False
    return True


_COT_MASTER_CACHE: dict[str, Any] = {"mtime": None, "df": None}


def _load_cot_master() -> pd.DataFrame:
    """Load the normalized COT master, cached by file mtime.

    data_status_for_record() is called once per confluence record (tens of thousands per
    full build). Re-reading this CSV every call was the dominant build cost / apparent hang.
    """
    path = Path("data/processed/cot_tracked_master_normalized.csv")
    if not path.exists():
        return pd.DataFrame()
    mtime = path.stat().st_mtime
    if _COT_MASTER_CACHE["mtime"] == mtime and _COT_MASTER_CACHE["df"] is not None:
        return _COT_MASTER_CACHE["df"]
    df = pd.read_csv(path, low_memory=False)
    _COT_MASTER_CACHE["mtime"] = mtime
    _COT_MASTER_CACHE["df"] = df
    return df


def _cot_stats_for_market(cot: pd.DataFrame, market_id: str, proxy_of: str | None) -> tuple[int, str | None]:
    """Return (row_count, latest_cot_date) for market or proxy target."""
    targets = [market_id]
    if proxy_of:
        targets.append(proxy_of)
    if cot.empty or "market" not in cot.columns:
        return 0, None
    sub = cot[cot["market"].isin(targets)]
    if sub.empty:
        return 0, None
    dates = pd.to_datetime(sub.get("cot_report_date"), errors="coerce").dropna()
    if dates.empty:
        return len(sub), None
    return len(sub), dates.max().strftime("%Y-%m-%d")


def _macro_transmission_ok(rec: dict[str, Any]) -> bool:
    tx = rec.get("macro_transmission")
    if not isinstance(tx, dict):
        tx = (rec.get("institutional_context") or {}).get("macro_transmission")
    return isinstance(tx, dict) and tx.get("available") is True


def _macro_generic_only(rec: dict[str, Any]) -> bool:
    tx = rec.get("macro_transmission") or (rec.get("institutional_context") or {}).get("macro_transmission")
    if not isinstance(tx, dict):
        return True
    return tx.get("transmission_mode") == "generic_rates_only"


def explain_attention_eligibility(rec: dict[str, Any]) -> tuple[bool, str]:
    """Whether instrument can appear on priority board + reason."""
    inst = rec.get("institutional_context") or {}
    att = inst.get("attention")
    if not att:
        if not _macro_transmission_ok(rec) and not _cot_resolved(rec):
            return False, "no_attention_layer_and_no_macro"
        if not _macro_transmission_ok(rec):
            return False, "no_attention_layer_no_macro_transmission"
        return False, "no_attention_layer"

    tier = att.get("priority_tier", PRIORITY_LOW)
    score = float(att.get("priority_score") or 0)
    mode = inst.get("data_mode")

    if tier == PRIORITY_LOW:
        if mode == "macro_only":
            return False, f"macro_only_priority_low_score_{score:.0f}"
        return False, f"priority_tier_low_score_{score:.0f}"

    if mode == "macro_only" and tier == PRIORITY_HIGH and score < 42:
        return True, f"macro_only_capped_to_developing_score_{score:.0f}"

    if tier in {PRIORITY_HIGH, PRIORITY_DEVELOPING, PRIORITY_WATCHLIST}:
        return True, f"eligible_{tier}_score_{score:.0f}"

    return False, f"not_actionable_{tier}"


def classify_data_status(
    *,
    spec: InstrumentSpec,
    rec: dict[str, Any] | None,
    cot_rows: int,
    cot_resolved: bool,
    macro_ok: bool,
    macro_generic: bool,
) -> str:
    if spec.id in EXPECTED_DIRECT_COT and not cot_resolved and cot_rows == 0:
        if spec.has_cot_mapping:
            return "broken_mapping"
        return "cot_mapping_missing"

    if spec.id in EXPECTED_DIRECT_COT and spec.has_cot_mapping and cot_rows > 0 and not cot_resolved:
        return "broken_mapping"

    if spec.id in EXPECTED_DIRECT_COT and spec.has_cot_mapping and cot_resolved and macro_ok and not macro_generic:
        return "complete"

    if spec.id in EXPECTED_DIRECT_COT and spec.has_cot_mapping and cot_resolved:
        if macro_generic:
            return "complete" if macro_ok else "cot_missing"
        return "complete" if macro_ok else "macro_only"

    if spec.cot_proxy_of and not cot_resolved:
        if cot_rows > 0:
            return "proxy_required"
        return "proxy_required"

    if not cot_resolved and macro_ok and not macro_generic:
        return "macro_only"

    if not cot_resolved and macro_ok and macro_generic:
        return "macro_only"

    if not cot_resolved and not macro_ok:
        if spec.cot_proxy_of:
            return "proxy_required"
        if spec.has_cot_mapping:
            return "cot_missing"
        return "no_data"

    if cot_resolved and not macro_ok:
        return "cot_missing"

    return "no_data"


def audit_instrument(
    spec: InstrumentSpec,
    *,
    cot: pd.DataFrame,
    week_rec: dict[str, Any] | None,
    latest_calendar_week: str,
) -> dict[str, Any]:
    proxy = spec.cot_proxy_of
    cot_rows, latest_cot = _cot_stats_for_market(cot, spec.id, proxy)
    cot_resolved = _cot_resolved(week_rec) if week_rec else False
    macro_ok = _macro_transmission_ok(week_rec) if week_rec else False
    macro_generic = _macro_generic_only(week_rec) if week_rec else True

    if week_rec and cot_resolved and not latest_cot:
        latest_cot = str(week_rec.get("cot_report_date") or week_rec.get("latest_report_date") or "") or None

    status = classify_data_status(
        spec=spec,
        rec=week_rec,
        cot_rows=cot_rows,
        cot_resolved=cot_resolved,
        macro_ok=macro_ok,
        macro_generic=macro_generic,
    )

    att_ok, att_reason = explain_attention_eligibility(week_rec) if week_rec else (False, "no_week_record")

    expected_note = EXPECTED_COT_ALIASES.get(spec.id)
    if spec.id in EXPECTED_DIRECT_COT and status in {"broken_mapping", "cot_mapping_missing"}:
        expected_note = "Expected direct COT in pipeline — check MARKET_ALIASES / contracts.py"

    return {
        "instrument_id": spec.id,
        "display_name": spec.display_name,
        "asset_class": spec.asset_class,
        "subgroup": spec.subgroup,
        "has_cot_mapping": spec.has_cot_mapping,
        "cot_market_code": spec.cot_market_code,
        "cot_report_type": spec.cot_report_type,
        "cot_proxy_of": proxy,
        "latest_cot_date_available": latest_cot,
        "cot_rows_found": cot_rows,
        "cot_resolved_latest_week": cot_resolved,
        "has_macro_transmission": macro_ok,
        "macro_transmission_generic_only": macro_generic,
        "macro_driver_profile": spec.macro_driver_profile,
        "has_price_proxy": bool(proxy),
        "attention_eligible": att_ok,
        "reason_if_not_attention_eligible": None if att_ok else att_reason,
        "data_status": status,
        "expected_cot_note": expected_note,
        "latest_calendar_week": latest_calendar_week,
    }


def run_coverage_audit(
    records: list[dict[str, Any]] | None = None,
    *,
    latest_calendar_week: str | None = None,
) -> dict[str, Any]:
    """Build full coverage audit from confluence records + COT master."""
    cot = _load_cot_master()
    reg = load_registry()

    if records:
        dates = [str(r.get("date") or "") for r in records if r.get("date")]
        latest_calendar_week = latest_calendar_week or (max(dates) if dates else "")
        week_rows = [r for r in records if str(r.get("date") or "") == latest_calendar_week]
    else:
        week_rows = []
        latest_calendar_week = latest_calendar_week or ""

    by_market = {str(r.get("market")): r for r in week_rows}
    instruments: list[dict[str, Any]] = []

    for iid in all_instrument_ids():
        spec = reg[iid]
        row = audit_instrument(
            spec,
            cot=cot,
            week_rec=by_market.get(iid),
            latest_calendar_week=latest_calendar_week or "",
        )
        instruments.append(row)

    priority_debug = _priority_board_debug(week_rows)

    summary = {
        "total": len(instruments),
        "complete": sum(1 for x in instruments if x["data_status"] == "complete"),
        "cot_missing": sum(1 for x in instruments if x["data_status"] == "cot_missing"),
        "cot_mapping_missing": sum(1 for x in instruments if x["data_status"] == "cot_mapping_missing"),
        "macro_only": sum(1 for x in instruments if x["data_status"] == "macro_only"),
        "proxy_required": sum(1 for x in instruments if x["data_status"] == "proxy_required"),
        "broken_mapping": sum(1 for x in instruments if x["data_status"] == "broken_mapping"),
        "no_data": sum(1 for x in instruments if x["data_status"] == "no_data"),
        "attention_eligible_count": sum(1 for x in instruments if x["attention_eligible"]),
        "legacy_cot_markets": list(LEGACY_COT_MARKETS),
        "expected_direct_cot_count": len(EXPECTED_DIRECT_COT),
    }

    return {
        "generated_at": pd.Timestamp.now("UTC").isoformat(),
        "latest_calendar_week": latest_calendar_week,
        "summary": summary,
        "instruments": instruments,
        "priority_markets_debug": priority_debug,
    }


def _priority_board_debug(week_rows: list[dict[str, Any]]) -> dict[str, Any]:
    board = aggregate_priority_markets(week_rows, top_n=12)
    included = {m["market"] for m in board.get("priority_markets") or [] if m.get("market")}

    excluded: list[dict[str, str]] = []
    eligible_not_shown: list[dict[str, str]] = []

    for rec in week_rows:
        market = str(rec.get("market") or "")
        ok, reason = explain_attention_eligibility(rec)
        if market in included:
            continue
        if ok:
            eligible_not_shown.append({"market": market, "reason": f"eligible_but_ranked_out: {reason}"})
        else:
            excluded.append({"market": market, "reason": reason})

    return {
        "priority_markets": board.get("priority_markets"),
        "total_actionable": board.get("total_actionable"),
        "excluded_count": len(excluded),
        "excluded_sample": excluded[:40],
        "eligible_not_in_top_n": eligible_not_shown[:20],
    }


def write_coverage_audit(
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


def data_status_for_record(rec: dict[str, Any], spec: InstrumentSpec | None = None) -> str:
    """Lightweight data_status for dashboard rows."""
    spec = spec or get_instrument(str(rec.get("market") or ""))
    if not spec:
        return "no_data"
    cot = _load_cot_master()
    cot_rows, _ = _cot_stats_for_market(cot, spec.id, spec.cot_proxy_of)
    return classify_data_status(
        spec=spec,
        rec=rec,
        cot_rows=cot_rows,
        cot_resolved=_cot_resolved(rec),
        macro_ok=_macro_transmission_ok(rec),
        macro_generic=_macro_generic_only(rec),
    )
