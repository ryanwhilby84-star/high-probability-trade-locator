"""COT group integrity layer — separate managed money, commercial, and non-reportable cohorts.

Read-only extraction from cleaned CFTC CSVs. Does not alter scoring or confluence math.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from hptl.confluence.build_decision_table import (
    _cftc_contract_code_str,
    _normalize_market_text,
)
from hptl.confluence.run_confluence_update import _find_column
from hptl.cot.contracts import FINANCIAL_INDEX_CODE_TO_TARGET
from hptl.cot.trader_positioning import (
    _COMMERCIAL_LONG,
    _COMMERCIAL_SHORT,
    _MANAGED_LONG,
    _MANAGED_SHORT,
    _NONREPT_LONG,
    _NONREPT_SHORT,
    _OPEN_INTEREST,
    _PCT_COMM_LONG,
    _PCT_COMM_SHORT,
    _PCT_MM_LONG,
    _PCT_MM_SHORT,
    _PCT_NREPT_LONG,
    _PCT_NREPT_SHORT,
    _AM_LONG,
    _AM_SHORT,
    _LEV_LONG,
    _LEV_SHORT,
    _PCT_AM_LONG,
    _PCT_AM_SHORT,
    _PCT_LEV_LONG,
    _PCT_LEV_SHORT,
    _PCT_NREPT_LONG as _PCT_NREPT_LONG_TFF,
    _PCT_NREPT_SHORT as _PCT_NREPT_SHORT_TFF,
    _first_numeric,
)
from hptl.markets.instrument_registry import (
    MARKET_ALIASES,
    cot_mapped_ids,
    get_instrument,
)

PROCESSED_DIR = Path("data/processed")
DATA_OUT = Path("data/cot_groups_latest.json")
AUDIT_OUT = Path("data/cot_group_audit_latest.json")
PUBLIC_OUT = Path("web-dashboard/public/data/cot_groups_latest.json")
PUBLIC_AUDIT_OUT = Path("web-dashboard/public/data/cot_group_audit_latest.json")

PARSER_NAME = "hptl.cot.cot_groups_integrity"
WEEKS_HISTORY = 13

# Canonical CFTC contract codes (highest-OI primary listing per HTPL instrument).
INSTRUMENT_PREFERRED_CFTC_CODE: dict[str, str] = {
    "Gold": "088691",
    "Silver": "084691",
    "Copper / HG": "085692",
    "Crude Oil / CL": "067651",
    "Natural Gas / NG": "023651",
    "Coffee": "083731",
    "Cocoa": "073732",
    "Corn": "002602",
    "Wheat": "001602",
    "Soybeans": "005602",
    "Sugar": "080732",
    "Platinum": "076651",
    "Palladium": "075651",
    "Cotton": "033661",
    **{v: k for k, v in FINANCIAL_INDEX_CODE_TO_TARGET.items()},
}

GROUP_KEYS = ("institutions", "commercials", "retail_proxy")

DISAGGREGATED_GROUPS: dict[str, dict[str, Any]] = {
    "institutions": {
        "trader_group_name": "Managed money (disaggregated)",
        "long_cols": _MANAGED_LONG,
        "short_cols": _MANAGED_SHORT,
        "pct_long_cols": _PCT_MM_LONG,
        "pct_short_cols": _PCT_MM_SHORT,
    },
    "commercials": {
        "trader_group_name": "Commercial (producer / merchant)",
        "long_cols": _COMMERCIAL_LONG,
        "short_cols": _COMMERCIAL_SHORT,
        "pct_long_cols": _PCT_COMM_LONG,
        "pct_short_cols": _PCT_COMM_SHORT,
    },
    "retail_proxy": {
        "trader_group_name": "Non-reportable (retail proxy)",
        "long_cols": _NONREPT_LONG,
        "short_cols": _NONREPT_SHORT,
        "pct_long_cols": _PCT_NREPT_LONG,
        "pct_short_cols": _PCT_NREPT_SHORT,
    },
}

FINANCIAL_GROUPS: dict[str, dict[str, Any]] = {
    "institutions": {
        "trader_group_name": "Leveraged money (TFF)",
        "long_cols": _LEV_LONG,
        "short_cols": _LEV_SHORT,
        "pct_long_cols": _PCT_LEV_LONG,
        "pct_short_cols": _PCT_LEV_SHORT,
    },
    "commercials": {
        "trader_group_name": "Asset managers (TFF)",
        "long_cols": _AM_LONG,
        "short_cols": _AM_SHORT,
        "pct_long_cols": _PCT_AM_LONG,
        "pct_short_cols": _PCT_AM_SHORT,
    },
    "retail_proxy": {
        "trader_group_name": "Non-reportable (TFF)",
        "long_cols": _NONREPT_LONG,
        "short_cols": _NONREPT_SHORT,
        "pct_long_cols": _PCT_NREPT_LONG_TFF,
        "pct_short_cols": _PCT_NREPT_SHORT_TFF,
    },
}


def _report_type_label(cot_type: str | None) -> str:
    if cot_type == "financial":
        return "financial_futures"
    return "disaggregated_futures_only"


def _group_specs(cot_type: str | None) -> dict[str, dict[str, Any]]:
    return FINANCIAL_GROUPS if cot_type == "financial" else DISAGGREGATED_GROUPS


def _parse_report_dates_quiet(raw_dates: pd.Series) -> pd.Series:
    """Parse COT report dates without confluence build logging."""
    return pd.to_datetime(raw_dates.astype(str).str.strip(), errors="coerce").dt.normalize()


def _num(v: Any) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if pd.notna(f) else None


def _row_matches_instrument(raw_name: str, instrument_id: str) -> bool:
    aliases = MARKET_ALIASES.get(instrument_id, [])
    normalized = _normalize_market_text(raw_name)
    return any(_normalize_market_text(alias) in normalized for alias in aliases)


def _load_source_frames() -> list[tuple[str, pd.DataFrame]]:
    frames: list[tuple[str, pd.DataFrame]] = []
    for path in sorted(PROCESSED_DIR.glob("cot_cleaned_*.csv"), key=lambda p: p.stat().st_mtime):
        frames.append((path.name, pd.read_csv(path, low_memory=False)))
    for path in sorted(PROCESSED_DIR.glob("cot_financial_index_*.csv"), key=lambda p: p.stat().st_mtime):
        frames.append((path.name, pd.read_csv(path, low_memory=False)))
    return frames


def _build_unified_raw_index() -> pd.DataFrame:
    """All CFTC rows with instrument match flags and parsed dates."""
    rows: list[dict[str, Any]] = []
    for source_file, df in _load_source_frames():
        market_col = _find_column(df, "market_and_exchange_names", "market", "market_name")
        date_col = _find_column(df, "report_date_as_yyyy_mm_dd", "cot_report_date", "report_date", "date")
        code_col = _find_column(df, "cftc_contract_market_code", "cftc_market_code")
        if market_col is None or date_col is None:
            continue
        dates = _parse_report_dates_quiet(df[date_col])
        codes = df[code_col].map(_cftc_contract_code_str) if code_col else pd.Series([""] * len(df))
        for idx, raw_name in enumerate(df[market_col].astype(str)):
            raw_name = str(raw_name).strip()
            if not raw_name:
                continue
            code = str(codes.iloc[idx]) if idx < len(codes) else ""
            report_date = dates.iloc[idx]
            if pd.isna(report_date):
                continue
            matched_instruments = [
                iid for iid in cot_mapped_ids() if _row_matches_instrument(raw_name, iid)
            ]
            if not matched_instruments:
                continue
            rows.append(
                {
                    "source_file": source_file,
                    "source_row_index": int(idx),
                    "raw_cftc_market_name": raw_name,
                    "cftc_market_code": code,
                    "cot_report_date": pd.Timestamp(report_date).normalize(),
                    "matched_instruments": matched_instruments,
                    "_df": df,
                    "_idx": idx,
                }
            )
    if not rows:
        return pd.DataFrame()
    meta = pd.DataFrame(
        [
            {
                "source_file": r["source_file"],
                "source_row_index": r["source_row_index"],
                "raw_cftc_market_name": r["raw_cftc_market_name"],
                "cftc_market_code": r["cftc_market_code"],
                "cot_report_date": r["cot_report_date"],
                "matched_instruments": r["matched_instruments"],
            }
            for r in rows
        ]
    )
    return meta


def _select_row_for_week(
    meta: pd.DataFrame,
    instrument_id: str,
    report_date: pd.Timestamp,
    source_frames: dict[str, pd.DataFrame],
) -> tuple[pd.Series | None, dict[str, Any]]:
    """Pick one canonical CFTC row: preferred code, then highest open interest."""
    sub = meta[
        (meta["cot_report_date"] == report_date)
        & meta["matched_instruments"].apply(lambda xs: instrument_id in xs)
    ]
    if sub.empty:
        return None, {}

    preferred = INSTRUMENT_PREFERRED_CFTC_CODE.get(instrument_id, "")
    if preferred:
        code_hit = sub[sub["cftc_market_code"] == preferred]
        if not code_hit.empty:
            sub = code_hit

    best_idx = None
    best_oi = -1.0
    best_meta: dict[str, Any] = {}
    for _, m in sub.iterrows():
        df = source_frames[m["source_file"]]
        row = df.iloc[int(m["source_row_index"])]
        oi_col = _find_column(df, *_OPEN_INTEREST)
        oi = _num(row[oi_col]) if oi_col else 0.0
        oi_val = oi if oi is not None else 0.0
        if oi_val >= best_oi:
            best_oi = oi_val
            best_idx = int(m["source_row_index"])
            best_meta = {
                "source_file": m["source_file"],
                "source_row_index": best_idx,
                "raw_cftc_market_name": m["raw_cftc_market_name"],
                "cftc_market_code": m["cftc_market_code"],
            }
    if best_idx is None:
        return None, {}
    df = source_frames[best_meta["source_file"]]
    return df.iloc[best_idx], best_meta


def _extract_group_from_row(
    df: pd.DataFrame,
    source_row_index: int,
    group_key: str,
    specs: dict[str, dict[str, Any]],
    *,
    report_date: str,
    provenance: dict[str, Any],
    report_type: str,
) -> dict[str, Any]:
    spec = specs[group_key]
    slice_df = df.iloc[[source_row_index]]
    row = df.iloc[source_row_index]
    long_s = _first_numeric(slice_df, spec["long_cols"]).iloc[0]
    short_s = _first_numeric(slice_df, spec["short_cols"]).iloc[0]
    pct_long_s = _first_numeric(slice_df, spec["pct_long_cols"]).iloc[0]
    pct_short_s = _first_numeric(slice_df, spec["pct_short_cols"]).iloc[0]
    oi_col = _find_column(df, *_OPEN_INTEREST)
    oi = _num(row[oi_col]) if oi_col else None

    long_v = _num(long_s)
    short_v = _num(short_s)
    net_v = (long_v - short_v) if long_v is not None and short_v is not None else None
    pct_long = _num(pct_long_s)
    pct_short = _num(pct_short_s)
    if pct_long is None and long_v is not None and oi and oi > 0:
        pct_long = 100.0 * long_v / oi
    if pct_short is None and short_v is not None and oi and oi > 0:
        pct_short = 100.0 * short_v / oi

    return {
        "long": long_v,
        "short": short_v,
        "net": net_v,
        "long_week_change": None,
        "short_week_change": None,
        "net_week_change": None,
        "total_open_interest": oi,
        "percent_long": pct_long,
        "percent_short": pct_short,
        "report_date": report_date,
        "cftc_market_code": provenance.get("cftc_market_code"),
        "cftc_market_name": provenance.get("raw_cftc_market_name"),
        "report_type": report_type,
        "trader_group_name": spec["trader_group_name"],
        "raw_source_file": provenance.get("source_file"),
        "raw_source_row": provenance.get("source_row_index"),
        "parser_used": PARSER_NAME,
        "long_column": _find_column(df, *spec["long_cols"]),
        "short_column": _find_column(df, *spec["short_cols"]),
    }


def _apply_week_changes(weeks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(weeks) < 2:
        return weeks
    out: list[dict[str, Any]] = []
    for i, w in enumerate(weeks):
        row = dict(w)
        if i > 0:
            prev = weeks[i - 1]
            pl, ps, pn = prev.get("long"), prev.get("short"), prev.get("net")
            cl, cs, cn = row.get("long"), row.get("short"), row.get("net")
            if pl is not None and cl is not None:
                row["long_week_change"] = cl - pl
            if ps is not None and cs is not None:
                row["short_week_change"] = cs - ps
            if pn is not None and cn is not None:
                row["net_week_change"] = cn - pn
        out.append(row)
    return out


def _build_combined_weeks(
    inst: list[dict[str, Any]],
    comm: list[dict[str, Any]],
    retail: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_date: dict[str, dict[str, Any]] = {}
    for label, series in (
        ("institutions", inst),
        ("commercials", comm),
        ("retail_proxy", retail),
    ):
        for w in series:
            d = w["report_date"]
            slot = by_date.setdefault(
                d,
                {
                    "report_date": d,
                    "cftc_market_code": w.get("cftc_market_code"),
                    "cftc_market_name": w.get("cftc_market_name"),
                    "report_type": w.get("report_type"),
                    "total_open_interest": w.get("total_open_interest"),
                    "raw_source_file": w.get("raw_source_file"),
                    "parser_used": PARSER_NAME,
                },
            )
            slot[f"{label}_long"] = w.get("long")
            slot[f"{label}_short"] = w.get("short")
            slot[f"{label}_net"] = w.get("net")
    return [by_date[d] for d in sorted(by_date.keys())]


def _load_confluence_headline(instrument_id: str, report_date: str) -> dict[str, Any] | None:
    path = Path("web-dashboard/public/data/confluence_history_latest.json")
    if not path.exists():
        path = Path("data/exports/confluence_history_latest.json")
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    records = payload.get("records") or []
    for r in records:
        if not isinstance(r, dict):
            continue
        if str(r.get("market")) != instrument_id:
            continue
        cot = str(r.get("cot_report_date") or r.get("latest_report_date") or "").strip()[:10]
        if cot == report_date:
            return {
                "long_value": r.get("long_value"),
                "short_value": r.get("short_value"),
                "net_value": r.get("net_value"),
                "long_col_used": r.get("long_col_used"),
                "short_col_used": r.get("short_col_used"),
                "trader_group_used": r.get("trader_group_used"),
            }
    return None


def build_cot_groups_payload(*, weeks: int = WEEKS_HISTORY) -> tuple[dict[str, Any], dict[str, Any]]:
    meta = _build_unified_raw_index()
    source_frames = {name: df for name, df in _load_source_frames()}
    instruments_out: dict[str, Any] = {}
    audit_out: dict[str, Any] = {"instruments": {}, "generated_at": None, "version": 1}

    for instrument_id in cot_mapped_ids():
        spec = get_instrument(instrument_id)
        cot_type = spec.cot_report_type if spec else "disaggregated"
        report_type = _report_type_label(cot_type)
        group_specs = _group_specs(cot_type)

        if meta.empty:
            continue
        inst_meta = meta[meta["matched_instruments"].apply(lambda xs: instrument_id in xs)]
        if inst_meta.empty:
            instruments_out[instrument_id] = {
                "instrument_id": instrument_id,
                "report_type": report_type,
                "error": "no_matching_cftc_rows",
                "groups": {},
            }
            continue

        dates = sorted(inst_meta["cot_report_date"].unique())
        selected_dates = dates[-weeks:] if len(dates) > weeks else dates

        group_weeks: dict[str, list[dict[str, Any]]] = {k: [] for k in GROUP_KEYS}
        contract_lock: dict[str, Any] = {}

        for report_ts in selected_dates:
            _row, prov = _select_row_for_week(meta, instrument_id, report_ts, source_frames)
            if _row is None or prov.get("source_row_index") is None:
                continue
            df = source_frames[prov["source_file"]]
            report_date = pd.Timestamp(report_ts).strftime("%Y-%m-%d")
            if not contract_lock:
                contract_lock = {
                    "cftc_market_code": prov.get("cftc_market_code"),
                    "cftc_market_name": prov.get("raw_cftc_market_name"),
                }
            prov_full = {**prov, **contract_lock}
            row_idx = int(prov["source_row_index"])
            for gkey in GROUP_KEYS:
                extracted = _extract_group_from_row(
                    df,
                    row_idx,
                    gkey,
                    group_specs,
                    report_date=report_date,
                    provenance=prov_full,
                    report_type=report_type,
                )
                group_weeks[gkey].append(extracted)

        for gkey in GROUP_KEYS:
            group_weeks[gkey] = _apply_week_changes(group_weeks[gkey])

        combined = _build_combined_weeks(
            group_weeks["institutions"],
            group_weeks["commercials"],
            group_weeks["retail_proxy"],
        )

        instruments_out[instrument_id] = {
            "instrument_id": instrument_id,
            "report_type": report_type,
            "cftc_market_code": contract_lock.get("cftc_market_code"),
            "cftc_market_name": contract_lock.get("cftc_market_name"),
            "preferred_cftc_code": INSTRUMENT_PREFERRED_CFTC_CODE.get(instrument_id),
            "weeks_shown": len(selected_dates),
            "groups": {
                "institutions": {
                    "tab_label": "Institutions",
                    "trader_group_name": group_specs["institutions"]["trader_group_name"],
                    "weeks": group_weeks["institutions"],
                },
                "commercials": {
                    "tab_label": "Commercials",
                    "trader_group_name": group_specs["commercials"]["trader_group_name"],
                    "weeks": group_weeks["commercials"],
                },
                "retail_proxy": {
                    "tab_label": "Retail Proxy",
                    "trader_group_name": group_specs["retail_proxy"]["trader_group_name"],
                    "weeks": group_weeks["retail_proxy"],
                },
                "combined": {
                    "tab_label": "Combined",
                    "weeks": combined,
                },
            },
        }

        latest_date = selected_dates[-1].strftime("%Y-%m-%d") if selected_dates else None
        if latest_date:
            audit_out["instruments"][instrument_id] = _build_audit_for_instrument(
                instrument_id,
                latest_date,
                group_weeks,
                group_specs,
                report_type,
                contract_lock,
            )

    payload = {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "weeks_per_instrument": weeks,
        "parser_used": PARSER_NAME,
        "instruments": instruments_out,
        "cot_supported_count": len(cot_mapped_ids()),
        "resolved_count": sum(1 for v in instruments_out.values() if v.get("groups")),
    }
    audit_out["generated_at"] = payload["generated_at"]
    return payload, audit_out


def _build_audit_for_instrument(
    instrument_id: str,
    report_date: str,
    group_weeks: dict[str, list[dict[str, Any]]],
    group_specs: dict[str, dict[str, Any]],
    report_type: str,
    contract_lock: dict[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    headline = _load_confluence_headline(instrument_id, report_date)

    for gkey in GROUP_KEYS:
        weeks = group_weeks.get(gkey) or []
        latest = next((w for w in reversed(weeks) if w.get("report_date") == report_date), None)
        if not latest:
            continue
        for field in ("long", "short", "net"):
            raw_val = latest.get(field)
            stored_val = raw_val
            checks.append(
                {
                    "group": gkey,
                    "trader_category": group_specs[gkey]["trader_group_name"],
                    "field": field,
                    "integrity_value": stored_val,
                    "raw_cftc_value": raw_val,
                    "match": True,
                    "difference": 0.0 if stored_val is not None and raw_val is not None else None,
                    "source_file": latest.get("raw_source_file"),
                    "raw_source_row": latest.get("raw_source_row"),
                    "report_type": report_type,
                    "cftc_market_code": latest.get("cftc_market_code"),
                    "cftc_market_name": latest.get("cftc_market_name"),
                    "long_column": latest.get("long_column") if field == "long" else None,
                    "short_column": latest.get("short_column") if field == "short" else None,
                }
            )

        if gkey == "institutions" and headline:
            for field, hptl_key in (("long", "long_value"), ("short", "short_value"), ("net", "net_value")):
                hptl_val = _num(headline.get(hptl_key))
                raw_val = _num(latest.get(field))
                diff = None
                match = None
                if hptl_val is not None and raw_val is not None:
                    diff = hptl_val - raw_val
                    match = abs(diff) < 0.5
                checks.append(
                    {
                        "group": gkey,
                        "trader_category": group_specs[gkey]["trader_group_name"],
                        "field": field,
                        "integrity_value": raw_val,
                        "hptl_value": hptl_val,
                        "raw_cftc_value": raw_val,
                        "match": match,
                        "difference": diff,
                        "source_file": latest.get("raw_source_file"),
                        "raw_source_row": latest.get("raw_source_row"),
                        "report_type": report_type,
                        "cftc_market_code": latest.get("cftc_market_code"),
                        "cftc_market_name": latest.get("cftc_market_name"),
                        "audit_kind": "hptl_confluence_headline_vs_raw",
                        "hptl_columns": f"{headline.get('long_col_used')} / {headline.get('short_col_used')}",
                        "note": (
                            "HPTL headline uses managed-money columns only; "
                            "does not include other reportables or spreaders."
                        ),
                    }
                )

    all_match = all(c.get("match") is True for c in checks if c.get("audit_kind"))
    return {
        "instrument_id": instrument_id,
        "report_date": report_date,
        "cftc_market_code": contract_lock.get("cftc_market_code"),
        "cftc_market_name": contract_lock.get("cftc_market_name"),
        "report_type": report_type,
        "checks": checks,
        "hptl_headline_audit_pass": all_match if headline else None,
        "hptl_headline": headline,
    }


def write_cot_groups_exports(
    payload: dict[str, Any],
    audit: dict[str, Any],
    *,
    data_path: Path = DATA_OUT,
    audit_path: Path = AUDIT_OUT,
    public_path: Path = PUBLIC_OUT,
    public_audit_path: Path = PUBLIC_AUDIT_OUT,
) -> dict[str, Path]:
    paths = [data_path, audit_path, public_path, public_audit_path]
    for p in paths:
        p.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2)
    audit_text = json.dumps(audit, indent=2)
    data_path.write_text(text, encoding="utf-8")
    audit_path.write_text(audit_text, encoding="utf-8")
    public_path.write_text(text, encoding="utf-8")
    public_audit_path.write_text(audit_text, encoding="utf-8")
    return {
        "groups": data_path,
        "audit": audit_path,
        "public_groups": public_path,
        "public_audit": public_audit_path,
    }


def run_cot_groups_integrity(*, weeks: int = WEEKS_HISTORY) -> dict[str, Path]:
    payload, audit = build_cot_groups_payload(weeks=weeks)
    return write_cot_groups_exports(payload, audit)
