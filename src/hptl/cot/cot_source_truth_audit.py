"""Independent COT source-of-truth audit — official CFTC Legacy Futures Only vs dashboard rendered."""
from __future__ import annotations

import io
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import requests

from hptl.cot.contracts import LEGACY_FUTURES_ONLY_URL_TEMPLATE
from hptl.cot.legacy_cot import (
    CANONICAL_LEGACY_CODE,
    EXCLUDE_NAME_HINTS,
    LEGACY_COLUMN_MAP,
    PARSER_NAME as LEGACY_PARSER,
    _candidate_rows_for_instrument,
    _code_norm,
    _evaluate_mapping_status,
    _exchange_from_name,
    _extract_legacy_position_row,
    _num,
    _row_matches_instrument,
    _select_canonical_row,
)
from hptl.markets.instrument_registry import LEGACY_MARKET_ALIASES, cot_mapped_ids

AUDIT_PARSER = "hptl.cot.cot_source_truth_audit"
DATA_OUT = Path("data/cot_source_truth_audit_latest.json")
PUBLIC_OUT = Path("web-dashboard/public/data/cot_source_truth_audit_latest.json")
DELIVERABLE_MD = Path("data/exports/cot_source_truth_deliverable.md")
DASHBOARD_JSON = Path("web-dashboard/public/data/confluence_history_latest.json")

OverallStatus = Literal["PASS", "FAIL", "NEEDS_MANUAL_REVIEW"]


def download_fresh_cftc_legacy_futures_only(year: int) -> tuple[Path, str]:
    """Download official deacot zip; never use HTPL cache as audit source."""
    url = LEGACY_FUTURES_ONLY_URL_TEMPLATE.format(year=year)
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = Path("data/raw") / f"cot_source_truth_official_{year}_{ts}.zip"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(r.content)
    return out, url


def load_official_legacy_dataframe(year: int, *, force_download: bool = True) -> tuple[pd.DataFrame, dict[str, Any]]:
    if force_download:
        zpath, url = download_fresh_cftc_legacy_futures_only(year)
    else:
        from hptl.cot.legacy_cot import ensure_legacy_futures_only_year

        url = LEGACY_FUTURES_ONLY_URL_TEMPLATE.format(year=year)
        zpath = ensure_legacy_futures_only_year(year, download=True)
        if zpath is None:
            return pd.DataFrame(), {"source_url": url, "source_file": "", "error": "download_failed"}

    with zipfile.ZipFile(zpath) as zf:
        raw = zf.read("annual.txt")
    df = pd.read_csv(io.BytesIO(raw), low_memory=False)
    df = df.copy()
    df["_report_date"] = pd.to_datetime(df[LEGACY_COLUMN_MAP["date"]], errors="coerce").dt.normalize()
    df["_code"] = df[LEGACY_COLUMN_MAP["code"]].map(_code_norm)
    df["_market"] = df[LEGACY_COLUMN_MAP["market"]].astype(str).str.strip()
    meta = {
        "report_type": "legacy_futures_only",
        "source_url": url,
        "source_file": zpath.name,
        "source_path": str(zpath.resolve()),
        "inner_file": "annual.txt",
        "parser": AUDIT_PARSER,
        "legacy_htpl_parser_not_used": LEGACY_PARSER,
    }
    return df, meta


def official_row_snapshot(row: pd.Series, meta: dict[str, Any], row_index: int) -> dict[str, Any]:
    from hptl.cot.legacy_cot import LegacyFrameMeta

    frame_meta = LegacyFrameMeta(
        str(meta.get("report_type") or "legacy_futures_only"),
        str(meta.get("source_file") or ""),
        str(meta.get("source_url") or ""),
    )
    extracted = _extract_legacy_position_row(row, frame_meta, row_index)
    return {
        **extracted,
        "exchange": _exchange_from_name(extracted.get("market_name") or ""),
        "contract_description": extracted.get("market_name"),
        "noncommercial_spread": _num(row.get(LEGACY_COLUMN_MAP["nc_spread"])),
    }


def rows_matching_instrument_on_date(
    df: pd.DataFrame,
    instrument_id: str,
    report_date: pd.Timestamp,
) -> pd.DataFrame:
    if df.empty:
        return df.iloc[0:0]
    sub = df.loc[df["_report_date"] == report_date].copy()
    mask = sub["_market"].apply(lambda n: _row_matches_instrument(n, instrument_id))
    return sub.loc[mask]


def crude_related_rows_on_date(df: pd.DataFrame, report_date: pd.Timestamp) -> list[dict[str, Any]]:
    if df.empty:
        return []
    sub = df.loc[df["_report_date"] == report_date]
    mask = sub["_market"].astype(str).str.contains(
        r"CRUDE|WTI|PHYSICAL.*OIL|OIL.*WTI",
        case=False,
        regex=True,
        na=False,
    )
    out: list[dict[str, Any]] = []
    for idx, row in sub.loc[mask].sort_values("_code").iterrows():
        snap = official_row_snapshot(row, {}, int(idx))
        out.append(
            {
                "market_name": snap["market_name"],
                "exchange": snap["exchange"],
                "cftc_code": snap["cftc_market_code"],
                "noncommercial_long": snap["noncommercial_long"],
                "noncommercial_short": snap["noncommercial_short"],
                "noncommercial_spread": snap.get("noncommercial_spread"),
                "nonreportable_long": snap["nonreportable_long"],
                "nonreportable_short": snap["nonreportable_short"],
                "open_interest": snap["open_interest"],
                "raw_row_index": int(idx),
                "htpl_canonical_for_cl": snap["cftc_market_code"] == CANONICAL_LEGACY_CODE.get("Crude Oil / CL"),
            }
        )
    return out


def nq_related_rows_on_date(df: pd.DataFrame, report_date: pd.Timestamp) -> list[dict[str, Any]]:
    if df.empty:
        return []
    sub = df.loc[df["_report_date"] == report_date]
    mask = sub["_market"].astype(str).str.contains("NASDAQ", case=False, na=False) | sub["_code"].isin(
        ["209742", "20974", "209747", "20974+"]
    )
    out: list[dict[str, Any]] = []
    for idx, row in sub.loc[mask].sort_values("_code").iterrows():
        snap = official_row_snapshot(row, {}, int(idx))
        out.append(
            {
                "market_name": snap["market_name"],
                "exchange": snap["exchange"],
                "cftc_code": snap["cftc_market_code"],
                "noncommercial_long": snap["noncommercial_long"],
                "noncommercial_short": snap["noncommercial_short"],
                "nonreportable_long": snap["nonreportable_long"],
                "nonreportable_short": snap["nonreportable_short"],
                "open_interest": snap["open_interest"],
                "raw_row_index": int(idx),
                "htpl_canonical_for_nq": snap["cftc_market_code"] == CANONICAL_LEGACY_CODE.get("NASDAQ / NQ"),
            }
        )
    return out


def _load_dashboard_confluence_row(instrument_id: str, report_date: str) -> dict[str, Any] | None:
    """Rendered positioning trail / snapshot — sampled from live export, not used as truth."""
    if not DASHBOARD_JSON.exists():
        return None
    doc = json.loads(DASHBOARD_JSON.read_text(encoding="utf-8"))
    hits = [
        r
        for r in doc.get("records") or []
        if r.get("market") == instrument_id
        and str(r.get("cot_report_date") or r.get("date") or "")[:10] == report_date
    ]
    return hits[-1] if hits else None


def _dashboard_group_values(row: dict[str, Any] | None, group_key: str) -> dict[str, float | None]:
    if not row:
        return {"long": None, "short": None, "net": None}
    groups = row.get("cot_positioning_groups") or {}
    block = groups.get(group_key) if isinstance(groups, dict) else None
    if group_key == "managed_money" and (not block or not block.get("available")):
        long_v = _num(row.get("long_value"))
        short_v = _num(row.get("short_value"))
        net_v = _num(row.get("net_value"))
        if net_v is None and long_v is not None and short_v is not None:
            net_v = long_v - short_v
        return {"long": long_v, "short": short_v, "net": net_v}
    if not block:
        return {"long": None, "short": None, "net": None}
    long_v = _num(block.get("long"))
    short_v = _num(block.get("short"))
    net_v = _num(block.get("net"))
    if net_v is None and long_v is not None and short_v is not None:
        net_v = long_v - short_v
    return {"long": long_v, "short": short_v, "net": net_v}


def _compare_group(
    official: dict[str, Any],
    dashboard: dict[str, float | None],
    *,
    long_key: str,
    short_key: str,
) -> dict[str, Any]:
    off_long = _num(official.get(long_key))
    off_short = _num(official.get(short_key))
    off_net = (off_long - off_short) if off_long is not None and off_short is not None else None

    def metric(dash_key: str, off_val: float | None) -> dict[str, Any]:
        d = dashboard.get(dash_key)
        match = None if d is None and off_val is None else (d is not None and off_val is not None and abs(d - off_val) < 0.5)
        return {
            "dashboard_value": d,
            "official_raw_value": off_val,
            "difference": (d - off_val) if d is not None and off_val is not None else None,
            "match": match,
        }

    return {
        "long": metric("long", off_long),
        "short": metric("short", off_short),
        "net": metric("net", off_net),
        "official_net_computed": off_net,
    }


def _group_pass(metrics: dict[str, Any]) -> bool:
    for k in ("long", "short", "net"):
        if metrics[k].get("match") is not True:
            return False
    return True


def audit_instrument(
    df: pd.DataFrame,
    meta: dict[str, Any],
    instrument_id: str,
    report_date: pd.Timestamp,
) -> dict[str, Any]:
    preferred = CANONICAL_LEGACY_CODE.get(instrument_id, "")
    candidates = rows_matching_instrument_on_date(df, instrument_id, report_date)
    candidate_rows: list[dict[str, Any]] = []
    for idx, row in candidates.iterrows():
        candidate_rows.append(official_row_snapshot(row, meta, int(idx)))

    canonical_row = _select_canonical_row(candidates, instrument_id)
    mapping_status, mapping_reasons = _evaluate_mapping_status(
        instrument_id, canonical_row, preferred
    )

    date_str = report_date.strftime("%Y-%m-%d")
    dash_row = _load_dashboard_confluence_row(instrument_id, date_str)
    dash_nc = _dashboard_group_values(dash_row, "managed_money")
    dash_nr = _dashboard_group_values(dash_row, "nonreportable")

    failure_reasons: list[str] = []
    overall: OverallStatus = "PASS"

    if canonical_row is None:
        overall = "FAIL"
        failure_reasons.append("no_official_row_for_canonical_mapping")
    elif len(candidates) > 1 and mapping_status == "NEEDS_MANUAL_REVIEW":
        overall = "NEEDS_MANUAL_REVIEW"
        failure_reasons.extend(mapping_reasons)
    elif preferred and canonical_row is not None and str(canonical_row.get("_code") or "") != preferred:
        overall = "FAIL"
        failure_reasons.append(f"cftc_code_mismatch: got {canonical_row.get('_code')} expected {preferred}")

    official: dict[str, Any] = {}
    if canonical_row is not None:
        official = official_row_snapshot(canonical_row, meta, int(canonical_row.name))

    nc_metrics = _compare_group(
        official,
        dash_nc,
        long_key="noncommercial_long",
        short_key="noncommercial_short",
    )
    nr_metrics = _compare_group(
        official,
        dash_nr,
        long_key="nonreportable_long",
        short_key="nonreportable_short",
    )

    nc_match = _group_pass(nc_metrics) if official else False
    nr_match = _group_pass(nr_metrics) if official else False

    if not dash_row:
        overall = "FAIL"
        failure_reasons.append("dashboard_confluence_row_missing_for_report_date")
        nc_match = False
        nr_match = False
    else:
        if not nc_match:
            for field in ("long", "short", "net"):
                m = nc_metrics[field]
                if m.get("match") is False:
                    failure_reasons.append(
                        f"NC {field}: dashboard={m.get('dashboard_value')} official={m.get('official_raw_value')}"
                    )
        if not nr_match:
            for field in ("long", "short", "net"):
                m = nr_metrics[field]
                if m.get("match") is False:
                    failure_reasons.append(
                        f"Non-Reportable {field}: dashboard={m.get('dashboard_value')} official={m.get('official_raw_value')}"
                    )

    if overall == "PASS" and (not nc_match or not nr_match):
        overall = "FAIL"

    return {
        "instrument": instrument_id,
        "report_date": date_str,
        "selected_cftc_code": official.get("cftc_market_code") or preferred,
        "selected_market_name": official.get("market_name"),
        "exchange": official.get("exchange"),
        "contract_description": official.get("contract_description"),
        "mapping_status": mapping_status,
        "official_raw_source_url": meta.get("source_url"),
        "official_raw_source_file": meta.get("source_file"),
        "official_raw_row_index": official.get("raw_source_row"),
        "dashboard_source_file": str(DASHBOARD_JSON),
        "dashboard_values": {
            "noncommercials": dash_nc,
            "nonreportables": dash_nr,
        },
        "official_raw_values": {
            "noncommercials": {
                "long": official.get("noncommercial_long"),
                "short": official.get("noncommercial_short"),
                "spread": official.get("noncommercial_spread"),
                "net": (
                    official.get("noncommercial_long") - official.get("noncommercial_short")
                    if official.get("noncommercial_long") is not None
                    and official.get("noncommercial_short") is not None
                    else None
                ),
            },
            "nonreportables": {
                "long": official.get("nonreportable_long"),
                "short": official.get("nonreportable_short"),
                "net": (
                    official.get("nonreportable_long") - official.get("nonreportable_short")
                    if official.get("nonreportable_long") is not None
                    and official.get("nonreportable_short") is not None
                    else None
                ),
            },
            "open_interest": official.get("open_interest"),
        },
        "comparisons": {
            "noncommercials": nc_metrics,
            "nonreportables": nr_metrics,
        },
        "nc_match": nc_match,
        "nonreportable_match": nr_match,
        "candidate_row_count": len(candidate_rows),
        "candidate_rows": candidate_rows,
        "status": overall,
        "failure_reasons": failure_reasons,
    }


def build_cot_source_truth_audit(*, year: int | None = None, force_download: bool = True) -> dict[str, Any]:
    year = year or datetime.now(timezone.utc).year
    df, meta = load_official_legacy_dataframe(year, force_download=force_download)
    if df.empty:
        raise RuntimeError("Official CFTC Legacy Futures Only dataframe is empty")

    latest_ts = df["_report_date"].max()
    report_date = pd.Timestamp(latest_ts)

    instruments: dict[str, Any] = {}
    counts = {"PASS": 0, "FAIL": 0, "NEEDS_MANUAL_REVIEW": 0}
    failed: list[str] = []
    review: list[str] = []

    for iid in cot_mapped_ids():
        result = audit_instrument(df, meta, iid, report_date)
        instruments[iid] = result
        st = result["status"]
        counts[st] += 1
        if st == "FAIL":
            failed.append(iid)
        elif st == "NEEDS_MANUAL_REVIEW":
            review.append(iid)

    payload = {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parser": AUDIT_PARSER,
        "audit_chain": [
            "Official CFTC Legacy Futures Only (deacot annual.txt, fresh download)",
            "fresh parse (cot_source_truth_audit)",
            "expected raw values",
            "compare to dashboard rendered (confluence_history_latest.json records)",
        ],
        "official_source": meta,
        "latest_report_date": report_date.strftime("%Y-%m-%d"),
        "special_focus": {
            "crude_oil_cl_all_rows_2026_05_26": crude_related_rows_on_date(df, report_date),
            "nasdaq_nq_all_rows_2026_05_26": nq_related_rows_on_date(df, report_date),
            "htpl_canonical_cl_code": CANONICAL_LEGACY_CODE.get("Crude Oil / CL"),
            "htpl_canonical_nq_code": CANONICAL_LEGACY_CODE.get("NASDAQ / NQ"),
            "cl_recommendation": (
                "Use CFTC code 067651 — WTI-PHYSICAL - NEW YORK MERCANTILE EXCHANGE for Crude Oil / CL. "
                "Do not use 06765A (WTI Financial), disaggregated Managed Money, or TFF leveraged funds."
            ),
        },
        "summary": {
            "total_instruments_checked": len(cot_mapped_ids()),
            "pass_count": counts["PASS"],
            "fail_count": counts["FAIL"],
            "needs_manual_review_count": counts["NEEDS_MANUAL_REVIEW"],
            "failed_instruments": failed,
            "needs_review_instruments": review,
            "all_pass": counts["FAIL"] == 0 and counts["NEEDS_MANUAL_REVIEW"] == 0,
        },
        "instruments": instruments,
    }
    return payload


def write_cot_source_truth_exports(
    payload: dict[str, Any],
    *,
    skip_deliverable: bool = False,
) -> dict[str, Path]:
    for path in (DATA_OUT, PUBLIC_OUT):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if skip_deliverable:
        return {"audit": DATA_OUT, "public": PUBLIC_OUT}

    summary = payload["summary"]
    focus = payload.get("special_focus") or {}
    lines = [
        "# COT Source-of-Truth Audit Deliverable",
        "",
        f"Generated: {payload.get('generated_at')}",
        f"Official file: {payload.get('official_source', {}).get('source_file')}",
        f"URL: {payload.get('official_source', {}).get('source_url')}",
        f"Report date: {payload.get('latest_report_date')}",
        "",
        "## Summary",
        f"- Checked: {summary.get('total_instruments_checked')}",
        f"- PASS: {summary.get('pass_count')}",
        f"- FAIL: {summary.get('fail_count')}",
        f"- NEEDS_MANUAL_REVIEW: {summary.get('needs_manual_review_count')}",
        "",
    ]
    if summary.get("failed_instruments"):
        lines.append("## Failed instruments")
        for iid in summary["failed_instruments"]:
            inst = payload["instruments"][iid]
            lines.append(f"- **{iid}**: {'; '.join(inst.get('failure_reasons') or [])}")
        lines.append("")
    lines.append("## Crude-related official rows (focus date)")
    for row in focus.get("crude_oil_cl_all_rows_2026_05_26") or []:
        lines.append(
            f"- `{row.get('cftc_code')}` {row.get('market_name')} | NC {row.get('noncommercial_long')}/{row.get('noncommercial_short')} "
            f"| NR {row.get('nonreportable_long')}/{row.get('nonreportable_short')} | OI {row.get('open_interest')} "
            f"{'[HTPL CL]' if row.get('htpl_canonical_for_cl') else ''}"
        )
    lines.append("")
    lines.append("## NQ-related official rows")
    for row in focus.get("nasdaq_nq_all_rows_2026_05_26") or []:
        lines.append(
            f"- `{row.get('cftc_code')}` {row.get('market_name')} | NC {row.get('noncommercial_long')}/{row.get('noncommercial_short')} "
            f"{'[HTPL NQ]' if row.get('htpl_canonical_for_nq') else ''}"
        )
    lines.append("")
    lines.append(focus.get("cl_recommendation", ""))
    DELIVERABLE_MD.parent.mkdir(parents=True, exist_ok=True)
    DELIVERABLE_MD.write_text("\n".join(lines), encoding="utf-8")

    return {"audit": DATA_OUT, "public": PUBLIC_OUT, "deliverable": DELIVERABLE_MD}
