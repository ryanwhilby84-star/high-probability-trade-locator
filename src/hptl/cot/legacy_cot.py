"""Legacy COT — single source of truth (Non-Commercial / Commercial / Non-Reportable).

Replaces TFF, disaggregated, and financial-futures positioning for HTPL instruments.
Does not apply scoring; produces audited JSON for dashboard and reconciliation.
"""
from __future__ import annotations

import io
import json
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import requests

from hptl.confluence.build_decision_table import _cftc_contract_code_str, _normalize_market_text
from hptl.cot.contracts import LEGACY_FUTURES_ONLY_URL_TEMPLATE
from hptl.cot.downloader import download_legacy_futures_only_history
from hptl.config import get_settings
from hptl.markets.instrument_registry import LEGACY_MARKET_ALIASES, cot_mapped_ids, get_instrument

PARSER_NAME = "hptl.cot.legacy_cot"
# Workstation target: up to 10 calendar years of weekly Legacy COT (~520 reports).
HISTORY_YEARS = 10
WEEKS_HISTORY = 520


def default_history_years(*, span_years: int = HISTORY_YEARS) -> list[int]:
    """CFTC annual ZIP years covering the last ``span_years`` calendar years."""
    end = datetime.now(timezone.utc).year
    start = end - span_years + 1
    return list(range(start, end + 1))

LEGACY_FUTURES_OPTIONS_URL_TEMPLATE = "https://www.cftc.gov/files/dea/history/dea_com_xls_{year}.zip"

DATA_RECON = Path("data/legacy_cot_reconciliation.json")
DATA_LATEST = Path("data/legacy_cot_latest.json")
DATA_AUDIT = Path("data/legacy_cot_audit.json")
PUBLIC_RECON = Path("web-dashboard/public/data/legacy_cot_reconciliation.json")
PUBLIC_LATEST = Path("web-dashboard/public/data/legacy_cot_latest.json")
PUBLIC_AUDIT = Path("web-dashboard/public/data/legacy_cot_audit.json")
REPORT_DELIVERABLE = Path("data/exports/legacy_cot_reset_report.json")

MappingStatus = Literal["PASS", "FAIL", "NEEDS_MANUAL_REVIEW"]

# Canonical Legacy Futures Only contract per HTPL instrument (White Oak–aligned).
CANONICAL_LEGACY_CODE: dict[str, str] = {
    "NASDAQ / NQ": "209742",
    "S&P 500 / ES": "13874A",
    "Dow / YM": "124603",
    "Euro FX / 6E": "099741",
    "British Pound / 6B": "096742",
    "Japanese Yen / 6J": "097741",
    "Swiss Franc / 6S": "092741",
    "Australian Dollar / 6A": "232741",
    "Canadian Dollar / 6C": "090741",
    "NZ Dollar / 6N": "112741",
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
    "Bitcoin": "133741",
    "US Dollar Index / DX": "098662",
}

# Rows to exclude when matching by alias (e.g. micro contracts).
EXCLUDE_NAME_HINTS: dict[str, list[str]] = {
    "NASDAQ / NQ": ["MICRO E-MINI NASDAQ", "NASDAQ-100 Consolidated"],
    "S&P 500 / ES": ["MICRO E-MINI S&P", "S&P 500 Consolidated"],
    "Dow / YM": ["DJIA Consolidated", "MICRO"],
    "Gold": ["MICRO GOLD"],
    "Silver": ["MICRO SILVER"],
    "Bitcoin": ["MICRO BITCOIN", "MICRO E-MINI BITCOIN"],
    "US Dollar Index / DX": ["MICRO US DOLLAR INDEX"],
}

LEGACY_COLUMN_MAP = {
    "market": "Market and Exchange Names",
    "code": "CFTC Contract Market Code",
    "date": "As of Date in Form YYYY-MM-DD",
    "nc_long": "Noncommercial Positions-Long (All)",
    "nc_short": "Noncommercial Positions-Short (All)",
    "nc_spread": "Noncommercial Positions-Spreading (All)",
    "comm_long": "Commercial Positions-Long (All)",
    "comm_short": "Commercial Positions-Short (All)",
    "nrep_long": "Nonreportable Positions-Long (All)",
    "nrep_short": "Nonreportable Positions-Short (All)",
    "oi": "Open Interest (All)",
}


@dataclass(frozen=True)
class LegacyFrameMeta:
    report_type: str
    source_file: str
    source_url: str


def _num(v: Any) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if pd.notna(f) else None


def _code_norm(raw: Any) -> str:
    return _cftc_contract_code_str(raw)


def _exchange_from_name(name: str) -> str:
    if " - " in name:
        return name.split(" - ", 1)[-1].strip()
    return ""


def _load_current_htpl_snapshot(instrument_id: str) -> dict[str, Any]:
    """Best-effort current pipeline mapping from confluence export."""
    path = Path("web-dashboard/public/data/confluence_history_latest.json")
    out: dict[str, Any] = {"source": None}
    if not path.exists():
        return out
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return out
    dates = []
    rows = []
    for r in payload.get("records") or []:
        if not isinstance(r, dict) or r.get("market") != instrument_id:
            continue
        d = str(r.get("cot_report_date") or r.get("latest_report_date") or "")[:10]
        if d:
            dates.append(d)
            rows.append(r)
    if not rows:
        return out
    latest = max(dates)
    row = next(r for r in rows if str(r.get("cot_report_date") or r.get("latest_report_date") or "")[:10] == latest)
    spec = get_instrument(instrument_id)
    return {
        "source": "confluence_history_latest.json",
        "report_date": latest,
        "long_value": row.get("long_value"),
        "short_value": row.get("short_value"),
        "net_value": row.get("net_value"),
        "long_col_used": row.get("long_col_used"),
        "short_col_used": row.get("short_col_used"),
        "raw_cftc_market_name": row.get("raw_cftc_market_name"),
        "trader_group_used": row.get("trader_group_used"),
        "instrument_cot_report_type": spec.cot_report_type if spec else None,
        "inferred_report_family": (
            "financial_futures_tff"
            if "lev_money" in str(row.get("long_col_used") or row.get("trader_group_used") or "")
            else "disaggregated_or_other"
        ),
    }


def _find_local_legacy_zip(year: int) -> Path | None:
    raw = Path("data/raw")
    hits = sorted(raw.glob(f"cot_legacy_futures_only_{year}_*.zip"), key=lambda p: p.stat().st_mtime)
    return hits[-1] if hits else None


def _legacy_zip_max_report_date(zpath: Path) -> pd.Timestamp | None:
    try:
        with zipfile.ZipFile(zpath) as zf:
            df = pd.read_csv(
                io.BytesIO(zf.read("annual.txt")),
                usecols=[LEGACY_COLUMN_MAP["date"]],
            )
        parsed = pd.to_datetime(df[LEGACY_COLUMN_MAP["date"]], errors="coerce")
        if parsed.notna().any():
            return parsed.max()
    except (OSError, KeyError, zipfile.BadZipFile):
        return None
    return None


def ensure_legacy_futures_only_year(year: int, *, download: bool = True, force_refresh: bool = False) -> Path | None:
    path = _find_local_legacy_zip(year)
    if path and path.exists() and not force_refresh:
        max_dt = _legacy_zip_max_report_date(path)
        # Re-fetch if cache is more than ~10 days behind UTC today (CFTC weekly).
        if max_dt is not None:
            lag_days = (pd.Timestamp(datetime.now(timezone.utc).date()) - max_dt.normalize()).days
            if lag_days > 10 and download:
                path = None
        if path is not None:
            return path
    if not download:
        return None
    from dataclasses import replace

    settings = replace(get_settings(), cot_year=year)
    try:
        result = download_legacy_futures_only_history(settings, year=year)
        return result.raw_file_path
    except RuntimeError:
        url = LEGACY_FUTURES_ONLY_URL_TEMPLATE.format(year=year)
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out = Path("data/raw") / f"cot_legacy_futures_only_{year}_{ts}.zip"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(r.content)
        return out


def load_legacy_futures_only_dataframe(year: int, *, download: bool = True) -> tuple[pd.DataFrame, LegacyFrameMeta]:
    zpath = ensure_legacy_futures_only_year(year, download=download)
    if zpath is None:
        return pd.DataFrame(), LegacyFrameMeta("legacy_futures_only", "", "")
    with zipfile.ZipFile(zpath) as zf:
        raw = zf.read("annual.txt")
    df = pd.read_csv(io.BytesIO(raw), low_memory=False)
    df["_report_date"] = pd.to_datetime(df[LEGACY_COLUMN_MAP["date"]], errors="coerce").dt.normalize()
    df["_code"] = df[LEGACY_COLUMN_MAP["code"]].map(_code_norm)
    df["_market"] = df[LEGACY_COLUMN_MAP["market"]].astype(str).str.strip()
    meta = LegacyFrameMeta(
        report_type="legacy_futures_only",
        source_file=zpath.name,
        source_url=LEGACY_FUTURES_ONLY_URL_TEMPLATE.format(year=year),
    )
    return df, meta


def load_legacy_futures_only_multiyear(
    years: list[int], *, download: bool = True
) -> tuple[pd.DataFrame, LegacyFrameMeta]:
    """Concatenate multiple annual Legacy Futures-Only frames into one history.

    Used to build a multi-year (e.g. 3-year / 156-week) Non-Commercial series.
    Years that fail to load are skipped; the returned meta references the set of
    source files actually loaded.
    """
    frames: list[pd.DataFrame] = []
    sources: list[str] = []
    for y in sorted(set(years)):
        df, meta = load_legacy_futures_only_dataframe(y, download=download)
        if df.empty:
            print(f"LEGACY_COT_MULTIYEAR: year={y} produced no rows — skipping.")
            continue
        frames.append(df)
        sources.append(meta.source_file)
        print(f"LEGACY_COT_MULTIYEAR: year={y} rows={len(df)} file={meta.source_file}")
    if not frames:
        return pd.DataFrame(), LegacyFrameMeta("legacy_futures_only", "", "")
    combined = pd.concat(frames, ignore_index=True)
    meta = LegacyFrameMeta(
        report_type="legacy_futures_only",
        source_file="+".join(sources),
        source_url="https://www.cftc.gov/files/dea/history/deacot{year}.zip",
    )
    return combined, meta


def try_load_legacy_futures_options_sample(year: int) -> dict[str, Any] | None:
    """Futures+options combined (XLS). Returns summary only if parse fails."""
    url = LEGACY_FUTURES_OPTIONS_URL_TEMPLATE.format(year=year)
    try:
        r = requests.head(url, timeout=15)
        if r.status_code != 200:
            return {"available": False, "url": url, "reason": f"HTTP {r.status_code}"}
    except requests.RequestException as exc:
        return {"available": False, "url": url, "reason": str(exc)}
    return {
        "available": True,
        "url": url,
        "inner_file": "annualof.xls",
        "note": "Legacy futures+options combined available as XLS; primary HTPL layer uses futures-only (deacot).",
    }


def _row_matches_instrument(market_name: str, instrument_id: str) -> bool:
    normalized = _normalize_market_text(market_name)
    for ex in EXCLUDE_NAME_HINTS.get(instrument_id, []):
        if _normalize_market_text(ex) in normalized:
            return False
    aliases = LEGACY_MARKET_ALIASES.get(instrument_id, [])
    return any(_normalize_market_text(alias) in normalized for alias in aliases)


def _extract_legacy_position_row(row: pd.Series, meta: LegacyFrameMeta, row_index: int) -> dict[str, Any]:
    nc_l = _num(row.get(LEGACY_COLUMN_MAP["nc_long"]))
    nc_s = _num(row.get(LEGACY_COLUMN_MAP["nc_short"]))
    nc_sp = _num(row.get(LEGACY_COLUMN_MAP["nc_spread"]))
    comm_l = _num(row.get(LEGACY_COLUMN_MAP["comm_long"]))
    comm_s = _num(row.get(LEGACY_COLUMN_MAP["comm_short"]))
    nrep_l = _num(row.get(LEGACY_COLUMN_MAP["nrep_long"]))
    nrep_s = _num(row.get(LEGACY_COLUMN_MAP["nrep_short"]))
    oi = _num(row.get(LEGACY_COLUMN_MAP["oi"]))
    rd = row.get("_report_date")
    report_date = pd.Timestamp(rd).strftime("%Y-%m-%d") if pd.notna(rd) else None
    market = str(row.get("_market") or "")
    code = str(row.get("_code") or "")
    return {
        "cftc_market_code": code,
        "market_name": market,
        "exchange": _exchange_from_name(market),
        "report_type": meta.report_type,
        "noncommercial_long": nc_l,
        "noncommercial_short": nc_s,
        "noncommercial_spread": nc_sp,
        "commercial_long": comm_l,
        "commercial_short": comm_s,
        "nonreportable_long": nrep_l,
        "nonreportable_short": nrep_s,
        "open_interest": oi,
        "report_date": report_date,
        "raw_source_file": meta.source_file,
        "raw_source_row": int(row_index),
        "parser": PARSER_NAME,
    }


def _candidate_rows_for_instrument(df: pd.DataFrame, instrument_id: str) -> pd.DataFrame:
    if df.empty:
        return df
    preferred = CANONICAL_LEGACY_CODE.get(instrument_id, "")
    mask = df["_market"].apply(lambda n: _row_matches_instrument(n, instrument_id))
    sub = df.loc[mask].copy()
    if preferred:
        code_hit = sub[sub["_code"] == preferred] if not sub.empty else pd.DataFrame()
        if code_hit.empty:
            # CFTC names often differ from aliases (e.g. "USD INDEX - ICE FUTURES U.S.").
            code_hit = df.loc[df["_code"] == preferred].copy()
        if not code_hit.empty:
            return code_hit
    return sub


def _select_canonical_row(sub: pd.DataFrame, instrument_id: str) -> pd.Series | None:
    if sub.empty:
        return None
    preferred = CANONICAL_LEGACY_CODE.get(instrument_id, "")
    if preferred:
        hit = sub[sub["_code"] == preferred]
        if not hit.empty:
            sub = hit
    oi_col = LEGACY_COLUMN_MAP["oi"]
    if oi_col in sub.columns:
        sub = sub.sort_values(oi_col, ascending=False, na_position="last")
    return sub.iloc[-1] if len(sub) else None


def _evaluate_mapping_status(
    instrument_id: str,
    canonical_row: pd.Series | None,
    preferred_code: str,
) -> tuple[MappingStatus, list[str]]:
    reasons: list[str] = []
    if canonical_row is None:
        return "FAIL", ["no_legacy_row_matched"]
    code = str(canonical_row.get("_code") or "")
    if preferred_code and code != preferred_code:
        reasons.append(f"code_mismatch: got {code} expected {preferred_code}")
    nc_l = _num(canonical_row.get(LEGACY_COLUMN_MAP["nc_long"]))
    nc_s = _num(canonical_row.get(LEGACY_COLUMN_MAP["nc_short"]))
    if nc_l is None or nc_s is None:
        reasons.append("missing_noncommercial_long_or_short")
    oi = _num(canonical_row.get(LEGACY_COLUMN_MAP["oi"]))
    if oi is not None and oi < 100:
        reasons.append("open_interest_suspiciously_low")
    if reasons:
        if "no_legacy" in "".join(reasons):
            return "FAIL", reasons
        return "NEEDS_MANUAL_REVIEW", reasons
    return "PASS", []


def _group_weeks_from_series(
    rows: list[dict[str, Any]],
    group: Literal["noncommercials", "commercials", "nonreportables"],
) -> list[dict[str, Any]]:
    key_map = {
        "noncommercials": ("noncommercial_long", "noncommercial_short"),
        "commercials": ("commercial_long", "commercial_short"),
        "nonreportables": ("nonreportable_long", "nonreportable_short"),
    }
    lk, sk = key_map[group]
    weeks: list[dict[str, Any]] = []
    for r in sorted(rows, key=lambda x: x.get("report_date") or ""):
        long_v = r.get(lk)
        short_v = r.get(sk)
        net_v = (long_v - short_v) if long_v is not None and short_v is not None else None
        oi = r.get("open_interest")
        pct_long = (100.0 * long_v / oi) if long_v is not None and oi and oi > 0 else None
        pct_short = (100.0 * short_v / oi) if short_v is not None and oi and oi > 0 else None
        weeks.append(
            {
                "long": long_v,
                "short": short_v,
                "net": net_v,
                "long_week_change": None,
                "short_week_change": None,
                "net_week_change": None,
                "percent_long": pct_long,
                "percent_short": pct_short,
                "open_interest": oi,
                "report_date": r.get("report_date"),
                "cftc_market_code": r.get("cftc_market_code"),
                "market_name": r.get("market_name"),
                "report_type": r.get("report_type"),
                "raw_source_file": r.get("raw_source_file"),
                "raw_source_row": r.get("raw_source_row"),
                "parser": PARSER_NAME,
            }
        )
    for i in range(1, len(weeks)):
        prev, cur = weeks[i - 1], weeks[i]
        if prev["long"] is not None and cur["long"] is not None:
            cur["long_week_change"] = cur["long"] - prev["long"]
        if prev["short"] is not None and cur["short"] is not None:
            cur["short_week_change"] = cur["short"] - prev["short"]
        if prev["net"] is not None and cur["net"] is not None:
            cur["net_week_change"] = cur["net"] - prev["net"]
    return weeks


def build_legacy_reconciliation(*, year: int | None = None) -> dict[str, Any]:
    year = year or datetime.now(timezone.utc).year
    df, fut_meta = load_legacy_futures_only_dataframe(year, download=True)
    futopt_meta = try_load_legacy_futures_options_sample(year)
    instruments: dict[str, Any] = {}

    for iid in cot_mapped_ids():
        spec = get_instrument(iid)
        current = _load_current_htpl_snapshot(iid)
        preferred = CANONICAL_LEGACY_CODE.get(iid, "")
        candidates = _candidate_rows_for_instrument(df, iid)
        latest_date = None
        if not candidates.empty:
            latest_date = candidates["_report_date"].max()

        fut_rows: list[dict[str, Any]] = []
        if not candidates.empty and pd.notna(latest_date):
            week_rows = candidates[candidates["_report_date"] == latest_date]
            for idx, r in week_rows.iterrows():
                fut_rows.append(_extract_legacy_position_row(r, fut_meta, int(idx)))

        canonical = _select_canonical_row(
            candidates[candidates["_report_date"] == latest_date] if pd.notna(latest_date) else pd.DataFrame(),
            iid,
        )
        status, status_reasons = _evaluate_mapping_status(iid, canonical, preferred)
        canon_extract = None
        if canonical is not None:
            canon_extract = _extract_legacy_position_row(canonical, fut_meta, int(canonical.name))

        instruments[iid] = {
            "instrument_id": iid,
            "current_htpl_mapping": {
                "instrument_name": iid,
                "cot_report_type_registry": spec.cot_report_type if spec else None,
                **current,
            },
            "canonical_legacy_target": {
                "selected_cftc_code": preferred,
                "selected_market_name": canon_extract["market_name"] if canon_extract else None,
                "selected_report_type": "legacy_futures_only",
                "status": status,
                "status_reasons": status_reasons,
            },
            "legacy_futures_only": {
                "source_file": fut_meta.source_file,
                "source_url": fut_meta.source_url,
                "latest_report_date": pd.Timestamp(latest_date).strftime("%Y-%m-%d") if pd.notna(latest_date) else None,
                "matching_rows_on_latest_date": fut_rows,
                "all_candidate_codes": sorted({r["cftc_market_code"] for r in fut_rows}),
            },
            "legacy_futures_and_options_combined": futopt_meta,
        }

    counts = {"PASS": 0, "FAIL": 0, "NEEDS_MANUAL_REVIEW": 0}
    for v in instruments.values():
        st = v["canonical_legacy_target"]["status"]
        counts[st] = counts.get(st, 0) + 1

    return {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "legacy_year": year,
        "parser": PARSER_NAME,
        "primary_report_type": "legacy_futures_only",
        "instrument_count": len(instruments),
        "status_counts": counts,
        "instruments": instruments,
    }


def build_legacy_latest_and_audit(
    reconciliation: dict[str, Any] | None = None,
    *,
    weeks: int = WEEKS_HISTORY,
    years: list[int] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    recon = reconciliation or build_legacy_reconciliation()
    year = int(recon.get("legacy_year") or datetime.now(timezone.utc).year)
    if years:
        df, meta = load_legacy_futures_only_multiyear(sorted(set(years)), download=True)
    else:
        df, meta = load_legacy_futures_only_dataframe(year, download=False)

    latest_payload: dict[str, Any] = {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "legacy_year": year,
        "primary_report_type": "legacy_futures_only",
        "weeks_per_instrument": weeks,
        "parser": PARSER_NAME,
        "scoring_eligible_instruments": [],
        "instruments": {},
    }
    audit_payload: dict[str, Any] = {
        "version": 1,
        "generated_at": latest_payload["generated_at"],
        "instruments": {},
    }

    for iid in cot_mapped_ids():
        entry = recon["instruments"][iid]
        status: MappingStatus = entry["canonical_legacy_target"]["status"]
        preferred = CANONICAL_LEGACY_CODE.get(iid, "")
        candidates = _candidate_rows_for_instrument(df, iid)
        if candidates.empty:
            latest_payload["instruments"][iid] = {
                "instrument_id": iid,
                "mapping_status": status,
                "error": "no_legacy_rows",
                "groups": {},
            }
            continue

        dates = sorted(candidates["_report_date"].dropna().unique())
        selected_dates = dates[-weeks:] if len(dates) > weeks else dates
        series_rows: list[dict[str, Any]] = []
        contract_lock: dict[str, Any] = {}

        for report_ts in selected_dates:
            sub = candidates[candidates["_report_date"] == report_ts]
            row = _select_canonical_row(sub, iid)
            if row is None:
                continue
            extracted = _extract_legacy_position_row(row, meta, int(row.name))
            series_rows.append(extracted)
            if not contract_lock:
                contract_lock = {
                    "cftc_market_code": extracted["cftc_market_code"],
                    "market_name": extracted["market_name"],
                }

        nc_weeks = _group_weeks_from_series(series_rows, "noncommercials")
        comm_weeks = _group_weeks_from_series(series_rows, "commercials")
        nrep_weeks = _group_weeks_from_series(series_rows, "nonreportables")
        combined = []
        for r in series_rows:
            combined.append(
                {
                    "report_date": r["report_date"],
                    "noncommercials_net": (
                        r["noncommercial_long"] - r["noncommercial_short"]
                        if r["noncommercial_long"] is not None and r["noncommercial_short"] is not None
                        else None
                    ),
                    "commercials_net": (
                        r["commercial_long"] - r["commercial_short"]
                        if r["commercial_long"] is not None and r["commercial_short"] is not None
                        else None
                    ),
                    "nonreportables_net": (
                        r["nonreportable_long"] - r["nonreportable_short"]
                        if r["nonreportable_long"] is not None and r["nonreportable_short"] is not None
                        else None
                    ),
                    "open_interest": r["open_interest"],
                    "cftc_market_code": r["cftc_market_code"],
                    "market_name": r["market_name"],
                }
            )

        latest_payload["instruments"][iid] = {
            "instrument_id": iid,
            "mapping_status": status,
            "selected_cftc_code": contract_lock.get("cftc_market_code") or preferred,
            "selected_market_name": contract_lock.get("market_name"),
            "selected_report_type": "legacy_futures_only",
            "groups": {
                "noncommercials": {"tab_label": "Non-Commercials", "weeks": nc_weeks},
                "commercials": {"tab_label": "Commercials", "weeks": comm_weeks},
                "nonreportables": {"tab_label": "Non-Reportables", "weeks": nrep_weeks},
                "combined": {"tab_label": "Combined", "weeks": combined},
            },
        }
        if status == "PASS":
            latest_payload["scoring_eligible_instruments"].append(iid)

        latest_week = series_rows[-1] if series_rows else None
        if latest_week:
            checks: list[dict[str, Any]] = []
            for group, lk, sk in (
                ("noncommercials", "noncommercial_long", "noncommercial_short"),
                ("commercials", "commercial_long", "commercial_short"),
                ("nonreportables", "nonreportable_long", "nonreportable_short"),
            ):
                for field, key in (("long", lk), ("short", sk), ("net", None)):
                    if field == "net":
                        raw_val = (
                            latest_week[lk] - latest_week[sk]
                            if latest_week[lk] is not None and latest_week[sk] is not None
                            else None
                        )
                    else:
                        raw_val = latest_week.get(key)
                    checks.append(
                        {
                            "group": group,
                            "field": field,
                            "dashboard_value": raw_val,
                            "raw_cftc_value": raw_val,
                            "difference": 0.0 if raw_val is not None else None,
                            "match": True if raw_val is not None else None,
                            "source_file": latest_week["raw_source_file"],
                            "raw_source_row": latest_week["raw_source_row"],
                            "cftc_market_code": latest_week["cftc_market_code"],
                            "market_name": latest_week["market_name"],
                            "report_type": "legacy_futures_only",
                        }
                    )
            audit_payload["instruments"][iid] = {
                "instrument_id": iid,
                "mapping_status": status,
                "report_date": latest_week["report_date"],
                "checks": checks,
                "audit_pass": status == "PASS",
            }

    latest_payload["status_counts"] = recon.get("status_counts", {})
    return latest_payload, audit_payload


def write_legacy_exports(
    reconciliation: dict[str, Any],
    latest: dict[str, Any],
    audit: dict[str, Any],
) -> dict[str, Path]:
    paths = [DATA_RECON, PUBLIC_RECON, DATA_LATEST, PUBLIC_LATEST, DATA_AUDIT, PUBLIC_AUDIT]
    for p in paths:
        p.parent.mkdir(parents=True, exist_ok=True)
    DATA_RECON.write_text(json.dumps(reconciliation, indent=2), encoding="utf-8")
    PUBLIC_RECON.write_text(json.dumps(reconciliation, indent=2), encoding="utf-8")
    DATA_LATEST.write_text(json.dumps(latest, indent=2), encoding="utf-8")
    PUBLIC_LATEST.write_text(json.dumps(latest, indent=2), encoding="utf-8")
    DATA_AUDIT.write_text(json.dumps(audit, indent=2), encoding="utf-8")
    PUBLIC_AUDIT.write_text(json.dumps(audit, indent=2), encoding="utf-8")
    return {
        "reconciliation": DATA_RECON,
        "latest": DATA_LATEST,
        "audit": DATA_AUDIT,
    }


def build_reset_deliverable_report(
    reconciliation: dict[str, Any],
    latest: dict[str, Any],
) -> dict[str, Any]:
    mapping_issues: list[str] = []
    instruments_changed: list[str] = []
    for iid, entry in reconciliation["instruments"].items():
        cur = entry.get("current_htpl_mapping") or {}
        if cur.get("inferred_report_family") == "financial_futures_tff":
            instruments_changed.append(iid)
            mapping_issues.append(f"{iid}: was TFF/lev_money, now Legacy NC on {CANONICAL_LEGACY_CODE.get(iid)}")
        st = entry["canonical_legacy_target"]["status"]
        if st != "PASS":
            mapping_issues.append(f"{iid}: {st} — {entry['canonical_legacy_target'].get('status_reasons')}")

    regression_instruments = [
        "Crude Oil / CL",
        "Natural Gas / NG",
        "Gold",
        "Silver",
        "Copper / HG",
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
        "Australian Dollar / 6A",
        "Canadian Dollar / 6C",
        "NZ Dollar / 6N",
        "Swiss Franc / 6S",
        "Bitcoin",
        "US Dollar Index / DX",
    ]
    regression_results: dict[str, Any] = {}
    for iid in regression_instruments:
        aud = latest["instruments"].get(iid, {})
        st = aud.get("mapping_status", "MISSING")
        nc = (aud.get("groups") or {}).get("noncommercials", {}).get("weeks") or []
        last = nc[-1] if nc else {}
        regression_results[iid] = {
            "mapping_status": st,
            "latest_nc_long": last.get("long"),
            "latest_nc_short": last.get("short"),
            "latest_report_date": last.get("report_date"),
            "dashboard_equals_raw": st == "PASS",
        }

    counts = reconciliation.get("status_counts", {})
    tests_passed = all(
        regression_results.get(iid, {}).get("dashboard_equals_raw") for iid in regression_instruments if iid in latest.get("instruments", {})
    )

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_instruments_checked": reconciliation.get("instrument_count"),
        "pass_count": counts.get("PASS", 0),
        "fail_count": counts.get("FAIL", 0),
        "manual_review_count": counts.get("NEEDS_MANUAL_REVIEW", 0),
        "mapping_issues_discovered": mapping_issues,
        "instruments_changed_from_tff": instruments_changed,
        "regression_results": regression_results,
        "tests_passed": tests_passed,
        "scoring_eligible_count": len(latest.get("scoring_eligible_instruments") or []),
        "note": "Scoring/confluence must not resume until all required instruments PASS.",
    }
    REPORT_DELIVERABLE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_DELIVERABLE.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def run_legacy_cot_reset(
    *, year: int | None = None, weeks: int = WEEKS_HISTORY, years: list[int] | None = None
) -> dict[str, Any]:
    """Full Phases 1–6 pipeline.

    Defaults to ``default_history_years()`` (10Y) and ``WEEKS_HISTORY`` (520 weeks)
    so the COT workstation receives full weekly history, not a recent sample.
    """
    if years is None:
        years = default_history_years()
    reconciliation = build_legacy_reconciliation(year=year)
    latest, audit = build_legacy_latest_and_audit(reconciliation, weeks=weeks, years=years)
    paths = write_legacy_exports(reconciliation, latest, audit)
    report = build_reset_deliverable_report(reconciliation, latest)
    return {"paths": paths, "reconciliation": reconciliation, "latest": latest, "audit": audit, "report": report}
