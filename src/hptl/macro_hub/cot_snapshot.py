"""COT positioning snapshots for Macro Hub (legacy + 3Y percentiles + CFTC lookup)."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from typing import Any

from hptl.cot.legacy_cot import (
    _extract_legacy_position_row,
    load_legacy_futures_only_multiyear,
)
from hptl.cot.legacy_cot_loader import _week_rows_for_instrument, load_legacy_cot_document
from hptl.cot.positioning_percentiles import (
    WINDOW_WEEKS_3Y,
    empirical_percentile_rank,
)
from hptl.macro_hub.config import COT_3Y_PATHS, STALE_COT_DAYS
from hptl.macro_hub.freshness import freshness_status


def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _empty_cot_block(*, source: str, reason: str | None = None) -> dict[str, Any]:
    return {
        "long": None,
        "short": None,
        "net": None,
        "weekly_net_change": None,
        "four_week_net_change": None,
        "open_interest": None,
        "net_percentile_3y": None,
        "short_percentile_3y": None,
        "oi_percentile_3y": None,
        "report_date": None,
        "source": source,
        "freshness": {"status": "missing", "as_of": None, "age_days": None},
        "error": reason,
    }


def _percentile_or_none(window: list[float], value: float | None) -> float | None:
    if value is None or not window:
        return None
    pct = empirical_percentile_rank(window, value)
    if pct != pct:  # NaN
        return None
    return round(float(pct), 1)


def _cot_block_from_weeks(
    weeks: list[dict[str, Any]],
    *,
    source: str,
    cftc_code: str | None = None,
    market_name: str | None = None,
) -> dict[str, Any]:
    if not weeks:
        return _empty_cot_block(source=source, reason="no_weeks")

    sorted_weeks = sorted(weeks, key=lambda w: str(w.get("report_date") or ""))
    latest = sorted_weeks[-1]
    long_v = _safe_float(latest.get("long") or latest.get("nc_long"))
    short_v = _safe_float(latest.get("short") or latest.get("nc_short"))
    net_v = _safe_float(latest.get("net") or latest.get("nc_net"))
    if net_v is None and long_v is not None and short_v is not None:
        net_v = long_v - short_v
    oi = _safe_float(latest.get("open_interest"))
    report_date = str(latest.get("report_date") or "")[:10] or None

    weekly_net_change = _safe_float(latest.get("net_week_change") or latest.get("nc_net_week_change"))
    if weekly_net_change is None and len(sorted_weeks) >= 2:
        prev_net = _safe_float(sorted_weeks[-2].get("net") or sorted_weeks[-2].get("nc_net"))
        if net_v is not None and prev_net is not None:
            weekly_net_change = net_v - prev_net

    four_week_net_change = None
    if len(sorted_weeks) >= 5 and net_v is not None:
        prior = _safe_float(sorted_weeks[-5].get("net") or sorted_weeks[-5].get("nc_net"))
        if prior is not None:
            four_week_net_change = net_v - prior

    window = sorted_weeks[-WINDOW_WEEKS_3Y:]
    net_hist = [_safe_float(w.get("net") or w.get("nc_net")) for w in window]
    long_hist = [_safe_float(w.get("long") or w.get("nc_long")) for w in window]
    short_hist = [_safe_float(w.get("short") or w.get("nc_short")) for w in window]
    oi_hist = [_safe_float(w.get("open_interest")) for w in window]
    net_hist = [x for x in net_hist if x is not None]
    long_hist = [x for x in long_hist if x is not None]
    short_hist = [x for x in short_hist if x is not None]
    oi_hist = [x for x in oi_hist if x is not None]

    return {
        "long": long_v,
        "short": short_v,
        "net": net_v,
        "weekly_net_change": weekly_net_change,
        "four_week_net_change": four_week_net_change,
        "open_interest": oi,
        "net_percentile_3y": _percentile_or_none(net_hist, net_v),
        "long_percentile_3y": _percentile_or_none(long_hist, long_v),
        "short_percentile_3y": _percentile_or_none(short_hist, short_v),
        "oi_percentile_3y": _percentile_or_none(oi_hist, oi),
        "report_date": report_date,
        "cftc_code": cftc_code,
        "market_name": market_name,
        "weeks_available": len(sorted_weeks),
        "source": source,
        "freshness": freshness_status(report_date, stale_after_days=STALE_COT_DAYS),
        "error": None,
    }


def _load_cot_3y_doc() -> dict[str, Any]:
    for path in COT_3Y_PATHS:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    return {}


def _weeks_from_cot_3y(market_key: str) -> list[dict[str, Any]]:
    doc = _load_cot_3y_doc()
    block = (doc.get("markets") or {}).get(market_key)
    if not block:
        return []
    weeks: list[dict[str, Any]] = []
    for row in block.get("series") or []:
        if not isinstance(row, dict):
            continue
        weeks.append(
            {
                "report_date": row.get("date"),
                "long": row.get("institutional_long"),
                "short": row.get("institutional_short"),
                "net": row.get("institutional_net"),
                "open_interest": row.get("open_interest"),
                "net_week_change": row.get("one_week_net_change"),
            }
        )
    return weeks


def cot_block_for_instrument(instrument_id: str) -> dict[str, Any]:
    """COT from legacy_cot_latest (13w) enriched with 3Y percentiles from cot_3y when available."""
    weeks_3y = _weeks_from_cot_3y(instrument_id)
    if weeks_3y:
        return _cot_block_from_weeks(weeks_3y, source="cot_3y_series_latest.json")

    doc = load_legacy_cot_document()
    inst = (doc.get("instruments") or {}).get(instrument_id)
    if not inst:
        return _empty_cot_block(source="legacy_cot_latest.json", reason=f"{instrument_id} not mapped")

    legacy_weeks = _week_rows_for_instrument(inst)
    mapped = [
        {
            "report_date": w.get("report_date"),
            "long": w.get("nc_long"),
            "short": w.get("nc_short"),
            "net": w.get("nc_net"),
            "open_interest": w.get("open_interest"),
            "net_week_change": w.get("nc_net_week_change"),
        }
        for w in legacy_weeks
    ]
    block = _cot_block_from_weeks(
        mapped,
        source="legacy_cot_latest.json",
        cftc_code=inst.get("selected_cftc_code"),
        market_name=inst.get("selected_market_name"),
    )
    if block.get("weeks_available", 0) < WINDOW_WEEKS_3Y:
        block["percentile_note"] = "3Y percentiles require cot_3y_series export (156 weeks)."
    return block


def cot_block_from_cftc_code(
    cftc_code: str,
    *,
    label: str,
    years: list[int] | None = None,
    download: bool = False,
) -> dict[str, Any]:
    """Build COT block by scanning Legacy Futures Only history for a CFTC code."""
    if not cftc_code:
        return _empty_cot_block(source="cftc_lookup", reason="missing_cftc_code")

    now_year = datetime.now(timezone.utc).year
    year_list = years or [now_year - 2, now_year - 1, now_year]
    df, meta = load_legacy_futures_only_multiyear(year_list, download=download)
    if df.empty:
        return _empty_cot_block(
            source="cftc_legacy_futures_only",
            reason=f"no legacy history loaded for {label} ({cftc_code})",
        )

    sub = df[df["_code"] == str(cftc_code).strip()].copy()
    if sub.empty:
        return _empty_cot_block(
            source="cftc_legacy_futures_only",
            reason=f"CFTC code {cftc_code} ({label}) not found in legacy history",
        )

    sub = sub.sort_values("_report_date")
    rows: list[dict[str, Any]] = []
    for idx, row in sub.iterrows():
        extracted = _extract_legacy_position_row(row, meta, int(idx))
        rows.append(extracted)

    # De-dupe by report_date (keep last).
    by_date: dict[str, dict[str, Any]] = {}
    for r in rows:
        d = str(r.get("report_date") or "")[:10]
        if d:
            by_date[d] = r
    weeks = [
        {
            "report_date": d,
            "long": r.get("noncommercial_long"),
            "short": r.get("noncommercial_short"),
            "net": (
                (r.get("noncommercial_long") - r.get("noncommercial_short"))
                if r.get("noncommercial_long") is not None and r.get("noncommercial_short") is not None
                else None
            ),
            "open_interest": r.get("open_interest"),
        }
        for d, r in sorted(by_date.items())
    ]

    block = _cot_block_from_weeks(
        weeks,
        source=f"cftc_legacy_futures_only ({meta.source_file})",
        cftc_code=cftc_code,
        market_name=label,
    )
    return block


def cot_payload_template() -> dict[str, Any]:
    return _empty_cot_block(source="none")
