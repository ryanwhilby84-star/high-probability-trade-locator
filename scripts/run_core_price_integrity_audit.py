#!/usr/bin/env python3
"""Durable price-integrity audit for LEGACY_COT_MARKETS (canonical production universe).

Outputs:
  data/audits/core_price_integrity_audit.json
  data/audits/core_price_integrity_audit.md

Usage:
  python scripts/run_core_price_integrity_audit.py
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.markets.instrument_registry import (  # noqa: E402
    LEGACY_COT_MARKETS,
    get_instrument,
)
from hptl.seasonality_workstation.indexed_seasonality import (  # noqa: E402
    load_daily_closes_for_seasonality,
)
from hptl.seasonality_workstation.integrity import audit_daily_series  # noqa: E402
from hptl.seasonality_workstation.payload import (  # noqa: E402
    build_seasonality_workstation_payload,
)
from hptl.prices.canonical_timeline import build_canonical_timeline  # noqa: E402
from hptl.prices.price_store import load_instrument_record_internal  # noqa: E402
from hptl.prices.softs_futures_backfill import SOFTS_YAHOO  # noqa: E402

OUT_JSON = ROOT / "data" / "audits" / "core_price_integrity_audit.json"
OUT_MD = ROOT / "data" / "audits" / "core_price_integrity_audit.md"
LOOKBACK = "15Y"
REQUESTED_YEARS = 15


def _continuous_method(mid: str, source: str | None, rec: dict[str, Any]) -> str | None:
    scale = rec.get("price_scale") or {}
    if mid in SOFTS_YAHOO:
        return (
            f"yahoo_prestitched_continuous:{SOFTS_YAHOO[mid]['yahoo_symbol']} "
            "(provider roll adjustment; seasonality uses adjusted closes)"
        )
    if mid == "Corn":
        return "yahoo_prestitched_continuous:ZC=F (cents→USD/bushel; adjusted closes)"
    if "Dollar Index" in mid or "DX" in mid:
        ysym = scale.get("yahoo_symbol")
        if ysym or (source or "").startswith("yahoo"):
            return f"yahoo_prestitched_continuous:{ysym or 'DX-Y.NYB'} (ICE DX; adjusted closes)"
    if (source or "").startswith("oanda") or scale.get("source") == "oanda":
        return "oanda_cfd_continuous (broker CFD; not exchange roll-adjusted futures)"
    return scale.get("note") or source


def _classify(integ: dict[str, Any], monthly_ok: bool, weekly_ok: bool) -> str:
    if integ.get("status") == "FAIL" or not monthly_ok or not weekly_ok:
        return "FAIL — ROADMAPS UNAVAILABLE"
    if integ.get("warnings"):
        return "WARNING — ROADMAPS AVAILABLE"
    return "PASS"


def _spec_field(spec: Any, name: str, default: Any = None) -> Any:
    if spec is None:
        return default
    if isinstance(spec, dict):
        return spec.get(name, default)
    return getattr(spec, name, default)


def _exclusion_causes(
    mid: str,
    *,
    requested: list[int],
    usable: list[int],
    thin: list[int],
    first_date: str | None,
    issues: list[str],
) -> list[dict[str, Any]]:
    first_year = int(str(first_date)[:4]) if first_date else None
    out: list[dict[str, Any]] = []
    for y in requested:
        if y in usable:
            continue
        if y in thin:
            out.append({"year": y, "reason": "incomplete_year_thin_weeks"})
        elif first_year is not None and y < first_year:
            out.append({"year": y, "reason": "source_begins_later_than_requested"})
        elif any("discontinu" in i for i in issues):
            out.append({"year": y, "reason": "discontinuity_failure_blocks_series"})
        elif any("gap" in i for i in issues):
            out.append({"year": y, "reason": "excessive_gaps"})
        else:
            out.append({"year": y, "reason": "missing_or_incomplete_history"})
    return out


def audit_one(mid: str) -> dict[str, Any]:
    spec = get_instrument(mid)
    daily, meta = load_daily_closes_for_seasonality(mid)
    price_id = meta.get("price_instrument_id") or mid
    source = meta.get("source")
    integ = audit_daily_series(price_id, daily, source=source)
    store_rec = load_instrument_record_internal(price_id) or load_instrument_record_internal(mid) or {}
    scale = store_rec.get("price_scale") or {}
    provider_symbol = (
        scale.get("yahoo_symbol")
        or scale.get("symbol")
        or _spec_field(spec, "oanda_symbol")
        or _spec_field(spec, "exchange_symbol")
    )

    tl = build_canonical_timeline(price_id, apply_supplements=False)
    canonical_latest = tl.date_end if tl else None
    raw_latest = None
    raw_daily = store_rec.get("daily") or []
    if raw_daily:
        raw_latest = str(raw_daily[-1].get("date") or "")[:10]

    payload = build_seasonality_workstation_payload(mid, lookback=LOOKBACK)
    monthly = payload.get("seasonal_roadmap") or payload.get("monthly_roadmap") or {}
    weekly = payload.get("weekly_roadmap") or {}
    if payload.get("status") != "ok":
        monthly_status = "unavailable"
        weekly_status = weekly.get("quality_status") if weekly else "unavailable"
        monthly_ok = False
        weekly_ok = False
    else:
        monthly_ok = bool(monthly.get("available"))
        weekly_ok = bool(weekly.get("available"))
        monthly_status = "available" if monthly_ok else "unavailable"
        weekly_status = weekly.get("quality_status") or (
            "available" if weekly_ok else "unavailable"
        )

    cls = _classify(integ, monthly_ok, weekly_ok)
    usable = list(integ.get("usable_history_years") or [])
    thin = list(integ.get("thin_years") or [])
    asof_year = date.today().year
    requested = list(range(asof_year - REQUESTED_YEARS, asof_year))
    excluded = [y for y in requested if y not in usable]
    seasonality_latest = (
        (weekly.get("actual_price") or {}).get("latest_price_date")
        or payload.get("report_date")
        or integ.get("last_date")
    )

    # Expected Fri close for seasonality as-of calendar (today Saturday 2026-08-01 → 2026-07-31)
    today = date.today()
    expected_session = today
    # walk back to weekday
    while expected_session.weekday() >= 5:
        from datetime import timedelta

        expected_session = expected_session - timedelta(days=1)
    stale_vs_session = None
    if seasonality_latest:
        try:
            lag = (expected_session - date.fromisoformat(str(seasonality_latest)[:10])).days
            stale_vs_session = lag
        except ValueError:
            stale_vs_session = None

    return {
        "instrument": mid,
        "display_name": _spec_field(spec, "display_name", mid),
        "provider": (
            integ.get("source")
            or source
            or scale.get("source")
            or _spec_field(spec, "price_provider")
        ),
        "symbol": provider_symbol,
        "exchange_symbol": _spec_field(spec, "exchange_symbol"),
        "continuous_futures_method": _continuous_method(mid, source, store_rec),
        "earliest_date": integ.get("first_date"),
        "latest_date": integ.get("last_date"),
        "latest_date_layers": {
            "provider_latest_date": integ.get("last_date"),
            "raw_hptl_latest_date": raw_latest,
            "canonical_latest_date": canonical_latest,
            "seasonality_payload_latest_date": seasonality_latest,
            "rendered_latest_date": seasonality_latest,
            "expected_completed_session": expected_session.isoformat(),
            "lag_calendar_days_vs_expected_session": stale_vs_session,
            "stale_ingestion": bool(stale_vs_session is not None and stale_vs_session > 1),
            "first_stale_layer": (
                None
                if stale_vs_session is None or stale_vs_session <= 1
                else (
                    "raw_hptl_store"
                    if raw_latest and raw_latest < expected_session.isoformat()
                    else "canonical_or_payload"
                )
            ),
        },
        "requested_years": REQUESTED_YEARS,
        "requested_year_list": requested,
        "available_years": round(float(integ.get("available_history_years") or 0), 2),
        "valid_years": usable,
        "valid_year_count": len(usable),
        "excluded_years": excluded,
        "excluded_year_causes": _exclusion_causes(
            mid,
            requested=requested,
            usable=usable,
            thin=thin,
            first_date=integ.get("first_date"),
            issues=list(integ.get("issues") or []),
        ),
        "thin_years": thin,
        "missing_bars": None,
        "gap_count": integ.get("gap_count"),
        "discontinuity_count": integ.get("discontinuity_count"),
        "bar_count": integ.get("bar_count"),
        "integrity_status": integ.get("status"),
        "classification": cls,
        "monthly_roadmap_status": monthly_status,
        "weekly_roadmap_status": weekly_status,
        "failure_reasons": list(integ.get("issues") or [])
        + [f"warning:{w}" for w in (integ.get("warnings") or [])],
        "issues": integ.get("issues") or [],
        "warnings": integ.get("warnings") or [],
        "price_instrument_id": price_id,
        "load_meta": {
            k: meta.get(k) for k in ("source", "error", "price_instrument_id", "preferred_id")
        },
        "seasonality_payload_latest": seasonality_latest,
        "payload_status": payload.get("status"),
        "seasonality_uses_adjusted_history": True,
    }


def main() -> int:
    rows = [audit_one(mid) for mid in LEGACY_COT_MARKETS]
    pass_n = sum(1 for r in rows if r["classification"] == "PASS")
    warn_n = sum(1 for r in rows if r["classification"].startswith("WARNING"))
    fail_n = sum(1 for r in rows if r["classification"].startswith("FAIL"))
    doc = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "command": "python scripts/run_core_price_integrity_audit.py",
        "universe": "LEGACY_COT_MARKETS",
        "universe_count": len(LEGACY_COT_MARKETS),
        "universe_ids": list(LEGACY_COT_MARKETS),
        "lookback": LOOKBACK,
        "summary": {
            "PASS": pass_n,
            "WARNING — ROADMAPS AVAILABLE": warn_n,
            "FAIL — ROADMAPS UNAVAILABLE": fail_n,
        },
        "instruments": rows,
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(doc, indent=2), encoding="utf-8")

    lines = [
        "# Core Price Integrity Audit",
        "",
        f"- Generated: `{doc['generated_at']}`",
        f"- Command: `{doc['command']}`",
        f"- Universe: `{doc['universe']}` ({doc['universe_count']} instruments)",
        f"- Summary: PASS={pass_n} · WARNING={warn_n} · FAIL={fail_n}",
        "",
        "| Instrument | Provider | Symbol | Earliest | Latest | Valid yrs | Gaps | Jumps | Integrity | Monthly | Weekly | Class |",
        "|---|---|---|---|---|---:|---:|---:|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            "| {instrument} | {provider} | {symbol} | {earliest_date} | {latest_date} | {valid_year_count} | {gap_count} | {discontinuity_count} | {integrity_status} | {monthly_roadmap_status} | {weekly_roadmap_status} | {classification} |".format(
                **{k: (r.get(k) if r.get(k) is not None else "—") for k in (
                    "instrument", "provider", "symbol", "earliest_date", "latest_date",
                    "valid_year_count", "gap_count", "discontinuity_count", "integrity_status",
                    "monthly_roadmap_status", "weekly_roadmap_status", "classification",
                )}
            )
        )
    fails = [r for r in rows if r["classification"].startswith("FAIL")]
    if fails:
        lines += ["", "## Failures", ""]
        for r in fails:
            lines.append(f"### {r['instrument']}")
            lines.append(f"- Reasons: `{r['failure_reasons']}`")
            lines.append(f"- Thin years: `{r['thin_years']}`")
            lines.append("")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(doc["summary"], indent=2))
    print(f"Wrote {OUT_JSON}")
    print(f"Wrote {OUT_MD}")
    return 0 if fail_n == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
