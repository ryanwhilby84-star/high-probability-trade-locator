"""Full-universe integrity audit for seasonality inputs.

This sits above the low-level daily-series audit and answers the product-level
question: can this instrument publish a seasonal edge?  It records provenance,
coverage, return sanity and a deterministic series fingerprint so the scanner
and workstation can be checked against the same canonical input.
"""

from __future__ import annotations

import hashlib
import math
import statistics
from datetime import date, datetime
from typing import Any, Iterable

from hptl.markets.instrument_registry import LEGACY_COT_MARKETS
from hptl.seasonality_workstation.integrity import audit_daily_series
from hptl.seasonality_workstation.returns import load_daily_closes

# Known source contracts that are important enough to hard-gate.  Alpha Vantage
# CORN is the CORN ETF, not CBOT corn futures; using it in the seasonal engine
# creates a unit/source discontinuity and must never publish an edge.
REQUIRED_SOURCE_FAMILIES: dict[str, tuple[str, ...]] = {
    "Corn": ("yahoo_futures", "yahoo"),
}


def _parse_date(value: str) -> date | None:
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _series_fingerprint(daily: Iterable[tuple[str, float]]) -> str:
    h = hashlib.sha256()
    for d, c in daily:
        h.update(str(d)[:10].encode("utf-8"))
        h.update(b"|")
        h.update(f"{float(c):.10g}".encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()[:20]


def _return_diagnostics(daily: list[tuple[str, float]]) -> dict[str, Any]:
    returns: list[float] = []
    for i in range(1, len(daily)):
        prev = float(daily[i - 1][1])
        cur = float(daily[i][1])
        if prev > 0 and cur > 0 and math.isfinite(prev) and math.isfinite(cur):
            returns.append(cur / prev - 1.0)

    abs_rets = sorted(abs(r) for r in returns if math.isfinite(r))
    if not abs_rets:
        return {
            "daily_return_count": 0,
            "max_abs_daily_return_pct": None,
            "p99_abs_daily_return_pct": None,
            "median_abs_daily_return_pct": None,
        }

    p99_index = min(len(abs_rets) - 1, max(0, math.ceil(len(abs_rets) * 0.99) - 1))
    return {
        "daily_return_count": len(returns),
        "max_abs_daily_return_pct": round(abs_rets[-1] * 100.0, 3),
        "p99_abs_daily_return_pct": round(abs_rets[p99_index] * 100.0, 3),
        "median_abs_daily_return_pct": round(statistics.median(abs_rets) * 100.0, 3),
    }


def _source_issues(instrument_id: str, source: str | None) -> list[str]:
    allowed = REQUIRED_SOURCE_FAMILIES.get(instrument_id)
    if not allowed:
        return []
    src = str(source or "").lower()
    if any(src == family or src.startswith(f"{family}:") for family in allowed):
        return []
    return [
        f"invalid_seasonality_source:{source or 'unknown'};expected_one_of={','.join(allowed)}"
    ]


def audit_seasonality_instrument(
    instrument_id: str,
    *,
    daily: list[tuple[str, float]] | None = None,
    source: str | None = None,
    load_error: str | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    """Return the publishability audit for one instrument."""
    today = today or date.today()
    if daily is None:
        daily, source, load_error = load_daily_closes(instrument_id)

    if load_error or not daily:
        return {
            "instrument_id": instrument_id,
            "status": "FAIL",
            "scanner_eligible": False,
            "issues": [load_error or "no_daily_history"],
            "warnings": [],
            "source": source,
            "series_fingerprint": None,
            "bar_count": 0,
        }

    base = audit_daily_series(instrument_id, daily, source=source)
    issues = list(base.get("issues") or [])
    warnings = list(base.get("warnings") or [])
    issues.extend(_source_issues(instrument_id, source))

    last_date = _parse_date(daily[-1][0]) if daily else None
    staleness_days = (today - last_date).days if last_date else None
    # A stale series is useful for historical research but should be visible. It is
    # not a structural blocker because weekends/holidays and manual refreshes exist.
    if staleness_days is not None and staleness_days > 14:
        warnings.append(f"stale_latest_bar:{staleness_days}d")

    diagnostics = _return_diagnostics(daily)
    status = "FAIL" if issues else "PASS"
    return {
        **base,
        "status": status,
        "scanner_eligible": status == "PASS",
        "issues": list(dict.fromkeys(issues)),
        "warnings": list(dict.fromkeys(warnings)),
        "series_fingerprint": _series_fingerprint(daily),
        "latest_bar_staleness_days": staleness_days,
        "return_diagnostics": diagnostics,
        "source_contract": {
            "required": instrument_id in REQUIRED_SOURCE_FAMILIES,
            "allowed": list(REQUIRED_SOURCE_FAMILIES.get(instrument_id, ())),
            "actual": source,
            "passed": not bool(_source_issues(instrument_id, source)),
        },
    }


def audit_seasonality_universe(
    *,
    instruments: Iterable[str] | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    universe = list(instruments or LEGACY_COT_MARKETS)
    rows: dict[str, dict[str, Any]] = {}
    for instrument_id in universe:
        rows[instrument_id] = audit_seasonality_instrument(instrument_id, today=today)

    passed = sorted(k for k, v in rows.items() if v.get("status") == "PASS")
    failed = sorted(k for k, v in rows.items() if v.get("status") != "PASS")
    warnings = sorted(k for k, v in rows.items() if v.get("warnings"))
    return {
        "status": "PASS" if not failed else "FAIL",
        "engine": "seasonality_universe_audit_v2",
        "asof": (today or date.today()).isoformat(),
        "instrument_count": len(universe),
        "pass_count": len(passed),
        "fail_count": len(failed),
        "warning_count": len(warnings),
        "passed": passed,
        "failed": failed,
        "warning_instruments": warnings,
        "instruments": rows,
        "policy": {
            "publish_rule": "Only PASS instruments may publish seasonal edges or alerts.",
            "source_contracts": {
                k: list(v) for k, v in REQUIRED_SOURCE_FAMILIES.items()
            },
            "fingerprint_rule": "Scanner/workstation must consume the same canonical daily series fingerprint.",
        },
    }
