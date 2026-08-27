"""Operational health checks for the canonical HPTL price store.

This audit answers a different question from canonical_price_audit: not merely
whether consumers are wired to the canonical timeline, but whether the stored
price history is fresh and structurally trustworthy right now.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone
from typing import Any

from hptl.config import PROCESSED_DIR
from hptl.prices.coverage import load_price_coverage, supported_instrument_ids
from hptl.prices.price_store import load_instrument_record_internal

OUT_JSON = PROCESSED_DIR / "price_health_latest.json"
OUT_MD = PROCESSED_DIR / "price_health_latest.md"

MAX_STALE_CALENDAR_DAYS = 5
MAX_RECENT_GAP_DAYS = 7
RECENT_GAP_WINDOW_BARS = 90


def _as_date(value: Any) -> date | None:
    if not value:
        return None
    text = str(value).strip()
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _finite_number(value: Any) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return number == number and number not in (float("inf"), float("-inf"))


def audit_record(instrument_id: str, *, today: date | None = None) -> dict[str, Any]:
    today = today or datetime.now(timezone.utc).date()
    rec = load_instrument_record_internal(instrument_id) or {}
    daily = list(rec.get("daily") or [])
    issues: list[str] = []
    warnings: list[str] = []
    source = rec.get("_fetched_via") or "unknown"
    historical_source = rec.get("_historical_via")
    stored_at = rec.get("stored_at")

    if not daily:
        return {
            "instrument": instrument_id,
            "status": "FAIL",
            "bars": 0,
            "last_bar": None,
            "age_days": None,
            "source": source,
            "historical_source": historical_source,
            "stored_at": stored_at,
            "issues": ["no_daily_history"],
            "warnings": [],
            "source_error": rec.get("error"),
        }

    parsed: list[tuple[date, dict[str, Any]]] = []
    invalid_dates = 0
    invalid_ohlc = 0
    impossible_ohlc = 0
    for bar in daily:
        d = _as_date(bar.get("date"))
        if d is None:
            invalid_dates += 1
            continue
        parsed.append((d, bar))
        o, h, l, c = (bar.get("open"), bar.get("high"), bar.get("low"), bar.get("close"))
        if not all(_finite_number(v) for v in (o, h, l, c)):
            invalid_ohlc += 1
            continue
        of, hf, lf, cf = map(float, (o, h, l, c))
        if hf < max(of, cf, lf) or lf > min(of, cf, hf) or hf < lf:
            impossible_ohlc += 1

    if invalid_dates:
        issues.append(f"invalid_dates:{invalid_dates}")
    if invalid_ohlc:
        issues.append(f"invalid_ohlc:{invalid_ohlc}")
    if impossible_ohlc:
        issues.append(f"impossible_ohlc:{impossible_ohlc}")

    last_bar = None
    age_days = None
    if not parsed:
        issues.append("no_parseable_daily_dates")
    else:
        dates = [d for d, _ in parsed]
        unique_dates = set(dates)
        duplicates = len(dates) - len(unique_dates)
        if duplicates:
            issues.append(f"duplicate_dates:{duplicates}")
        if dates != sorted(dates):
            issues.append("daily_history_not_sorted")

        last_bar = max(dates)
        age_days = (today - last_bar).days
        if age_days < 0:
            issues.append(f"future_last_bar:{last_bar.isoformat()}")
        elif age_days > MAX_STALE_CALENDAR_DAYS:
            issues.append(f"stale:{age_days}_calendar_days")

        recent = sorted(unique_dates)[-RECENT_GAP_WINDOW_BARS:]
        large_gaps = [
            (a, b, (b - a).days)
            for a, b in zip(recent, recent[1:])
            if (b - a).days > MAX_RECENT_GAP_DAYS
        ]
        if large_gaps:
            a, b, gap = max(large_gaps, key=lambda row: row[2])
            issues.append(f"recent_gap:{gap}_days:{a.isoformat()}->{b.isoformat()}")

    source_error = rec.get("error")
    if source_error:
        # A failed current provider fetch is operationally important even when
        # last-known-good bars remain available.
        issues.append(f"latest_fetch_error:{source_error}")

    return {
        "instrument": instrument_id,
        "status": "FAIL" if issues else ("WARN" if warnings else "PASS"),
        "bars": len(daily),
        "last_bar": last_bar.isoformat() if last_bar else None,
        "age_days": age_days,
        "source": source,
        "historical_source": historical_source,
        "stored_at": stored_at,
        "issues": issues,
        "warnings": warnings,
        "source_error": source_error,
    }


def run_health_audit(instrument_ids: list[str] | None = None) -> dict[str, Any]:
    coverage = load_price_coverage()
    ids = instrument_ids or supported_instrument_ids(coverage)
    rows = [audit_record(iid) for iid in ids]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thresholds": {
            "max_stale_calendar_days": MAX_STALE_CALENDAR_DAYS,
            "max_recent_gap_days": MAX_RECENT_GAP_DAYS,
            "recent_gap_window_bars": RECENT_GAP_WINDOW_BARS,
        },
        "summary": {
            "total": len(rows),
            "pass": sum(r["status"] == "PASS" for r in rows),
            "warn": sum(r["status"] == "WARN" for r in rows),
            "fail": sum(r["status"] == "FAIL" for r in rows),
        },
        "rows": rows,
    }


def render_md(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = [
        "# Price health audit",
        "",
        f"Generated: {report['generated_at']}",
        f"PASS {s['pass']} | WARN {s['warn']} | FAIL {s['fail']} | TOTAL {s['total']}",
        "",
        "| Instrument | Status | Source | Last bar | Age | Bars | Detail |",
        "|---|---|---|---|---:|---:|---|",
    ]
    for row in report["rows"]:
        detail = "; ".join(row["issues"] + row["warnings"]) or "ok"
        lines.append(
            f"| {row['instrument']} | {row['status']} | {row.get('source') or '—'} | "
            f"{row.get('last_bar') or '—'} | "
            f"{row.get('age_days') if row.get('age_days') is not None else '—'} | "
            f"{row.get('bars', 0)} | {detail} |"
        )
    return "\n".join(lines) + "\n"


def write_health_audit(instrument_ids: list[str] | None = None) -> dict[str, Any]:
    report = run_health_audit(instrument_ids)
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    OUT_MD.write_text(render_md(report), encoding="utf-8")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit HPTL canonical prices for freshness and integrity")
    parser.add_argument("--instrument", action="append", default=[], help="Audit only this instrument (repeatable)")
    args = parser.parse_args(argv)

    report = write_health_audit(args.instrument or None)
    s = report["summary"]
    print(f"Price health: PASS={s['pass']} WARN={s['warn']} FAIL={s['fail']} TOTAL={s['total']}")
    for row in report["rows"]:
        if row["status"] != "PASS":
            detail = "; ".join(row["issues"] + row["warnings"])
            print(
                f"  {row['status']:4} {row['instrument']}: source={row.get('source')} "
                f"last_bar={row.get('last_bar')} {detail}"
            )
    print(f"JSON: {OUT_JSON}")
    print(f"MD:   {OUT_MD}")
    return 1 if s["fail"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
