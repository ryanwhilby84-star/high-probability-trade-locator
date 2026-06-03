"""Per-asset macro data audit + coverage/freshness summary.

Builds a structured audit from the macro relationship maps (data_status,
freshness, source series, last successful refresh, failure reason) and the FRED
refresh log. Pure aggregation — no scoring, confluence, valuation, COT, relative
strength or radar logic is involved.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.macro import fred_client, macro_freshness

PUBLIC_AUDIT_PATH = Path("web-dashboard/public/data/macro_audit_latest.json")
DIST_AUDIT_PATH = Path("web-dashboard/dist/data/macro_audit_latest.json")
MD_AUDIT_PATH = Path("data/macro_audit.md")


def _renderable(payload: dict[str, Any]) -> bool:
    return payload.get("available") is True or payload.get("carried_over") is True


def _max_iso(values: list[str | None]) -> str | None:
    real = [v for v in values if v]
    return max(real) if real else None


def build_macro_audit(maps: dict[str, Any], *, generated_at: str | None = None) -> dict[str, Any]:
    """Assemble the macro audit document from the relationship maps."""
    log = fred_client.refresh_log()
    counts = macro_freshness.empty_status_counts()
    rows: list[dict[str, Any]] = []
    available = 0
    successes: list[str | None] = []
    failures: list[str | None] = []

    for market, payload in maps.items():
        if not isinstance(payload, dict):
            continue
        status = payload.get("data_status") or (
            macro_freshness.STATUS_MISSING if payload.get("available") is not True else macro_freshness.STATUS_UNKNOWN
        )
        counts[status] = counts.get(status, 0) + 1
        renderable = _renderable(payload)
        if renderable:
            available += 1
        last_success = payload.get("last_successful_refresh")
        successes.append(last_success)
        failure_reason = payload.get("error") or payload.get("last_refresh_error")
        if failure_reason:
            failures.append(payload.get("last_refresh_error_at"))

        rows.append(
            {
                "asset": market,
                "data_source": "FRED",
                "available": renderable,
                "coverage_status": "available" if renderable else "missing",
                "data_status": status,
                "freshness": macro_freshness.status_label(status),
                "source_series_ids": payload.get("source_series_ids") or [],
                "latest_observation_date": payload.get("latest_observation_date")
                or payload.get("latest_date"),
                "latency_days": payload.get("latency_days"),
                "last_successful_refresh": last_success,
                "carried_over": bool(payload.get("carried_over")),
                "failure_reason": failure_reason,
            }
        )

    summary = {
        "total": len(rows),
        "available": available,
        "live": counts.get(macro_freshness.STATUS_LIVE, 0),
        "cached": counts.get(macro_freshness.STATUS_CACHED, 0),
        "stale": counts.get(macro_freshness.STATUS_STALE, 0),
        "warning": counts.get(macro_freshness.STATUS_WARNING, 0),
        "missing": counts.get(macro_freshness.STATUS_MISSING, 0),
        "unknown": counts.get(macro_freshness.STATUS_UNKNOWN, 0),
        "last_successful_refresh": _max_iso(successes) or log.get("last_success"),
        "last_failed_refresh": _max_iso(failures) or log.get("last_failure"),
    }

    return {
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "assets": rows,
    }


def _audit_markdown(audit: dict[str, Any]) -> str:
    s = audit.get("summary", {})
    lines = [
        "# Macro Data Audit",
        "",
        f"Generated: {audit.get('generated_at')}",
        "",
        f"- Coverage: {s.get('available')}/{s.get('total')} assets available",
        f"- Live: {s.get('live')}  Cached: {s.get('cached')}  Stale: {s.get('stale')}  "
        f"Warning: {s.get('warning')}  Missing: {s.get('missing')}",
        f"- Last successful refresh: {s.get('last_successful_refresh')}",
        f"- Last failed refresh: {s.get('last_failed_refresh')}",
        "",
        "| Asset | Source | Coverage | Freshness | Latest obs | Latency (d) | Last success | Failure reason |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in audit.get("assets", []):
        ids = ", ".join(r.get("source_series_ids") or [])
        lines.append(
            "| {asset} | {ids} | {cov} | {fresh} | {obs} | {lat} | {ls} | {fail} |".format(
                asset=r.get("asset"),
                ids=ids or "—",
                cov=r.get("coverage_status"),
                fresh=r.get("freshness"),
                obs=r.get("latest_observation_date") or "—",
                lat=r.get("latency_days") if r.get("latency_days") is not None else "—",
                ls=r.get("last_successful_refresh") or "—",
                fail=(r.get("failure_reason") or "—"),
            )
        )
    return "\n".join(lines) + "\n"


def write_macro_audit(
    audit: dict[str, Any],
    *,
    public_path: Path | None = None,
    dist_path: Path | None = None,
    md_path: Path | None = None,
) -> list[Path]:
    """Write the audit JSON (public + dist mirror) and a markdown summary."""
    written: list[Path] = []
    pub = Path(public_path or PUBLIC_AUDIT_PATH)
    pub.parent.mkdir(parents=True, exist_ok=True)
    pub.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    written.append(pub)

    dist = Path(dist_path or DIST_AUDIT_PATH)
    if dist.parent.exists():
        dist.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
        written.append(dist)

    md = Path(md_path or MD_AUDIT_PATH)
    md.parent.mkdir(parents=True, exist_ok=True)
    md.write_text(_audit_markdown(audit), encoding="utf-8")
    written.append(md)
    return written
