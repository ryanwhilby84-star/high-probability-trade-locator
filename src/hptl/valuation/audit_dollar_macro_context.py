"""Dollar macro context audit — TWEXBGSMTH + DGS10 (audit/shadow only).

Usage:
    set PYTHONPATH=src          (Windows cmd)
    $env:PYTHONPATH="src"      (PowerShell)
    python -m hptl.valuation.audit_dollar_macro_context

Writes:
    data/audits/dollar_macro_context_audit.json
    data/audits/dollar_macro_context_audit.md

Audit-only. Does NOT wire to live valuation, 5-pillar thesis, or frontend.
TWEXBGSMTH is macro context for FX/commodity research — NOT index valuation.
Does not emit valuation_bias, valuation_score, or Fair/Under/Overvalued labels.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from typing import Any

from hptl.config import DATA_DIR, get_fred_api_key
from hptl.data_sources.fred_client import FredAuditClient, redact_secrets

AUDIT_JSON = DATA_DIR / "audits" / "dollar_macro_context_audit.json"
AUDIT_MD = DATA_DIR / "audits" / "dollar_macro_context_audit.md"

MISSING_VALUE = "."

SERIES_SPECS: tuple[dict[str, str], ...] = (
    {
        "series_id": "TWEXBGSMTH",
        "friendly_name": "Trade Weighted USD Index (Broad)",
        "role": "dollar_strength_macro_context",
    },
    {
        "series_id": "DGS10",
        "friendly_name": "10-Year Treasury Constant Maturity",
        "role": "rates_macro_context",
    },
)

STALENESS_DAYS = {
    "Daily": 10,
    "Weekly": 21,
    "Monthly": 45,
    "Quarterly": 120,
    "default": 45,
}

INDEX_V2_REQUIRED_INPUTS = (
    "CAPE (or verified CAPE proxy)",
    "earnings yield (1/CAPE)",
    "dividend yield",
    "DGS10 (for ERP = earnings yield - DGS10)",
    "equity risk premium (ERP)",
)


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _numeric_observations(obs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for o in obs:
        d = str(o.get("date") or "")[:10]
        raw = o.get("value")
        if not d or raw is None or str(raw).strip() in ("", MISSING_VALUE):
            continue
        try:
            val = float(raw)
        except (TypeError, ValueError):
            continue
        out.append({"date": d, "value": val})
    return out


def _staleness_threshold_days(frequency: str | None) -> int:
    f = str(frequency or "").strip()
    if f in STALENESS_DAYS:
        return STALENESS_DAYS[f]
    if "Daily" in f:
        return STALENESS_DAYS["Daily"]
    if "Weekly" in f:
        return STALENESS_DAYS["Weekly"]
    if "Monthly" in f:
        return STALENESS_DAYS["Monthly"]
    if "Quarterly" in f:
        return STALENESS_DAYS["Quarterly"]
    return STALENESS_DAYS["default"]


def _staleness_status(latest: date | None, frequency: str | None) -> str:
    if latest is None:
        return "unknown"
    age = (date.today() - latest).days
    threshold = _staleness_threshold_days(frequency)
    if age <= threshold:
        return "fresh"
    if age <= threshold * 2:
        return "stale"
    return "discontinued_or_severely_stale"


def _macro_context_usefulness(
    *,
    series_id: str,
    role: str,
    available: bool,
    staleness: str,
    numeric_count: int,
) -> str:
    if not available:
        return "unusable — fetch failed"
    if staleness == "discontinued_or_severely_stale":
        return "low — series appears stale or discontinued"
    if numeric_count < 24:
        return "low — insufficient history for macro context"
    if series_id == "TWEXBGSMTH":
        if staleness == "fresh" and numeric_count >= 120:
            return "high — suitable as FX/commodity dollar-strength macro context (audit only)"
        return "medium — dollar index available but dated or shallow for full macro overlay"
    if series_id == "DGS10":
        if staleness == "fresh":
            return "high — suitable rates macro context (ERP input for Index V2; not a dollar proxy)"
        return "medium — DGS10 available but latest print is dated"
    return "medium — macro context candidate (audit only)"


def _audit_series(client: FredAuditClient, spec: dict[str, str]) -> dict[str, Any]:
    series_id = spec["series_id"]
    probe = client.probe_series(series_id, tail_limit=12)

    base = {
        "series_id": series_id,
        "friendly_name": spec["friendly_name"],
        "role": spec["role"],
        "is_index_valuation_input": False,
        "valuation_bias_emitted": False,
        "valuation_score_emitted": False,
    }

    if not probe.get("ok"):
        return {
            **base,
            "available": False,
            "earliest_date": None,
            "latest_date": None,
            "observation_count": 0,
            "numeric_observation_count": 0,
            "frequency": None,
            "units": None,
            "staleness_status": "unknown",
            "staleness_age_days": None,
            "macro_context_usefulness": _macro_context_usefulness(
                series_id=series_id,
                role=spec["role"],
                available=False,
                staleness="unknown",
                numeric_count=0,
            ),
            "last_12_values": [],
            "error": redact_secrets(probe.get("error") or "fetch failed"),
        }

    meta = probe.get("metadata") or {}
    all_obs = probe.get("all_observations") or []
    tail = probe.get("tail_observations") or []
    numeric = _numeric_observations(all_obs)
    earliest = numeric[0]["date"] if numeric else None
    latest = numeric[-1]["date"] if numeric else None
    frequency = meta.get("frequency") or meta.get("frequency_short")
    latest_d = _parse_date(latest)
    staleness = _staleness_status(latest_d, str(frequency))
    age_days = (date.today() - latest_d).days if latest_d else None

    last_12 = [{"date": o["date"], "value": o["value"]} for o in _numeric_observations(tail)[:12]]

    warnings: list[str] = []
    if series_id == "TWEXBGSMTH":
        warnings.append(
            "TWEXBGSMTH is a broad trade-weighted dollar index — macro context only. "
            "It does NOT replace CAPE, earnings yield, dividend yield, or ERP for index valuation."
        )
        warnings.append(
            "Do not emit valuation_bias, valuation_score, or Undervalued/Overvalued/Fair Value from this series."
        )
    if series_id == "DGS10":
        warnings.append(
            "DGS10 is a rates input for ERP in Index Valuation V2 — not a substitute for CAPE or dollar index valuation."
        )

    return {
        **base,
        "available": True,
        "earliest_date": earliest,
        "latest_date": latest,
        "observation_count": len(all_obs),
        "numeric_observation_count": len(numeric),
        "frequency": frequency,
        "units": meta.get("units"),
        "seasonal_adjustment": meta.get("seasonal_adjustment"),
        "staleness_status": staleness,
        "staleness_age_days": age_days,
        "macro_context_usefulness": _macro_context_usefulness(
            series_id=series_id,
            role=spec["role"],
            available=True,
            staleness=staleness,
            numeric_count=len(numeric),
        ),
        "last_12_values": last_12,
        "fred_title": (meta.get("title") or "")[:200] or None,
        "warnings": warnings,
        "error": None,
    }


def build_audit() -> dict[str, Any]:
    client = FredAuditClient()
    key_ok = bool(get_fred_api_key())

    if not key_ok:
        series_rows = [
            {
                "series_id": spec["series_id"],
                "friendly_name": spec["friendly_name"],
                "role": spec["role"],
                "available": False,
                "error": "FRED_API_KEY not set",
                "is_index_valuation_input": False,
                "valuation_bias_emitted": False,
                "valuation_score_emitted": False,
            }
            for spec in SERIES_SPECS
        ]
    else:
        series_rows = [_audit_series(client, spec) for spec in SERIES_SPECS]

    twex = next((r for r in series_rows if r.get("series_id") == "TWEXBGSMTH"), {})
    dgs10 = next((r for r in series_rows if r.get("series_id") == "DGS10"), {})

    twex_usable = (
        twex.get("available")
        and twex.get("staleness_status") in ("fresh", "stale")
        and (twex.get("numeric_observation_count") or 0) >= 24
    )

    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parser": "hptl.valuation.audit_dollar_macro_context",
        "audit_only": True,
        "live_wired": False,
        "fred_api_key_configured": key_ok,
        "not_index_valuation": True,
        "prohibited_outputs": [
            "valuation_bias",
            "valuation_score",
            "Undervalued",
            "Overvalued",
            "Fair Value",
        ],
        "summary": {
            "twexbgsmth_audit_only": True,
            "twexbgsmth_usable_as_fx_commodity_macro_context": twex_usable,
            "twexbgsmth_is_not_cape_or_erp_substitute": True,
            "index_valuation_v2_still_requires": list(INDEX_V2_REQUIRED_INPUTS),
            "dgs10_available": bool(dgs10.get("available")),
            "explicit_warning": (
                "TWEXBGSMTH may inform FX/commodity macro context in shadow audits only. "
                "It must never drive index valuation_bias, valuation_score, or fair-value labels. "
                "Index Valuation V2 still requires verified CAPE, earnings yield, dividend yield, and ERP."
            ),
        },
        "series": series_rows,
    }


def _render_markdown(report: dict[str, Any]) -> str:
    s = report.get("summary") or {}
    lines = [
        "# Dollar Macro Context Audit (TWEXBGSMTH + DGS10)",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        "**Audit-only / shadow** — not wired to live valuation, 5-pillar thesis, or frontend.",
        "",
        "## Hard rules",
        "",
        "- **No** `valuation_bias` or `valuation_score` from this audit",
        "- **No** Undervalued / Overvalued / Fair Value labels from TWEXBGSMTH",
        "- TWEXBGSMTH is **not** a CAPE, earnings yield, dividend yield, or ERP substitute",
        "",
        f"**Warning:** {s.get('explicit_warning')}",
        "",
        "## Index Valuation V2 still requires",
        "",
    ]
    for item in s.get("index_valuation_v2_still_requires") or []:
        lines.append(f"- {item}")

    lines.extend(
        [
            "",
            "## Summary",
            "",
            f"- FRED API key configured: **{report.get('fred_api_key_configured')}**",
            f"- TWEXBGSMTH usable as FX/commodity macro context: **{s.get('twexbgsmth_usable_as_fx_commodity_macro_context')}**",
            f"- DGS10 available: **{s.get('dgs10_available')}**",
            "",
            "## Series",
            "",
            "| Series | Available | Earliest | Latest | Count | Frequency | Staleness | Macro context |",
            "|---|:---:|---|---|---:|---|---|---|",
        ]
    )

    for row in report.get("series") or []:
        lines.append(
            "| {sid} | {ok} | {earliest} | {latest} | {n} | {freq} | {stale} | {use} |".format(
                sid=row.get("series_id"),
                ok="Yes" if row.get("available") else "No",
                earliest=row.get("earliest_date") or "—",
                latest=row.get("latest_date") or "—",
                n=row.get("numeric_observation_count") or row.get("observation_count") or 0,
                freq=row.get("frequency") or "—",
                stale=row.get("staleness_status") or "—",
                use=str(row.get("macro_context_usefulness") or "—")[:48],
            )
        )

    lines.append("")
    for row in report.get("series") or []:
        warns = row.get("warnings") or []
        if warns:
            lines.append(f"### {row.get('series_id')} warnings")
            for w in warns:
                lines.append(f"- {w}")
            lines.append("")

    return "\n".join(lines)


def write_exports(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or build_audit()
    AUDIT_JSON.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    AUDIT_MD.write_text(_render_markdown(payload), encoding="utf-8")
    return {"json": AUDIT_JSON, "md": AUDIT_MD}


def run() -> dict[str, Any]:
    payload = build_audit()
    paths = write_exports(payload)
    s = payload["summary"]
    print("DOLLAR MACRO CONTEXT AUDIT (audit-only)")
    print(f"JSON: {paths['json']}")
    print(f"MD:   {paths['md']}")
    print(f"TWEXBGSMTH audit-only: {s.get('twexbgsmth_audit_only')}")
    print(f"FX/commodity macro context usable: {s.get('twexbgsmth_usable_as_fx_commodity_macro_context')}")
    print(f"Index V2 still requires: {', '.join(s.get('index_valuation_v2_still_requires') or [])}")
    return payload


def main() -> int:
    run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
