"""FRED Macro Valuation Blueprint audit — commodity valuation V2 feasibility.

Usage:
    python -m hptl.data_sources.audit_fred_macro_blueprint

Writes:
    data/audits/fred_macro_blueprint_audit.json
    data/audits/fred_macro_blueprint_audit.md

Audit-only. Does not modify live Copper scores or valuation formulas.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import DATA_DIR, get_fred_api_key
from hptl.data_sources.fred_client import FredAuditClient, redact_secrets

CONFIG_PATH = DATA_DIR / "config" / "fred_macro_blueprint.json"
AUDIT_JSON = DATA_DIR / "audits" / "fred_macro_blueprint_audit.json"
AUDIT_MD = DATA_DIR / "audits" / "fred_macro_blueprint_audit.md"

MISSING_VALUE = "."


def _load_blueprint_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {"series": []}
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


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


def _staleness_threshold_days(frequency: str | None, cfg: dict[str, Any]) -> int:
    bands = cfg.get("staleness_days") or {}
    f = str(frequency or "").strip()
    if f in bands:
        return int(bands[f])
    if "Daily" in f:
        return int(bands.get("Daily", 10))
    if "Weekly" in f:
        return int(bands.get("Weekly", 21))
    if "Monthly" in f:
        return int(bands.get("Monthly", 45))
    if "Quarterly" in f:
        return int(bands.get("Quarterly", 120))
    return int(bands.get("default", 45))


def _staleness_status(latest: date | None, frequency: str | None, cfg: dict[str, Any]) -> str:
    if latest is None:
        return "unknown"
    age = (date.today() - latest).days
    threshold = _staleness_threshold_days(frequency, cfg)
    if age <= threshold:
        return "fresh"
    if age <= threshold * 2:
        return "stale"
    return "discontinued_or_severely_stale"


def _valuation_usefulness(
    *,
    available: bool,
    staleness: str,
    numeric_count: int,
    copper_driver: str,
) -> str:
    if not available:
        return "unusable — fetch failed"
    if staleness == "discontinued_or_severely_stale":
        return "low — series appears stale or discontinued"
    if numeric_count < 24:
        return "low — insufficient history for macro model"
    if staleness == "stale":
        return "medium — data exists but latest print is dated"
    return f"high — suitable macro input for {copper_driver}"


def _audit_series(
    client: FredAuditClient,
    spec: dict[str, Any],
    cfg: dict[str, Any],
    *,
    tail_limit: int = 12,
) -> dict[str, Any]:
    friendly = spec.get("friendly_name") or spec.get("series_id")
    series_id = str(spec.get("series_id") or "")
    probe = client.probe_series(series_id, tail_limit=tail_limit)

    if not probe.get("ok"):
        return {
            "friendly_name": friendly,
            "series_id": series_id,
            "category": spec.get("category"),
            "copper_driver": spec.get("copper_driver"),
            "valuation_role": spec.get("valuation_role"),
            "available": False,
            "latest_date": None,
            "earliest_date": None,
            "observation_count": 0,
            "numeric_observation_count": 0,
            "missing_value_count": 0,
            "frequency": None,
            "units": None,
            "seasonal_adjustment": None,
            "last_12_values": [],
            "staleness_status": "unknown",
            "staleness_age_days": None,
            "valuation_usefulness": _valuation_usefulness(
                available=False,
                staleness="unknown",
                numeric_count=0,
                copper_driver=str(spec.get("copper_driver") or ""),
            ),
            "notes": redact_secrets(probe.get("error") or "fetch failed"),
            "elapsed_ms": probe.get("elapsed_ms"),
        }

    meta = probe.get("metadata") or {}
    all_obs = probe.get("all_observations") or []
    tail = probe.get("tail_observations") or []
    numeric = _numeric_observations(all_obs)
    missing_count = sum(
        1 for o in all_obs if str(o.get("value") or "").strip() in ("", MISSING_VALUE)
    )

    earliest = numeric[0]["date"] if numeric else None
    latest = numeric[-1]["date"] if numeric else None
    frequency = meta.get("frequency") or meta.get("frequency_short")
    latest_d = _parse_date(latest)
    staleness = _staleness_status(latest_d, str(frequency), cfg)
    age_days = (date.today() - latest_d).days if latest_d else None

    last_12 = []
    for o in _numeric_observations(tail)[:tail_limit]:
        last_12.append({"date": o["date"], "value": o["value"]})
    # If tail had gaps, backfill from numeric end
    if len(last_12) < tail_limit and numeric:
        for o in reversed(numeric[-tail_limit:]):
            if o not in last_12:
                last_12.insert(0, o)
        last_12 = last_12[-tail_limit:]

    notes: list[str] = []
    if meta.get("title"):
        notes.append(str(meta.get("title"))[:120])
    if meta.get("observation_end") and latest and str(meta.get("observation_end"))[:10] != latest:
        notes.append(f"FRED observation_end={str(meta.get('observation_end'))[:10]} vs latest numeric {latest}")
    if missing_count:
        notes.append(f"{missing_count} missing ('.') observations in fetched window")

    return {
        "friendly_name": friendly,
        "series_id": series_id,
        "category": spec.get("category"),
        "copper_driver": spec.get("copper_driver"),
        "valuation_role": spec.get("valuation_role"),
        "available": True,
        "latest_date": latest,
        "earliest_date": earliest,
        "observation_count": len(all_obs),
        "numeric_observation_count": len(numeric),
        "missing_value_count": missing_count,
        "frequency": frequency,
        "units": meta.get("units"),
        "seasonal_adjustment": meta.get("seasonal_adjustment"),
        "last_12_values": last_12,
        "staleness_status": staleness,
        "staleness_age_days": age_days,
        "valuation_usefulness": _valuation_usefulness(
            available=True,
            staleness=staleness,
            numeric_count=len(numeric),
            copper_driver=str(spec.get("copper_driver") or ""),
        ),
        "notes": "; ".join(notes) if notes else None,
        "elapsed_ms": probe.get("elapsed_ms"),
    }


def _copper_v2_feasibility(series_rows: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, Any]:
    by_driver: dict[str, list[dict[str, Any]]] = {}
    for row in series_rows:
        driver = str(row.get("copper_driver") or row.get("friendly_name") or "")
        by_driver.setdefault(driver, []).append(row)

    def _row_strength(row: dict[str, Any] | None) -> str:
        if not row or not row.get("available"):
            return "missing"
        use = str(row.get("valuation_usefulness") or "")
        stale = row.get("staleness_status")
        if use.startswith("high") and stale == "fresh":
            return "strong"
        if use.startswith("high") or use.startswith("medium"):
            return "moderate"
        return "weak"

    def _strength(driver_key: str) -> str:
        rows = by_driver.get(driver_key) or []
        if not rows:
            return "missing"
        ranks = [_row_strength(r) for r in rows]
        if "strong" in ranks:
            return "strong"
        if "moderate" in ranks:
            return "moderate"
        if all(r == "missing" for r in ranks):
            return "missing"
        return "weak"

    drivers = {
        "China manufacturing demand": _strength("China manufacturing demand"),
        "China credit impulse": _strength("China credit impulse"),
        "China industrial production": _strength("China industrial production"),
        "Global growth expectations": _strength("Global growth expectations"),
        "Supply chain pressures": _strength("Supply chain pressures"),
        "Energy cost pressure": _strength("Energy cost pressure"),
        "US yields / USD macro pressure": _strength("Yield / USD macro pressure"),
    }

    strong = [k for k, v in drivers.items() if v == "strong"]
    moderate = [k for k, v in drivers.items() if v == "moderate"]
    weak = [k for k, v in drivers.items() if v == "weak"]
    missing = [k for k, v in drivers.items() if v == "missing"]

    sufficient = len(strong) + len(moderate) >= 4 and len(missing) <= 2

    return {
        "fred_sufficient_for_copper_macro_foundation": sufficient,
        "strong_inputs": strong,
        "moderate_inputs": moderate,
        "weak_inputs": weak,
        "missing_inputs": missing,
        "additional_data_needed": [
            "LME/SHFE copper inventories and warehouse stocks",
            "China property/construction floor-space or cement output (copper-intensive)",
            "Explicit USD index (DXY) aligned to Copper / HG quote convention",
            "Mine supply disruptions / treatment-charge spreads (TC/RC)",
            "Real copper demand proxies (grid investment, EV wire demand) not in FRED blueprint",
        ],
        "keep_current_copper_valuation_low_confidence": True,
        "rationale_keep_low_confidence": (
            "Current single-factor commodity valuation (price percentile / relative score) lacks "
            "China demand, credit impulse, energy, and supply-chain macro context. Until Commodity "
            "Valuation V2 is designed, validated, and backtested, Copper scores should remain "
            "low-confidence and must not drive high-conviction over/under valued labels."
        ),
        "driver_assessment": drivers,
        "future_dashboard_fields": cfg.get("future_commodity_v2_fields") or [],
        "recommended_next_step": (
            "Build an offline shadow Commodity Valuation V2 notebook/pipeline that joins FRED "
            "blueprint series with Copper / HG weekly prices — compute z-scores and correlation "
            "stability before any live score replacement."
        ),
    }


def _fred_vs_fmp_valuation() -> dict[str, Any]:
    return {
        "fred_better_for_valuation": True,
        "fmp_better_for_seasonality_prices": True,
        "summary": (
            "FRED provides free macro economic series (yields, China proxies, OECD CLI, GSCPI, energy) "
            "required for commodity fundamental valuation. FMP provides asset price history only and "
            "cannot supply China credit, PMI, or supply-chain macro inputs. Use FRED for valuation "
            "backbone; use FMP/OANDA/AV for price/seasonality layers."
        ),
    }


def run_audit(*, write_files: bool = True) -> dict[str, Any]:
    cfg = _load_blueprint_config()
    key = get_fred_api_key()
    now = datetime.now(timezone.utc).isoformat()

    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_at_utc": now,
        "mode": "audit_only",
        "integration_status": "not wired to live Copper or commodity scores",
        "api_key_configured": bool(key),
        "api_key_length": len(key) if key else 0,
        "config_path": str(CONFIG_PATH),
    }

    if not key:
        report["error"] = "FRED_API_KEY not configured"
        report["series"] = []
        report["copper_valuation_v2_feasibility"] = _copper_v2_feasibility([], cfg)
        report["fred_vs_fmp"] = _fred_vs_fmp_valuation()
        if write_files:
            _write_reports(report)
        return report

    client = FredAuditClient()
    specs = cfg.get("series") or []
    rows = [_audit_series(client, spec, cfg) for spec in specs]

    report["series"] = rows
    report["summary"] = {
        "total": len(rows),
        "available": sum(1 for r in rows if r.get("available")),
        "failed": sum(1 for r in rows if not r.get("available")),
        "fresh": sum(1 for r in rows if r.get("staleness_status") == "fresh"),
        "stale": sum(1 for r in rows if r.get("staleness_status") == "stale"),
    }
    report["copper_valuation_v2_feasibility"] = _copper_v2_feasibility(rows, cfg)
    report["fred_vs_fmp"] = _fred_vs_fmp_valuation()
    report["commodity_v2_placeholder"] = {
        "status": "audit_only",
        "fields": cfg.get("future_commodity_v2_fields") or [],
        "note": "Not exported to dashboard until shadow model validates",
    }

    if write_files:
        _write_reports(report)
    return report


def _write_reports(report: dict[str, Any]) -> None:
    AUDIT_JSON.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    AUDIT_MD.write_text(_render_md(report), encoding="utf-8")


def _render_md(report: dict[str, Any]) -> str:
    lines = [
        "# FRED Macro Valuation Blueprint Audit",
        "",
        f"- Generated (UTC): {report.get('generated_at_utc')}",
        f"- API key configured: {report.get('api_key_configured')}",
        f"- Mode: {report.get('mode')} — {report.get('integration_status')}",
        "",
    ]
    if report.get("error"):
        lines.extend([f"**Blocked:** {report['error']}", ""])

    sm = report.get("summary") or {}
    if sm:
        lines.extend(
            [
                "## Summary",
                "",
                f"- Series available: {sm.get('available', 0)}/{sm.get('total', 0)}",
                f"- Fresh: {sm.get('fresh', 0)} | Stale: {sm.get('stale', 0)} | Failed: {sm.get('failed', 0)}",
                "",
            ]
        )

    lines.extend(
        [
            "## Series audit",
            "",
            "| Friendly name | FRED ID | Available | Latest | Earliest | Count | Freq | Staleness | Usefulness |",
            "|---|---|:---:|---|---|---:|---|---|---|",
        ]
    )
    for r in report.get("series") or []:
        lines.append(
            f"| {r.get('friendly_name')} | {r.get('series_id')} | {r.get('available')} | "
            f"{r.get('latest_date') or '—'} | {r.get('earliest_date') or '—'} | "
            f"{r.get('numeric_observation_count') or 0} | {r.get('frequency') or '—'} | "
            f"{r.get('staleness_status')} | {r.get('valuation_usefulness')} |"
        )
        if r.get("last_12_values"):
            vals = ", ".join(f"{v['date']}={v['value']}" for v in r["last_12_values"][:6])
            lines.append(f"  - Last values (sample): {vals}…")
        if r.get("notes"):
            lines.append(f"  - Notes: {r['notes']}")
    lines.append("")

    cv2 = report.get("copper_valuation_v2_feasibility") or {}
    lines.extend(
        [
            "## Copper Valuation V2 Feasibility",
            "",
            f"1. **FRED sufficient for Copper macro foundation?** {cv2.get('fred_sufficient_for_copper_macro_foundation')}",
            f"2. **Strong inputs:** {', '.join(cv2.get('strong_inputs') or []) or '—'}",
            f"3. **Moderate inputs:** {', '.join(cv2.get('moderate_inputs') or []) or '—'}",
            f"4. **Weak inputs:** {', '.join(cv2.get('weak_inputs') or []) or '—'}",
            f"5. **Missing inputs:** {', '.join(cv2.get('missing_inputs') or []) or '—'}",
            "6. **Additional data eventually needed:**",
        ]
    )
    for item in cv2.get("additional_data_needed") or []:
        lines.append(f"   - {item}")
    lines.extend(
        [
            f"7. **Keep current Copper valuation low-confidence?** {cv2.get('keep_current_copper_valuation_low_confidence')}",
            "",
            f"**Rationale:** {cv2.get('rationale_keep_low_confidence')}",
            "",
            f"**Next step:** {cv2.get('recommended_next_step')}",
            "",
        ]
    )

    fvf = report.get("fred_vs_fmp") or {}
    lines.extend(
        [
            "## FRED vs FMP for valuation",
            "",
            f"- FRED better for valuation macro: **{fvf.get('fred_better_for_valuation')}**",
            f"- FMP better for price/seasonality: **{fvf.get('fmp_better_for_seasonality_prices')}**",
            f"- {fvf.get('summary')}",
            "",
        ]
    )
    return "\n".join(lines)


def print_console_summary(report: dict[str, Any]) -> None:
    print("=" * 88)
    print("FRED MACRO VALUATION BLUEPRINT AUDIT (audit-only)")
    print("=" * 88)
    print(f"API key detected   : {'yes' if report.get('api_key_configured') else 'NO'}")
    if report.get("api_key_configured"):
        print(f"API key length     : {report.get('api_key_length')} chars (value not printed)")
    if report.get("error"):
        print(f"Status             : BLOCKED - {report['error']}")
    else:
        sm = report.get("summary") or {}
        print(f"Series pulled      : {sm.get('available', 0)}/{sm.get('total', 0)} OK")
        print(f"Fresh / stale      : {sm.get('fresh', 0)} fresh, {sm.get('stale', 0)} stale")
    print("-" * 88)
    print(f"{'Friendly name':<28} {'ID':<22} {'OK':>3}  {'Latest':<12} {'Rows':>6}  {'Stale':<12}")
    print("-" * 88)
    for r in report.get("series") or []:
        ok = "YES" if r.get("available") else "NO"
        print(
            f"{str(r.get('friendly_name') or ''):<28} "
            f"{str(r.get('series_id') or ''):<22} "
            f"{ok:>3}  "
            f"{(r.get('latest_date') or '—'):<12} "
            f"{r.get('numeric_observation_count') or 0:>6}  "
            f"{r.get('staleness_status') or '—'}"
        )
    print("-" * 88)
    cv2 = report.get("copper_valuation_v2_feasibility") or {}
    print(f"Copper V2 foundation : {cv2.get('fred_sufficient_for_copper_macro_foundation')}")
    print(f"Keep Copper low-conf : {cv2.get('keep_current_copper_valuation_low_confidence')}")
    fvf = report.get("fred_vs_fmp") or {}
    print(f"FRED > FMP valuation : {fvf.get('fred_better_for_valuation')}")
    print(f"JSON                   : {AUDIT_JSON}")
    print(f"Markdown               : {AUDIT_MD}")
    print("=" * 88)


def main(argv: list[str] | None = None) -> int:
    _ = argv
    report = run_audit(write_files=True)
    print_console_summary(report)
    if not report.get("api_key_configured"):
        return 2
    sm = report.get("summary") or {}
    if sm.get("available", 0) == 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
