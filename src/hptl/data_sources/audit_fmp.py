"""FMP endpoint audit — probe coverage for seasonality and FX valuation inputs.

Usage:
    python -m hptl.data_sources.audit_fmp

Writes:
    data/audits/fmp_endpoint_audit.json
    data/audits/fmp_endpoint_audit.md

Does NOT wire FMP into production HPTL outputs. Audit-only.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

from hptl.config import DATA_DIR, get_fmp_api_key
from hptl.data_sources.fmp_client import FmpClient, FmpApiError, redact_secrets
from hptl.data_sources.fmp_config import load_fmp_provider_config

# --------------------------------------------------------------------------- #
# Probe catalogue
# --------------------------------------------------------------------------- #

FX_SYMBOLS = ("EURUSD", "NZDUSD", "USDJPY", "AUDUSD", "GBPUSD", "USDCAD")

MARKET_SYMBOLS: dict[str, tuple[str, ...]] = {
    "commodity_gold": ("GCUSD", "XAUUSD"),
    "commodity_silver": ("SIUSD", "XAGUSD"),
    "commodity_copper": ("HGUSD", "CPER"),
    "commodity_natural_gas": ("NGUSD", "NG"),
    "commodity_wheat": ("ZWUSD", "ZOUSX"),
    "crypto_btc": ("BTCUSD",),
    "index_spx": ("^GSPC", "SPY"),
    "index_nasdaq": ("^IXIC", "QQQ"),
}

ECONOMIC_NAMES = (
    ("gdp", "GDP"),
    ("inflation", "inflationRate"),
    ("cpi", "CPI"),
    ("unemployment", "unemploymentRate"),
    ("pmi", "PMI"),
    ("fed_funds", "federalFunds"),
)


@dataclass(frozen=True)
class ProbeSpec:
    endpoint_name: str
    category: str
    path_template: str
    example_symbol: str
    hptl_modules: tuple[str, ...]
    build_path: Callable[[str], str]
    build_params: Callable[[str], dict[str, str]]
    alt_symbols: tuple[str, ...] = ()


def _hist_full_path(symbol: str) -> str:
    return f"api/v3/historical-price-full/{symbol}"


def _hist_full_params(_symbol: str) -> dict[str, str]:
    return {}


def _treasury_params(_symbol: str) -> dict[str, str]:
    today = date.today()
    return {"from": f"{today.year - 5}-01-01", "to": today.isoformat()}


def _economic_params(name: str) -> dict[str, str]:
    return {"name": name}


def _stable_forex_eod_params(symbol: str) -> dict[str, str]:
    return {"symbol": symbol}


PROBES: tuple[ProbeSpec, ...] = (
    ProbeSpec(
        endpoint_name="historical-price-full (FX daily EOD)",
        category="seasonality_fx",
        path_template="api/v3/historical-price-full/{symbol}",
        example_symbol="EURUSD",
        hptl_modules=("seasonality", "price_store"),
        build_path=_hist_full_path,
        build_params=_hist_full_params,
        alt_symbols=FX_SYMBOLS,
    ),
    ProbeSpec(
        endpoint_name="stable historical-price-eod/full (FX)",
        category="seasonality_fx",
        path_template="stable/historical-price-eod/full?symbol={symbol}",
        example_symbol="EURUSD",
        hptl_modules=("seasonality", "price_store"),
        build_path=lambda s: "stable/historical-price-eod/full",
        build_params=_stable_forex_eod_params,
        alt_symbols=FX_SYMBOLS,
    ),
    ProbeSpec(
        endpoint_name="historical-price-full (commodity daily EOD)",
        category="seasonality_commodity",
        path_template="api/v3/historical-price-full/{symbol}",
        example_symbol="GCUSD",
        hptl_modules=("seasonality", "price_store"),
        build_path=_hist_full_path,
        build_params=_hist_full_params,
        alt_symbols=MARKET_SYMBOLS["commodity_gold"]
        + MARKET_SYMBOLS["commodity_silver"]
        + MARKET_SYMBOLS["commodity_copper"]
        + MARKET_SYMBOLS["commodity_natural_gas"]
        + MARKET_SYMBOLS["commodity_wheat"],
    ),
    ProbeSpec(
        endpoint_name="historical-price-full (crypto daily EOD)",
        category="seasonality_crypto",
        path_template="api/v3/historical-price-full/{symbol}",
        example_symbol="BTCUSD",
        hptl_modules=("seasonality", "price_store"),
        build_path=_hist_full_path,
        build_params=_hist_full_params,
        alt_symbols=MARKET_SYMBOLS["crypto_btc"],
    ),
    ProbeSpec(
        endpoint_name="historical-price-full (index daily EOD)",
        category="seasonality_index",
        path_template="api/v3/historical-price-full/{symbol}",
        example_symbol="^GSPC",
        hptl_modules=("seasonality", "macro_relationship_maps"),
        build_path=_hist_full_path,
        build_params=_hist_full_params,
        alt_symbols=MARKET_SYMBOLS["index_spx"] + MARKET_SYMBOLS["index_nasdaq"],
    ),
    ProbeSpec(
        endpoint_name="treasury-rates v4 (US yields all maturities)",
        category="valuation_us_treasury",
        path_template="api/v4/treasury",
        example_symbol="US",
        hptl_modules=("fx_valuation", "macro_rates"),
        build_path=lambda _s: "api/v4/treasury",
        build_params=_treasury_params,
    ),
    ProbeSpec(
        endpoint_name="stable treasury-rates",
        category="valuation_us_treasury",
        path_template="stable/treasury-rates",
        example_symbol="US",
        hptl_modules=("fx_valuation", "macro_rates"),
        build_path=lambda _s: "stable/treasury-rates",
        build_params=_treasury_params,
    ),
    ProbeSpec(
        endpoint_name="economic indicator series",
        category="valuation_macro",
        path_template="api/v4/economic?name={name}",
        example_symbol="GDP",
        hptl_modules=("macro_context", "fx_valuation_context"),
        build_path=lambda _s: "api/v4/economic",
        build_params=lambda s: _economic_params(s),
        alt_symbols=tuple(n for _, n in ECONOMIC_NAMES),
    ),
)


# --------------------------------------------------------------------------- #
# Response parsing
# --------------------------------------------------------------------------- #


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for key in ("historical", "data", "results"):
            block = payload.get(key)
            if isinstance(block, list):
                return [r for r in block if isinstance(r, dict)]
        # Single-symbol wrapper: {"symbol": "...", "historical": [...]}
        if "historical" in payload:
            hist = payload["historical"]
            if isinstance(hist, list):
                return [r for r in hist if isinstance(r, dict)]
    return []


def _parse_dates(rows: list[dict[str, Any]]) -> tuple[str | None, str | None, int]:
    dates: list[str] = []
    for row in rows:
        for key in ("date", "Date", "datetime", "period"):
            val = row.get(key)
            if val:
                dates.append(str(val)[:10])
                break
    if not dates:
        return None, None, 0
    dates_sorted = sorted(dates)
    return dates_sorted[0], dates_sorted[-1], len(dates)


def _treasury_maturities(sample: dict[str, Any]) -> list[str]:
    keys = []
    for k in sample:
        kl = k.lower()
        if "year" in kl or kl in {"month1", "month3", "month6", "year1", "year2", "year3", "year5", "year7", "year10", "year20", "year30"}:
            keys.append(k)
    return sorted(keys)


def _confidence(
    *,
    ok: bool,
    row_count: int,
    earliest: str | None,
    latest: str | None,
    is_premium_block: bool = False,
) -> str:
    if not ok:
        return "none"
    if is_premium_block:
        return "low"
    if row_count >= 500 and earliest and latest:
        return "high"
    if row_count >= 60 and latest:
        return "medium"
    if row_count > 0:
        return "low"
    return "none"


def _is_premium_block(error: str | None, note: str | None) -> bool:
    blob = f"{error or ''} {note or ''}".lower()
    return any(
        tok in blob
        for tok in (
            "premium",
            "subscription",
            "not available under",
            "exclusive",
            "upgrade",
            "limited access",
        )
    )


# --------------------------------------------------------------------------- #
# Audit runner
# --------------------------------------------------------------------------- #


def _probe_symbol(client: FmpClient, spec: ProbeSpec, symbol: str) -> dict[str, Any]:
    path = spec.build_path(symbol)
    params = spec.build_params(symbol)
    result = client.probe_get(path, **params)

    rows: list[dict[str, Any]] = []
    extra: dict[str, Any] = {}
    if result["ok"] and result["payload"] is not None:
        rows = _extract_rows(result["payload"])
        if spec.category.startswith("valuation_us_treasury") and rows:
            extra["maturity_fields"] = _treasury_maturities(rows[0])
            extra["sample_latest"] = {k: rows[-1].get(k) for k in ("date", "year2", "year10", "year5", "year30") if k in rows[-1]}

    earliest, latest, count = _parse_dates(rows)
    premium = _is_premium_block(result.get("error"), result.get("note"))

    available = bool(result["ok"] and count > 0 and not premium)
    if result["ok"] and count == 0:
        available = False

    update_frequency = "daily" if "historical" in spec.endpoint_name.lower() or "treasury" in spec.endpoint_name.lower() else "varies"

    return {
        "endpoint_name": spec.endpoint_name,
        "category": spec.category,
        "example_symbol": symbol,
        "path": redact_secrets(path),
        "params": {k: v for k, v in params.items()},
        "available": available,
        "http_ok": result["ok"],
        "error": result.get("error"),
        "status_code": result.get("status_code"),
        "note": result.get("note"),
        "premium_gated": premium,
        "historical_depth_rows": count,
        "earliest_date": earliest,
        "latest_date": latest,
        "update_frequency": update_frequency if available else None,
        "hptl_modules": list(spec.hptl_modules),
        "confidence": _confidence(
            ok=result["ok"] and not premium,
            row_count=count,
            earliest=earliest,
            latest=latest,
            is_premium_block=premium,
        ),
        "elapsed_ms": result.get("elapsed_ms"),
        **extra,
    }


def _probe_with_fallbacks(client: FmpClient, spec: ProbeSpec) -> dict[str, Any]:
    symbols = (spec.example_symbol,) + spec.alt_symbols
    tried: list[dict[str, Any]] = []
    seen: set[str] = set()
    for sym in symbols:
        if sym in seen:
            continue
        seen.add(sym)
        row = _probe_symbol(client, spec, sym)
        tried.append(row)
        if row["available"]:
            row["symbols_tried"] = [t["example_symbol"] for t in tried]
            return row
    best = tried[0] if tried else {}
    best["symbols_tried"] = [t["example_symbol"] for t in tried]
    best["available"] = False
    return best


def _audit_fx_symbols(client: FmpClient) -> list[dict[str, Any]]:
    spec = PROBES[0]
    return [_probe_symbol(client, spec, sym) for sym in FX_SYMBOLS]


def _audit_market_symbols(client: FmpClient) -> list[dict[str, Any]]:
    spec = PROBES[2]
    rows: list[dict[str, Any]] = []
    for label, symbols in MARKET_SYMBOLS.items():
        row = _probe_with_fallbacks(
            client,
            ProbeSpec(
                endpoint_name=f"historical-price-full ({label})",
                category=f"seasonality_{label}",
                path_template=spec.path_template,
                example_symbol=symbols[0],
                hptl_modules=spec.hptl_modules,
                build_path=spec.build_path,
                build_params=spec.build_params,
                alt_symbols=symbols[1:],
            ),
        )
        row["market_bucket"] = label
        rows.append(row)
    return rows


def _valuation_gaps(endpoints: list[dict[str, Any]]) -> dict[str, Any]:
    """Explicit NZ/JP/policy-rate gap analysis for FX valuation."""
    us_treasury = [e for e in endpoints if e.get("category") == "valuation_us_treasury" and e.get("available")]
    return {
        "us_2y_10y_via_fmp": bool(us_treasury),
        "nz_2y_10y_via_fmp": False,
        "jp_2y_10y_via_fmp": False,
        "central_bank_policy_rates_via_fmp": False,
        "notes": [
            "FMP treasury endpoints cover US government yields only (not NZ/JP sovereign curves).",
            "HPTL FX valuation V1 uses BIS/MoF/RBNZ adapters for non-US legs — FMP does not replace them today.",
            "No FMP endpoint in this audit provides RBNZ or BoJ policy rates directly.",
        ],
    }


def _free_tier_assessment(probe_count: int, failures: int) -> dict[str, Any]:
    return {
        "documented_daily_limit_requests": 250,
        "probes_executed_this_run": probe_count,
        "failed_probes": failures,
        "assessment": (
            "Free tier (250 req/day) is sufficient for periodic endpoint audits and small "
            "symbol samples, but not for full-universe daily refresh of all HPTL instruments."
        ),
        "seasonality_refresh_estimate": "~1 request per instrument for daily EOD history",
        "valuation_refresh_estimate": "US treasury: 1–2 requests/day; no non-US yield coverage",
    }


def _recommendations(endpoints: list[dict[str, Any]], gaps: dict[str, Any]) -> dict[str, Any]:
    fx_ok = sum(1 for e in endpoints if e.get("category", "").startswith("seasonality_fx") and e.get("available"))
    seasonality_ok = sum(
        1 for e in endpoints if e.get("category", "").startswith("seasonality_") and e.get("available")
    )
    us_treasury_ok = gaps.get("us_2y_10y_via_fmp")

    seasonality_useful = seasonality_ok >= 2 and fx_ok >= 1
    valuation_useful = bool(us_treasury_ok)

    if not get_fmp_api_key():
        integrate = "do_not_integrate"
        upgrade = "unknown — run audit with FMP_API_KEY set"
    elif seasonality_useful and not valuation_useful:
        integrate = "audit_only_seasonality_candidate"
        upgrade = "optional — free tier may suffice for FX/commodity seasonality backfill trials"
    elif seasonality_useful and valuation_useful:
        integrate = "audit_only_dual_candidate"
        upgrade = "consider paid tier only if US-treasury redundancy is desired; non-US yields still need existing adapters"
    else:
        integrate = "do_not_integrate"
        upgrade = "not recommended until probes pass"

    return {
        "seasonality_useful": seasonality_useful,
        "valuation_useful": valuation_useful,
        "relative_strength_useful": seasonality_useful,
        "recommended_integration": integrate,
        "do_upgrade": upgrade,
        "next_action": (
            "Run a shadow seasonality compare (FMP vs OANDA/AV) on EURUSD + GCUSD + BTCUSD "
            "before any production wiring. Keep FX valuation on existing central-bank adapters."
        ),
    }


def run_audit(*, write_files: bool = True) -> dict[str, Any]:
    cfg = load_fmp_provider_config()
    now = datetime.now(timezone.utc).isoformat()

    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_at_utc": now,
        "provider": "financial_modeling_prep",
        "mode": cfg.mode,
        "enabled": cfg.enabled,
        "api_key_configured": cfg.api_key_configured,
        "primary_providers_unchanged": list(cfg.primary_providers),
        "integration_status": "audit_only — not wired to production outputs",
    }

    if not cfg.api_key_configured:
        report["endpoints"] = []
        report["fx_symbol_probes"] = []
        report["market_symbol_probes"] = []
        report["valuation_gaps"] = _valuation_gaps([])
        report["free_tier"] = _free_tier_assessment(0, 0)
        report["recommendations"] = {
            "seasonality_useful": False,
            "valuation_useful": False,
            "relative_strength_useful": False,
            "recommended_integration": "do_not_integrate",
            "do_upgrade": "unknown — set FMP_API_KEY and re-run audit",
            "next_action": "Export FMP_API_KEY in environment or .env (never commit), then: python -m hptl.data_sources.audit_fmp",
        }
        report["error"] = "FMP_API_KEY not configured"
        if write_files:
            _write_reports(report, cfg)
        return report

    client = FmpClient()
    endpoints: list[dict[str, Any]] = []

    for spec in PROBES:
        if spec.category == "valuation_macro":
            for _label, econ_name in ECONOMIC_NAMES:
                macro_spec = ProbeSpec(
                    endpoint_name=f"economic indicator ({econ_name})",
                    category=spec.category,
                    path_template=spec.path_template,
                    example_symbol=econ_name,
                    hptl_modules=spec.hptl_modules,
                    build_path=spec.build_path,
                    build_params=spec.build_params,
                )
                endpoints.append(_probe_symbol(client, macro_spec, econ_name))
        else:
            endpoints.append(_probe_with_fallbacks(client, spec))

    fx_rows = _audit_fx_symbols(client)
    market_rows = _audit_market_symbols(client)
    all_rows = endpoints + fx_rows + market_rows

    failures = sum(1 for e in all_rows if not e.get("available"))
    gaps = _valuation_gaps(all_rows)
    rec = _recommendations(all_rows, gaps)

    report["endpoints"] = endpoints
    report["fx_symbol_probes"] = fx_rows
    report["market_symbol_probes"] = market_rows
    report["valuation_gaps"] = gaps
    report["free_tier"] = _free_tier_assessment(len(all_rows), failures)
    report["recommendations"] = rec
    report["summary"] = {
        "total_probes": len(all_rows),
        "available_probes": sum(1 for e in all_rows if e.get("available")),
        "unavailable_probes": failures,
    }

    if write_files:
        _write_reports(report, cfg)
    return report


def _write_reports(report: dict[str, Any], cfg: Any) -> None:
    cfg.audit_json_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.audit_json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    cfg.audit_markdown_path.write_text(_render_markdown(report), encoding="utf-8")


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# FMP Endpoint Audit",
        "",
        f"- Generated (UTC): {report.get('generated_at_utc')}",
        f"- Mode: {report.get('mode')}",
        f"- API key configured: {report.get('api_key_configured')}",
        f"- Integration: {report.get('integration_status')}",
        "",
    ]
    if report.get("error"):
        lines.extend([f"**Blocked:** {report['error']}", ""])

    rec = report.get("recommendations") or {}
    lines.extend(
        [
            "## Recommendations",
            "",
            f"- Seasonality useful: {rec.get('seasonality_useful')}",
            f"- Valuation useful: {rec.get('valuation_useful')}",
            f"- Relative strength useful: {rec.get('relative_strength_useful')}",
            f"- Recommended integration: {rec.get('recommended_integration')}",
            f"- Upgrade guidance: {rec.get('do_upgrade')}",
            f"- Next action: {rec.get('next_action')}",
            "",
        ]
    )

    gaps = report.get("valuation_gaps") or {}
    lines.extend(
        [
            "## FX valuation gaps (non-US)",
            "",
            f"- US 2Y/10Y via FMP: {gaps.get('us_2y_10y_via_fmp')}",
            f"- NZ 2Y/10Y via FMP: {gaps.get('nz_2y_10y_via_fmp')}",
            f"- JP 2Y/10Y via FMP: {gaps.get('jp_2y_10y_via_fmp')}",
            f"- Central bank policy rates via FMP: {gaps.get('central_bank_policy_rates_via_fmp')}",
            "",
        ]
    )
    for note in gaps.get("notes") or []:
        lines.append(f"- {note}")
    lines.append("")

    ft = report.get("free_tier") or {}
    lines.extend(
        [
            "## Free tier",
            "",
            f"- Documented limit: {ft.get('documented_daily_limit_requests')} requests/day",
            f"- Probes this run: {ft.get('probes_executed_this_run')}",
            f"- Assessment: {ft.get('assessment')}",
            "",
            "## Endpoint results",
            "",
            "| Endpoint | Symbol | Available | Earliest | Latest | Rows | Confidence | HPTL modules |",
            "|---|---|:---:|---|---|---:|---|---|",
        ]
    )
    for ep in report.get("endpoints") or []:
        mods = ", ".join(ep.get("hptl_modules") or [])
        lines.append(
            f"| {ep.get('endpoint_name')} | {ep.get('example_symbol')} | {ep.get('available')} | "
            f"{ep.get('earliest_date') or '—'} | {ep.get('latest_date') or '—'} | "
            f"{ep.get('historical_depth_rows') or 0} | {ep.get('confidence')} | {mods} |"
        )
        if ep.get("error"):
            lines.append(f"  - Error: {ep.get('error')}")
    lines.append("")

    fx_rows = report.get("fx_symbol_probes") or []
    if fx_rows:
        lines.extend(
            [
                "## FX symbol probes (historical-price-full)",
                "",
                "| Symbol | Available | Earliest | Latest | Rows | Confidence |",
                "|---|:---:|---|---|---:|---|",
            ]
        )
        for ep in fx_rows:
            lines.append(
                f"| {ep.get('example_symbol')} | {ep.get('available')} | "
                f"{ep.get('earliest_date') or '—'} | {ep.get('latest_date') or '—'} | "
                f"{ep.get('historical_depth_rows') or 0} | {ep.get('confidence')} |"
            )
        lines.append("")

    return "\n".join(lines)


def print_console_summary(report: dict[str, Any]) -> None:
    print("=" * 72)
    print("FMP ENDPOINT AUDIT (audit-only — no production wiring)")
    print("=" * 72)
    print(f"API key configured : {'yes' if report.get('api_key_configured') else 'no'}")
    print(f"Mode               : {report.get('mode')}")
    print(f"Primary feeds      : unchanged ({', '.join((report.get('primary_providers_unchanged') or [])[:4])}…)")
    if report.get("error"):
        print(f"Status             : BLOCKED — {report['error']}")
    else:
        sm = report.get("summary") or {}
        print(f"Probes             : {sm.get('available_probes', 0)}/{sm.get('total_probes', 0)} available")
    rec = report.get("recommendations") or {}
    print("-" * 72)
    print(f"Seasonality useful : {rec.get('seasonality_useful')}")
    print(f"Valuation useful   : {rec.get('valuation_useful')}")
    print(f"RS useful          : {rec.get('relative_strength_useful')}")
    print(f"Integrate?         : {rec.get('recommended_integration')}")
    print(f"Upgrade?           : {rec.get('do_upgrade')}")
    cfg = load_fmp_provider_config()
    print("-" * 72)
    print(f"JSON report        : {cfg.audit_json_path}")
    print(f"Markdown report    : {cfg.audit_markdown_path}")
    print("=" * 72)


def main(argv: list[str] | None = None) -> int:
    _ = argv
    report = run_audit(write_files=True)
    print_console_summary(report)
    if not report.get("api_key_configured"):
        return 2
    sm = report.get("summary") or {}
    if sm.get("available_probes", 0) == 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
