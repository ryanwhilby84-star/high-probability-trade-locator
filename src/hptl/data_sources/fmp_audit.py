"""Focused FMP connectivity audit — specific endpoints for seasonality coverage.

Usage:
    python -m hptl.data_sources.fmp_audit

Writes:
    data/audits/fmp_audit.json
    data/audits/fmp_audit.md

Audit-only. Does not modify HPTL production pipelines.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from typing import Any

from hptl.config import DATA_DIR, get_fmp_api_key
from hptl.data_sources.fmp_client import FmpClient, redact_secrets
from hptl.data_sources.fmp_config import load_fmp_provider_config

AUDIT_JSON = DATA_DIR / "audits" / "fmp_audit.json"
AUDIT_MD = DATA_DIR / "audits" / "fmp_audit.md"


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
    return []


def _date_span(rows: list[dict[str, Any]]) -> tuple[str | None, str | None]:
    dates: list[str] = []
    for row in rows:
        for key in ("date", "Date", "datetime"):
            val = row.get(key)
            if val:
                dates.append(str(val)[:10])
                break
    if not dates:
        return None, None
    dates.sort()
    return dates[0], dates[-1]


def _probe(
    client: FmpClient,
    *,
    endpoint: str,
    path: str,
    params: dict[str, str] | None = None,
    symbol: str | None = None,
    alt_paths: tuple[tuple[str, dict[str, str] | None], ...] = (),
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    candidates = [(path, params or {})] + list(alt_paths)

    for p, q in candidates:
        result = client.probe_get(p, **(q or {}))
        payload = result.get("payload")
        rows = _extract_rows(payload)
        count = len(rows) if rows else (len(payload) if isinstance(payload, list) else 0)
        earliest, latest = _date_span(rows)

        note_parts: list[str] = []
        if result.get("note"):
            note_parts.append(str(result["note"])[:200])
        if result.get("status_code") == 402:
            note_parts.append("Premium/paid tier required for this symbol on current plan")
        if result.get("status_code") == 403 and result.get("note") and "Legacy" in str(result.get("note")):
            note_parts.append("Legacy v3 endpoint — use stable/ API instead")
        if count == 0 and result.get("ok"):
            note_parts.append("HTTP OK but zero records parsed")
        if symbol and p != path:
            note_parts.append(f"resolved via alternate path {redact_secrets(p)}")

        http_success = bool(result.get("ok") and count > 0)
        row = {
            "endpoint": endpoint,
            "symbol": symbol,
            "path": redact_secrets(p),
            "success": http_success,
            "http_ok": bool(result.get("ok")),
            "premium_gated": result.get("status_code") == 402,
            "earliest_date": earliest,
            "latest_date": latest,
            "record_count": count,
            "elapsed_ms": result.get("elapsed_ms"),
            "status_code": result.get("status_code"),
            "error": result.get("error"),
            "notes": "; ".join(note_parts) if note_parts else None,
        }
        attempts.append(row)
        if http_success or result.get("status_code") == 402:
            row["attempts"] = len(attempts)
            return row

    best = attempts[-1] if attempts else {
        "endpoint": endpoint,
        "symbol": symbol,
        "path": redact_secrets(path),
        "success": False,
        "http_ok": False,
        "earliest_date": None,
        "latest_date": None,
        "record_count": 0,
        "notes": "All symbol/path attempts failed",
    }
    best["attempts"] = len(attempts)
    best["symbols_tried"] = [a.get("path") for a in attempts]
    return best


def _hist_path(symbol: str) -> str:
    return f"api/v3/historical-price-full/{symbol}"


def _stable_hist(symbol: str) -> tuple[str, dict[str, str]]:
    return "stable/historical-price-eod/full", {"symbol": symbol}


def run_probes(client: FmpClient) -> list[dict[str, Any]]:
    return [
        _probe(
            client,
            endpoint="Forex Currency Pairs",
            path="stable/forex-list",
            alt_paths=(("api/v3/symbol/available-forex-currency-pairs", None),),
        ),
        _probe(
            client,
            endpoint="Historical Forex Data",
            path=_stable_hist("EURUSD")[0],
            params=_stable_hist("EURUSD")[1],
            symbol="EURUSD",
            alt_paths=((_hist_path("EURUSD"), None),),
        ),
        _probe(
            client,
            endpoint="Historical Forex Data",
            path=_stable_hist("NZDUSD")[0],
            params=_stable_hist("NZDUSD")[1],
            symbol="NZDUSD",
            alt_paths=((_hist_path("NZDUSD"), None),),
        ),
        _probe(
            client,
            endpoint="Historical Forex Data",
            path=_stable_hist("GBPUSD")[0],
            params=_stable_hist("GBPUSD")[1],
            symbol="GBPUSD",
            alt_paths=((_hist_path("GBPUSD"), None),),
        ),
        _probe(
            client,
            endpoint="Commodities List",
            path="stable/commodities-list",
            alt_paths=(("api/v3/symbol/available-commodities", None),),
        ),
        _probe(
            client,
            endpoint="Commodity Historical Data (Gold)",
            path=_stable_hist("GCUSD")[0],
            params=_stable_hist("GCUSD")[1],
            symbol="GCUSD",
            alt_paths=(
                _stable_hist("XAUUSD"),
                (_hist_path("GCUSD"), None),
            ),
        ),
        _probe(
            client,
            endpoint="Commodity Historical Data (Silver)",
            path=_stable_hist("SIUSD")[0],
            params=_stable_hist("SIUSD")[1],
            symbol="SIUSD",
            alt_paths=(
                _stable_hist("XAGUSD"),
                (_hist_path("SIUSD"), None),
            ),
        ),
        _probe(
            client,
            endpoint="Commodity Historical Data (Copper)",
            path=_stable_hist("HGUSD")[0],
            params=_stable_hist("HGUSD")[1],
            symbol="HGUSD",
        ),
        _probe(
            client,
            endpoint="Commodity Historical Data (Natural Gas)",
            path=_stable_hist("NGUSD")[0],
            params=_stable_hist("NGUSD")[1],
            symbol="NGUSD",
        ),
        _probe(
            client,
            endpoint="Index Historical Data (S&P 500)",
            path=_stable_hist("^GSPC")[0],
            params=_stable_hist("^GSPC")[1],
            symbol="^GSPC",
            alt_paths=(
                _stable_hist("SPY"),
                (_hist_path("^GSPC"), None),
            ),
        ),
        _probe(
            client,
            endpoint="Crypto Historical Data",
            path=_stable_hist("BTCUSD")[0],
            params=_stable_hist("BTCUSD")[1],
            symbol="BTCUSD",
            alt_paths=((_hist_path("BTCUSD"), None),),
        ),
    ]


def _assess_hptl(probes: list[dict[str, Any]]) -> dict[str, Any]:
    fx_ok = sum(
        1
        for p in probes
        if p.get("endpoint") == "Historical Forex Data" and p.get("success")
    )
    seasonality_assets = sum(
        1
        for p in probes
        if p.get("success")
        and (
            "Historical" in (p.get("endpoint") or "")
            or p.get("endpoint") == "Forex Currency Pairs"
            or p.get("endpoint") == "Commodities List"
        )
    )

    seasonality = fx_ok >= 2 and seasonality_assets >= 6
    relative_strength = fx_ok >= 2
    valuation = False

    depths = [
        p["record_count"]
        for p in probes
        if p.get("success") and p.get("record_count") and "Historical" in (p.get("endpoint") or "")
    ]
    min_depth = min(depths) if depths else 0
    max_depth = max(depths) if depths else 0

    return {
        "seasonality": {
            "can_improve": seasonality,
            "rationale": (
                "FMP returns daily EOD OHLC history for FX, commodities, indices, and crypto — "
                "suitable as a shadow/backfill source for seasonality and price_store if depth matches OANDA/AV."
                if seasonality
                else "Insufficient successful historical probes for seasonality trial."
            ),
        },
        "relative_strength": {
            "can_improve": relative_strength,
            "rationale": (
                "FX pair history supports spot validation and cross checks; RS itself is computed from "
                "HPTL confluence legs, not directly from FMP."
                if relative_strength
                else "FX historical probes failed — no RS benefit."
            ),
        },
        "valuation": {
            "can_improve": valuation,
            "rationale": (
                "This audit did not probe US treasury or macro rate endpoints. "
                "FMP cannot replace NZ/JP sovereign yield or central-bank policy adapters for FX valuation V1."
            ),
        },
        "history_depth": {
            "min_records_successful_historical": min_depth,
            "max_records_successful_historical": max_depth,
            "typical_fmp_cap_note": "Free tier often caps historical-price-full at ~5 years per request unless paid.",
        },
        "asset_coverage": {
            "forex_pairs_list": next((p.get("success") for p in probes if p.get("endpoint") == "Forex Currency Pairs"), False),
            "fx_historical_symbols_tested": fx_ok,
            "commodities_list": next((p.get("success") for p in probes if p.get("endpoint") == "Commodities List"), False),
            "commodity_historical_success": sum(
                1 for p in probes if "Commodity Historical" in (p.get("endpoint") or "") and p.get("success")
            ),
            "index_historical": next(
                (p.get("success") for p in probes if "Index Historical" in (p.get("endpoint") or "")), False
            ),
            "crypto_historical": next(
                (p.get("success") for p in probes if p.get("endpoint") == "Crypto Historical Data"), False
            ),
        },
        "missing_macro_data": [
            "Central bank policy rates (RBNZ, BoJ, ECB, etc.) — not probed in this run",
            "Non-US sovereign 2Y/10Y yields (NZ, JP) — FMP US-treasury only on other endpoints",
            "Inflation / GDP / PMI — not probed in this run",
            "Copper (HGUSD) and Natural Gas (NGUSD) historical EOD — premium-gated on free tier (HTTP 402)",
        ],
        "premium_gated_symbols": [
            p.get("symbol")
            for p in probes
            if p.get("premium_gated") and p.get("symbol")
        ],
        "history_depth_note": (
            "Successful historical probes returned ~1,255–1,826 daily rows (~5 years: 2021-06-08 → 2026-06-07). "
            "Matches FMP free-tier ~5-year cap on historical-price-eod/full."
        ),
        "recommended_hptl_use_cases": [
            "Shadow seasonality backfill for FX majors and BTC when OANDA/AV gaps exist",
            "Commodity seasonality trial (Gold, Silver, Copper, NG) after symbol mapping validation",
            "Index overlay sanity check (^GSPC/SPY) for macro relationship charts",
        ]
        if seasonality
        else ["Re-run after fixing API quota or symbol mappings before any HPTL use"],
        "free_tier_sufficient": {
            "for_this_audit": True,
            "for_daily_full_universe_refresh": False,
            "documented_daily_limit": 250,
            "probes_this_run": len(probes),
            "assessment": (
                "Free tier is enough for periodic audits and selective symbol backfills (~1 req/symbol). "
                "Not enough for daily refresh of all HPTL instruments plus macro layers."
            ),
        },
    }


def _render_md(report: dict[str, Any]) -> str:
    lines = [
        "# FMP Audit",
        "",
        f"- Generated (UTC): {report.get('generated_at_utc')}",
        f"- API key configured: {report.get('api_key_configured')}",
        f"- Key detected (length): {report.get('api_key_length')}",
        "",
        "## Probe results",
        "",
        "| Endpoint | Symbol | Success | Earliest | Latest | Records | Notes |",
        "|---|---|:---:|---|---|---:|---|",
    ]
    for p in report.get("probes") or []:
        sym = p.get("symbol") or "—"
        lines.append(
            f"| {p.get('endpoint')} | {sym} | {p.get('success')} | "
            f"{p.get('earliest_date') or '—'} | {p.get('latest_date') or '—'} | "
            f"{p.get('record_count') or 0} | {p.get('notes') or '—'} |"
        )
    hptl = report.get("hptl_assessment") or {}
    lines.extend(["", "## HPTL assessment", ""])
    for area in ("seasonality", "relative_strength", "valuation"):
        block = hptl.get(area) or {}
        lines.append(f"### {area.replace('_', ' ').title()}")
        lines.append(f"- Can improve: **{block.get('can_improve')}**")
        lines.append(f"- {block.get('rationale')}")
        lines.append("")
    ft = hptl.get("free_tier_sufficient") or {}
    lines.extend(
        [
            "## Free tier",
            "",
            f"- Assessment: {ft.get('assessment')}",
            f"- Probes this run: {ft.get('probes_this_run')}",
            "",
        ]
    )
    return "\n".join(lines)


def print_summary(report: dict[str, Any]) -> None:
    print("=" * 88)
    print("FMP AUDIT — connectivity & history depth (audit-only)")
    print("=" * 88)
    key_ok = report.get("api_key_configured")
    print(f"API key detected   : {'yes' if key_ok else 'NO'}")
    if key_ok:
        print(f"API key length     : {report.get('api_key_length')} chars (value not printed)")
    print("-" * 88)
    print(f"{'Endpoint':<42} {'OK':>4}  {'Earliest':<12} {'Latest':<12} {'Rows':>6}  Notes")
    print("-" * 88)
    for p in report.get("probes") or []:
        ep = p.get("endpoint") or ""
        if p.get("symbol"):
            ep = f"{ep} ({p['symbol']})"
        ok = "YES" if p.get("success") else "NO"
        notes = (p.get("notes") or p.get("error") or "")[:40]
        print(
            f"{ep:<42} {ok:>4}  "
            f"{(p.get('earliest_date') or '—'):<12} "
            f"{(p.get('latest_date') or '—'):<12} "
            f"{p.get('record_count') or 0:>6}  "
            f"{notes}"
        )
    print("-" * 88)
    hptl = report.get("hptl_assessment") or {}
    print(f"Seasonality improve  : {hptl.get('seasonality', {}).get('can_improve')}")
    print(f"Relative strength      : {hptl.get('relative_strength', {}).get('can_improve')}")
    print(f"Valuation improve      : {hptl.get('valuation', {}).get('can_improve')}")
    print(f"JSON                   : {AUDIT_JSON}")
    print(f"Markdown               : {AUDIT_MD}")
    print("=" * 88)


def main(argv: list[str] | None = None) -> int:
    _ = argv
    cfg = load_fmp_provider_config()
    key = get_fmp_api_key()

    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": cfg.mode,
        "api_key_configured": bool(key),
        "api_key_length": len(key) if key else 0,
        "integration_status": "audit_only — no production wiring",
        "primary_providers_unchanged": list(cfg.primary_providers),
    }

    if not key:
        report["error"] = "FMP_API_KEY not set in environment"
        report["probes"] = []
        AUDIT_JSON.parent.mkdir(parents=True, exist_ok=True)
        AUDIT_JSON.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        AUDIT_MD.write_text(_render_md(report), encoding="utf-8")
        print_summary(report)
        return 2

    client = FmpClient()
    probes = run_probes(client)
    report["probes"] = probes
    report["hptl_assessment"] = _assess_hptl(probes)
    report["summary"] = {
        "total": len(probes),
        "success": sum(1 for p in probes if p.get("success")),
        "failed": sum(1 for p in probes if not p.get("success")),
    }

    AUDIT_JSON.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    AUDIT_MD.write_text(_render_md(report), encoding="utf-8")
    print_summary(report)
    return 0 if report["summary"]["success"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
