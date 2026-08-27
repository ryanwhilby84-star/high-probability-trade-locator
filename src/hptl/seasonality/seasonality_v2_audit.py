"""Seasonality V2 audit runner — data-quality and statistical-confidence audit.

Usage:
    python -m hptl.seasonality.seasonality_v2_audit
    python -m hptl.seasonality.seasonality_v2_audit --source staging --asset-class fx

Writes:
    data/audits/seasonality_v2_audit.json          (default production audit)
    data/audits/seasonality_v2_audit.md
    data/audits/seasonality_v2_fx_staging_audit.json  (staging FX validation)
    data/audits/seasonality_v2_fx_staging_audit.md
    data/audits/seasonality_v2_detail.json           (FX ISO-week diagnostics)
    data/audits/seasonality_v2_detail.md

Audit-only. Does not modify live seasonality pillar or thesis panel.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import DATA_DIR, get_fmp_api_key
from hptl.data_sources.fmp_client import FmpApiError, FmpClient
from hptl.prices.fx_oanda_backfill_feasibility_audit import TEST_PAIRS
from hptl.prices.fx_daily_backfill import STAGING_DIR, staging_path
from hptl.prices.price_store import load_all_instrument_records, load_price_store
from hptl.seasonality.seasonality_v2_detail import DETAIL_JSON, DETAIL_MD, write_detail_exports
from hptl.seasonality.seasonality_v2 import (
    AUDIT_ASSETS,
    AuditAssetSpec,
    CONF_HIGH,
    CONF_LOW,
    CONF_MEDIUM,
    MEDIUM_SAMPLE,
    TRADING_MIN_YEARS,
    compute_seasonality_v2_from_daily,
    normalize_daily_bars,
    parse_fmp_historical_payload,
    trim_to_lookback_years,
)

AUDIT_JSON = DATA_DIR / "audits" / "seasonality_v2_audit.json"
AUDIT_MD = DATA_DIR / "audits" / "seasonality_v2_audit.md"
FX_STAGING_AUDIT_JSON = DATA_DIR / "audits" / "seasonality_v2_fx_staging_audit.json"
FX_STAGING_AUDIT_MD = DATA_DIR / "audits" / "seasonality_v2_fx_staging_audit.md"

MAJOR_GAP_DAYS = 7
FX_MAJOR_PAIRS = frozenset({"EURUSD", "GBPUSD", "USDJPY", "AUDUSD"})

FX_STAGING_SPECS: tuple[AuditAssetSpec, ...] = tuple(
    AuditAssetSpec(display, "FX", (), (store_key,))
    for display, _oanda, store_key in TEST_PAIRS
)


def _bars_from_price_store(
    spec_keys: tuple[str, ...],
    instruments: dict[str, Any],
) -> tuple[list[dict[str, Any]], str | None]:
    best: list[dict[str, Any]] = []
    source: str | None = None
    for key in spec_keys:
        rec = instruments.get(key)
        if not rec:
            continue
        daily = normalize_daily_bars(rec.get("daily") or [])
        daily = trim_to_lookback_years(daily)
        if len(daily) > len(best):
            best = daily
            source = f"price_store:{key}"
    return best, source


def _bars_from_staging(spec_keys: tuple[str, ...]) -> tuple[list[dict[str, Any]], str | None]:
    best: list[dict[str, Any]] = []
    source: str | None = None
    for key in spec_keys:
        path = staging_path(key)
        if not path.exists():
            continue
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        daily = normalize_daily_bars(doc.get("daily") or [])
        daily = trim_to_lookback_years(daily)
        if len(daily) > len(best):
            best = daily
            oanda = (doc.get("_backfill") or {}).get("oanda_symbol") or "oanda"
            source = f"staging:{oanda}:{key}"
    return best, source


def _major_date_gap_warnings(
    daily: list[dict[str, Any]],
    *,
    max_gap_days: int = MAJOR_GAP_DAYS,
) -> list[str]:
    if len(daily) < 2:
        return []
    gaps: list[str] = []
    for i in range(1, len(daily)):
        d0 = datetime.strptime(str(daily[i - 1]["date"])[:10], "%Y-%m-%d")
        d1 = datetime.strptime(str(daily[i]["date"])[:10], "%Y-%m-%d")
        gap = (d1 - d0).days
        if gap > max_gap_days:
            gaps.append(
                f"Gap of {gap} calendar days between {daily[i - 1]['date']} and {daily[i]['date']}"
            )
    if not gaps:
        return []
    if len(gaps) > 5:
        return [f"{len(gaps)} date gaps > {max_gap_days} days (e.g. {gaps[0]})"]
    return gaps


def _bars_from_fmp(client: FmpClient, symbols: tuple[str, ...]) -> tuple[list[dict[str, Any]], str | None, list[str]]:
    warnings: list[str] = []
    best: list[dict[str, Any]] = []
    source: str | None = None
    for sym in symbols:
        try:
            payload = client.get(f"api/v3/historical-price-full/{sym}")
            daily = trim_to_lookback_years(parse_fmp_historical_payload(payload))
            if len(daily) > len(best):
                best = daily
                source = f"fmp:{sym}"
        except FmpApiError as exc:
            warnings.append(f"FMP {sym}: {exc}")
    return best, source, warnings


def load_daily_for_asset(
    spec: AuditAssetSpec,
    *,
    instruments: dict[str, Any],
    fmp_client: FmpClient | None,
) -> tuple[list[dict[str, Any]], str, list[str]]:
    warnings: list[str] = []
    store_bars, store_src = _bars_from_price_store(spec.price_store_keys, instruments)
    fmp_bars: list[dict[str, Any]] = []
    fmp_src: str | None = None

    if fmp_client and fmp_client.configured:
        fmp_bars, fmp_src, fmp_warn = _bars_from_fmp(fmp_client, spec.fmp_symbols)
        warnings.extend(fmp_warn)
    elif spec.fmp_symbols:
        warnings.append("FMP_API_KEY not set — FMP historical fallback skipped.")

    if len(fmp_bars) > len(store_bars):
        if store_src and store_bars:
            warnings.append(
                f"FMP extended history ({len(fmp_bars)} daily bars) vs price store ({len(store_bars)})."
            )
        return fmp_bars, fmp_src or "fmp", warnings

    if store_bars:
        if fmp_bars and len(fmp_bars) <= len(store_bars):
            warnings.append("Price store covered equal or more daily bars than FMP for this asset.")
        return store_bars, store_src or "price_store", warnings

    if fmp_bars:
        return fmp_bars, fmp_src or "fmp", warnings

    return [], "none", warnings


def load_daily_for_staging(spec: AuditAssetSpec) -> tuple[list[dict[str, Any]], str, list[str]]:
    warnings: list[str] = []
    store_bars, store_src = _bars_from_staging(spec.price_store_keys)
    if not store_bars:
        for key in spec.price_store_keys:
            path = staging_path(key)
            if not path.exists():
                warnings.append(f"Staging file missing: {path}")
        return [], store_src or "staging:none", warnings

    warnings.extend(_major_date_gap_warnings(store_bars))
    return store_bars, store_src or "staging:oanda", warnings


def _select_asset_specs(*, source: str, asset_class: str | None) -> tuple[AuditAssetSpec, ...]:
    if source == "staging":
        return FX_STAGING_SPECS
    specs: tuple[AuditAssetSpec, ...] = AUDIT_ASSETS
    if asset_class == "fx":
        return tuple(s for s in specs if s.category == "FX")
    return specs


def _empty_result(spec: AuditAssetSpec, source: str, load_warnings: list[str]) -> dict[str, Any]:
    return {
        "asset": spec.asset,
        "pair": spec.asset,
        "category": spec.category,
        "data_source": source,
        "earliest_date": None,
        "latest_date": None,
        "daily_bars": 0,
        "years_covered": 0.0,
        "distinct_years": 0,
        "current_iso_week": None,
        "sample_size": 0,
        "win_rate_pct": None,
        "avg_return_pct": None,
        "median_return_pct": None,
        "std_dev_pct": None,
        "z_score": None,
        "bias": "Neutral",
        "confidence": CONF_LOW,
        "pass_fail_status": "FAIL",
        "warnings": load_warnings
        + ["Seasonality not reliable enough for trading decisions.", "No daily price data loaded."],
        "audit_only": True,
        "live_wired": False,
    }


def _feasibility_summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    high = [r["asset"] for r in results if r.get("confidence") == CONF_HIGH]
    medium = [r["asset"] for r in results if r.get("confidence") == CONF_MEDIUM]
    low = [r["asset"] for r in results if r.get("confidence") == CONF_LOW]
    pass_assets = [r["asset"] for r in results if r.get("pass_fail_status") == "PASS"]
    reliable = [r["asset"] for r in results if (r.get("years_covered") or 0) >= TRADING_MIN_YEARS]
    need_data = [r["asset"] for r in results if (r.get("years_covered") or 0) < TRADING_MIN_YEARS]
    fmp_helped = [
        r["asset"]
        for r in results
        if str(r.get("data_source") or "").startswith("fmp")
        and (r.get("years_covered") or 0) >= TRADING_MIN_YEARS
    ]

    production_ready = len(pass_assets) >= max(3, len(results) // 3)

    return {
        "assets_tested": len(results),
        "high_confidence": high,
        "medium_confidence": medium,
        "low_confidence": low,
        "pass_count": len(pass_assets),
        "pass_assets": pass_assets,
        "reliable_history_assets": reliable,
        "needs_better_data": need_data,
        "fmp_improved_coverage": fmp_helped,
        "eligible_for_v2_scoring": pass_assets,
        "remain_disabled_or_low_confidence": sorted(set(low + need_data)),
        "production_ready": production_ready,
        "recommended_next_step": (
            "Continue Index Valuation V2 / price backfill for assets under 5 years; "
            "re-run this audit after extending daily history. "
            "Only wire Seasonality V2 to the 5-pillar panel for PASS assets with Medium+ confidence."
            if not production_ready
            else "Re-run audit on a schedule; pilot Seasonality V2 on PASS assets only after stakeholder sign-off."
        ),
        "answers": {
            "can_build_reliable_weekly_seasonality": len(reliable) > 0,
            "assets_with_enough_history": reliable,
            "assets_needing_better_data": need_data,
            "fmp_improves_coverage": len(fmp_helped) > 0,
            "eligible_for_v2_scoring": pass_assets,
            "disabled_or_low_confidence": sorted(set(low + need_data)),
        },
    }


def _pair_meets_promotion_criteria(row: dict[str, Any]) -> bool:
    years_ok = (row.get("years_covered") or 0) >= TRADING_MIN_YEARS
    sample_ok = (row.get("sample_size") or 0) >= MEDIUM_SAMPLE
    gap_warnings = [w for w in (row.get("warnings") or []) if "gap" in w.lower()]
    conf_ok = row.get("confidence") in {CONF_MEDIUM, CONF_HIGH}
    return years_ok and sample_ok and not gap_warnings and conf_ok


def _fx_staging_feasibility_summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    high = [r["asset"] for r in results if r.get("confidence") == CONF_HIGH]
    medium = [r["asset"] for r in results if r.get("confidence") == CONF_MEDIUM]
    low = [r["asset"] for r in results if r.get("confidence") == CONF_LOW]
    pass_assets = [r["asset"] for r in results if r.get("pass_fail_status") == "PASS"]

    methodology_ok = all(
        (r.get("sample_size") or 0) >= MEDIUM_SAMPLE and (r.get("years_covered") or 0) >= TRADING_MIN_YEARS
        for r in results
        if r.get("daily_bars", 0) > 0
    )
    med_high_major = [
        r["asset"]
        for r in results
        if r.get("confidence") in {CONF_MEDIUM, CONF_HIGH} and r["asset"] in FX_MAJOR_PAIRS
    ]
    promotion_pairs = [r["asset"] for r in results if _pair_meets_promotion_criteria(r)]

    recommend_promotion = (
        methodology_ok
        and len(med_high_major) >= 2
        and len(promotion_pairs) >= 3
        and all((r.get("years_covered") or 0) >= TRADING_MIN_YEARS for r in results if r.get("daily_bars"))
    )

    production_testing_ready = methodology_ok and len(pass_assets) >= 3

    return {
        "assets_tested": len(results),
        "high_confidence": high,
        "medium_confidence": medium,
        "low_confidence": low,
        "pass_count": len(pass_assets),
        "pass_assets": pass_assets,
        "promotion_criteria_met_pairs": promotion_pairs,
        "iso_weekly_methodology_sensible": methodology_ok,
        "fx_seasonality_v2_production_testing_ready": production_testing_ready,
        "recommend_staging_promotion": recommend_promotion,
        "keep_5_pillar_fx_seasonality_disabled_until_promotion": not recommend_promotion,
        "promotion_gate": {
            "min_years": TRADING_MIN_YEARS,
            "min_sample_size": MEDIUM_SAMPLE,
            "requires_medium_or_high_on_major_pairs": 2,
            "requires_promotion_ready_pairs": 3,
            "auto_promote": False,
        },
        "answers": {
            "medium_or_high_confidence": sorted(high + medium),
            "low_confidence": low,
            "iso_weekly_methodology_sensible": methodology_ok,
            "ready_for_production_testing": production_testing_ready,
            "recommend_staging_promotion": recommend_promotion,
            "keep_5_pillar_disabled_until_promotion": not recommend_promotion,
        },
        "recommended_next_step": (
            "Promote staged FX daily OHLC to production price store, re-run production "
            "seasonality_v2_audit and fx_seasonality_coverage_audit, then pilot V2 on PASS pairs."
            if recommend_promotion
            else "Do not promote yet. Review Low-confidence pairs and gap warnings; "
            "re-run staging audit after addressing data issues."
        ),
    }


def run_audit(
    *,
    source: str = "production",
    asset_class: str | None = None,
    write_files: bool = True,
    audit_json: Path | None = None,
    audit_md: Path | None = None,
) -> dict[str, Any]:
    specs = _select_asset_specs(source=source, asset_class=asset_class)
    instruments = load_price_store().get("instruments") or {}
    if not instruments:
        instruments = {
            k: {
                "daily": v.get("daily") or [],
                "weekly": v.get("weekly") or [],
                "error": v.get("error"),
            }
            for k, v in load_all_instrument_records().items()
        }

    fmp = FmpClient() if get_fmp_api_key() and source == "production" else None
    results: list[dict[str, Any]] = []

    for spec in specs:
        if source == "staging":
            daily, data_source, load_warnings = load_daily_for_staging(spec)
        else:
            daily, data_source, load_warnings = load_daily_for_asset(
                spec, instruments=instruments, fmp_client=fmp
            )

        if not daily:
            results.append(_empty_result(spec, data_source, load_warnings))
            continue

        block = compute_seasonality_v2_from_daily(
            daily,
            asset=spec.asset,
            data_source=data_source,
            extra_warnings=load_warnings,
        )
        block["category"] = spec.category
        block["pair"] = spec.asset
        results.append(block)

    if source == "staging":
        feasibility = _fx_staging_feasibility_summary(results)
        parser_name = "hptl.seasonality.seasonality_v2_audit.staging_fx"
        out_json = audit_json or FX_STAGING_AUDIT_JSON
        out_md = audit_md or FX_STAGING_AUDIT_MD
        render_fn = _render_fx_staging_markdown
    else:
        feasibility = _feasibility_summary(results)
        parser_name = "hptl.seasonality.seasonality_v2_audit"
        out_json = audit_json or AUDIT_JSON
        out_md = audit_md or AUDIT_MD
        render_fn = _render_markdown

    report = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parser": parser_name,
        "audit_only": True,
        "live_wired": False,
        "data_source_mode": source,
        "asset_class_filter": asset_class,
        "staging_dir": str(STAGING_DIR) if source == "staging" else None,
        "production_store_modified": False,
        "fmp_key_configured": bool(get_fmp_api_key()) if source == "production" else None,
        "summary": feasibility,
        "pairs": results,
        "assets": results,
    }

    if write_files:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        out_md.write_text(render_fn(report), encoding="utf-8")
        write_detail_exports(report)

    return report


def _render_markdown(report: dict[str, Any]) -> str:
    s = report.get("summary") or {}
    lines = [
        "# Seasonality V2 Audit",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        "**Audit-only** — not wired to live seasonality pillar or 5-pillar thesis panel.",
        "",
        "## Executive summary",
        "",
        f"- Assets tested: **{s.get('assets_tested', 0)}**",
        f"- High confidence: **{len(s.get('high_confidence') or [])}** — {', '.join(s.get('high_confidence') or []) or 'none'}",
        f"- Medium confidence: **{len(s.get('medium_confidence') or [])}** — {', '.join(s.get('medium_confidence') or []) or 'none'}",
        f"- Low confidence: **{len(s.get('low_confidence') or [])}** — {', '.join(s.get('low_confidence') or []) or 'none'}",
        f"- PASS (audit): **{s.get('pass_count', 0)}**",
        f"- Production-ready: **{s.get('production_ready')}**",
        "",
        "## Audit questions",
        "",
    ]
    ans = s.get("answers") or {}
    lines.extend(
        [
            f"1. Can we build reliable weekly seasonality from current data? **{ans.get('can_build_reliable_weekly_seasonality')}**",
            f"2. Assets with enough history (≥{TRADING_MIN_YEARS}y): {', '.join(ans.get('assets_with_enough_history') or []) or 'none'}",
            f"3. Assets needing better data: {', '.join(ans.get('assets_needing_better_data') or []) or 'none'}",
            f"4. FMP improves coverage: **{ans.get('fmp_improves_coverage')}** ({', '.join(s.get('fmp_improved_coverage') or []) or 'none'})",
            f"5. Eligible for V2 scoring: {', '.join(ans.get('eligible_for_v2_scoring') or []) or 'none'}",
            f"6. Remain disabled / low-confidence: {', '.join(ans.get('disabled_or_low_confidence') or []) or 'none'}",
            "",
            f"**Recommended next step:** {s.get('recommended_next_step')}",
            "",
            "## Per-asset results",
            "",
            "| Asset | Source | Years | Sample | Win% | Avg% | Z | Bias | Confidence | Status |",
            "|---|---|---:|---:|---:|---:|---:|---|---|---|",
        ]
    )

    for row in report.get("assets") or []:
        lines.append(
            "| {asset} | {src} | {years:.1f} | {n} | {win} | {avg} | {z} | {bias} | {conf} | {status} |".format(
                asset=row.get("asset"),
                src=str(row.get("data_source") or "")[:24],
                years=float(row.get("years_covered") or 0),
                n=row.get("sample_size"),
                win=row.get("win_rate_pct") if row.get("win_rate_pct") is not None else "—",
                avg=row.get("avg_return_pct") if row.get("avg_return_pct") is not None else "—",
                z=row.get("z_score") if row.get("z_score") is not None else "—",
                bias=row.get("bias"),
                conf=row.get("confidence"),
                status=row.get("pass_fail_status"),
            )
        )

    lines.append("")
    for row in report.get("assets") or []:
        warns = row.get("warnings") or []
        if warns:
            lines.append(f"### {row.get('asset')} warnings")
            for w in warns:
                lines.append(f"- {w}")
            lines.append("")

    return "\n".join(lines)


def _render_fx_staging_markdown(report: dict[str, Any]) -> str:
    s = report.get("summary") or {}
    ans = s.get("answers") or {}
    lines = [
        "# FX Seasonality V2 Staging Validation Audit",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        "**Staging validation only** — production price store not modified; live seasonality unchanged.",
        "",
        f"Data source: `{report.get('data_source_mode')}` from `{report.get('staging_dir')}`",
        "",
        "## Executive summary",
        "",
        f"- FX pairs tested: **{s.get('assets_tested', 0)}**",
        f"- High confidence: **{', '.join(s.get('high_confidence') or []) or 'none'}**",
        f"- Medium confidence: **{', '.join(s.get('medium_confidence') or []) or 'none'}**",
        f"- Low confidence: **{', '.join(s.get('low_confidence') or []) or 'none'}**",
        f"- PASS (audit): **{s.get('pass_count', 0)}** — {', '.join(s.get('pass_assets') or []) or 'none'}",
        "",
        "## Validation questions",
        "",
        f"1. Medium or High confidence pairs: **{', '.join(ans.get('medium_or_high_confidence') or []) or 'none'}**",
        f"2. Low confidence pairs: **{', '.join(ans.get('low_confidence') or []) or 'none'}**",
        f"3. ISO weekly methodology sensible? **{ans.get('iso_weekly_methodology_sensible')}**",
        f"4. FX Seasonality V2 ready for production testing? **{ans.get('ready_for_production_testing')}**",
        f"5. Recommend staging promotion? **{ans.get('recommend_staging_promotion')}** (auto-promote: **False**)",
        f"6. Keep 5-pillar FX seasonality disabled until promotion? **{ans.get('keep_5_pillar_disabled_until_promotion')}**",
        "",
        f"**Recommended next step:** {s.get('recommended_next_step')}",
        "",
        "## Per-pair results",
        "",
        "| Pair | Source | Earliest | Latest | Bars | Years | ISO Wk | Sample | Win% | Avg% | Med% | Std% | Z | Bias | Conf | Status |",
        "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|",
    ]

    for row in report.get("pairs") or []:
        lines.append(
            "| {pair} | {src} | {earliest} | {latest} | {bars} | {years:.2f} | {wk} | {n} | {win} | {avg} | {med} | {std} | {z} | {bias} | {conf} | {status} |".format(
                pair=row.get("pair") or row.get("asset"),
                src=str(row.get("data_source") or "")[:20],
                earliest=row.get("earliest_date") or "—",
                latest=row.get("latest_date") or "—",
                bars=row.get("daily_bars"),
                years=float(row.get("years_covered") or 0),
                wk=row.get("current_iso_week"),
                n=row.get("sample_size"),
                win=row.get("win_rate_pct") if row.get("win_rate_pct") is not None else "—",
                avg=row.get("avg_return_pct") if row.get("avg_return_pct") is not None else "—",
                med=row.get("median_return_pct") if row.get("median_return_pct") is not None else "—",
                std=row.get("std_dev_pct") if row.get("std_dev_pct") is not None else "—",
                z=row.get("z_score") if row.get("z_score") is not None else "—",
                bias=row.get("bias"),
                conf=row.get("confidence"),
                status=row.get("pass_fail_status"),
            )
        )

    lines.append("")
    for row in report.get("pairs") or []:
        warns = row.get("warnings") or []
        if warns:
            lines.append(f"### {row.get('pair') or row.get('asset')} warnings")
            for w in warns:
                lines.append(f"- {w}")
            lines.append("")

    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seasonality V2 audit (audit-only)")
    parser.add_argument(
        "--source",
        default="production",
        choices=["production", "staging"],
        help="Price data source (default: production price store)",
    )
    parser.add_argument(
        "--asset-class",
        default=None,
        choices=["fx", "all"],
        help="Limit audit to asset class (fx filter for production; staging uses FX backfill pairs)",
    )
    args = parser.parse_args(argv)

    if args.source == "staging" and args.asset_class not in (None, "fx"):
        parser.error("--source staging only supports --asset-class fx")

    asset_class = "fx" if args.source == "staging" else args.asset_class
    report = run_audit(source=args.source, asset_class=asset_class, write_files=True)
    s = report.get("summary") or {}

    if args.source == "staging":
        print("FX SEASONALITY V2 STAGING AUDIT (audit-only)")
        print(f"JSON: {FX_STAGING_AUDIT_JSON}")
        print(f"MD:   {FX_STAGING_AUDIT_MD}")
    else:
        print("SEASONALITY V2 AUDIT (audit-only)")
        print(f"JSON: {AUDIT_JSON}")
        print(f"MD:   {AUDIT_MD}")
    print(f"Detail JSON: {DETAIL_JSON}")
    print(f"Detail MD:   {DETAIL_MD}")

    print(f"Assets tested: {s.get('assets_tested')}")
    print(f"High:   {s.get('high_confidence')}")
    print(f"Medium: {s.get('medium_confidence')}")
    print(f"Low:    {s.get('low_confidence')}")
    if args.source == "staging":
        ans = s.get("answers") or {}
        print(f"Production testing ready: {ans.get('ready_for_production_testing')}")
        print(f"Recommend promotion: {ans.get('recommend_staging_promotion')}")
    else:
        print(f"Production-ready: {s.get('production_ready')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
