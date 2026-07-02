"""FX Seasonality Coverage Audit — daily history depth per FX pair.

Usage:
    python -m hptl.seasonality.fx_seasonality_coverage_audit

Writes:
    data/audits/fx_seasonality_coverage.json
    data/audits/fx_seasonality_coverage.md

Audit-only. Does not modify live seasonality or valuation logic.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from typing import Any

from hptl.config import DATA_DIR
from hptl.markets.instrument_registry import get_instrument, load_registry
from hptl.prices.coverage import load_price_coverage, select_price_source
from hptl.prices.data_integrity import actual_fetch_meta
from hptl.prices.price_store import load_price_store
from hptl.seasonality.seasonality_price_bars import resolve_price_record
from hptl.seasonality.seasonality_v2 import normalize_daily_bars, years_spanned

AUDIT_JSON = DATA_DIR / "audits" / "fx_seasonality_coverage.json"
AUDIT_MD = DATA_DIR / "audits" / "fx_seasonality_coverage.md"

MIN_YEARS_FOR_10Y_SEASONALITY = 10.0
MIN_DAILY_BARS_FOR_10Y = 252 * 8  # ~8 years of trading days (practical floor)


def _fx_pair_ids() -> list[str]:
    reg = load_registry()
    legacy_fx = [
        iid
        for iid in reg
        if reg[iid].asset_class == "fx" and "/" not in iid
    ]
    crosses = sorted(iid for iid in reg if reg[iid].asset_class == "fx" and "/" in iid)
    return sorted(legacy_fx) + crosses


def _audit_one_pair(
    pair_id: str,
    *,
    instruments: dict[str, Any],
    cov: dict[str, Any],
) -> dict[str, Any]:
    spec = get_instrument(pair_id)
    rec, store_key, resolve_fail = resolve_price_record(pair_id, instruments)

    daily = normalize_daily_bars((rec or {}).get("daily") or [])
    earliest = daily[0]["date"] if daily else None
    latest = daily[-1]["date"] if daily else None
    total_bars = len(daily)
    years = round(years_spanned(daily), 2) if daily else 0.0

    fetch_src, actual_symbol, store_daily_n, _weekly_n, fetch_err = actual_fetch_meta(
        store_key or pair_id,
        public=instruments,
        cov=cov,
    )
    if not fetch_src:
        fetch_src = select_price_source(pair_id, cov) or select_price_source(store_key or "", cov)

    data_source = fetch_src or "none"
    if store_key and store_key != pair_id:
        data_source = f"{data_source} (via {store_key})" if data_source != "none" else f"proxy:{store_key}"

    can_10y = (
        total_bars >= MIN_DAILY_BARS_FOR_10Y
        and years >= MIN_YEARS_FOR_10Y_SEASONALITY
    )

    warnings: list[str] = []
    if resolve_fail:
        warnings.append(f"price_resolve: {resolve_fail}")
    if fetch_err:
        warnings.append(f"price_fetch_error: {fetch_err}")
    if total_bars == 0:
        warnings.append("No daily bars in price store for this pair.")
    elif years < MIN_YEARS_FOR_10Y_SEASONALITY:
        warnings.append(
            f"Only {years:.1f} years of daily history — 10-year seasonality not supported."
        )
    if store_key and store_key != pair_id:
        warnings.append(f"Daily bars resolved from COT proxy instrument {store_key}.")

    if can_10y:
        status = "PASS"
    elif total_bars > 0:
        status = "WARN"
    else:
        status = "FAIL"

    return {
        "pair": pair_id,
        "subgroup": spec.subgroup if spec else None,
        "cot_proxy_of": spec.cot_proxy_of if spec else None,
        "price_store_key": store_key,
        "data_source": data_source,
        "fetch_symbol": actual_symbol,
        "earliest_daily_bar": earliest,
        "latest_daily_bar": latest,
        "total_daily_bars": total_bars,
        "store_daily_bars_reported": store_daily_n,
        "years_of_coverage": years,
        "can_calculate_10y_seasonality": can_10y,
        "status": status,
        "warnings": warnings,
    }


def build_audit() -> dict[str, Any]:
    instruments = load_price_store().get("instruments") or {}
    try:
        cov = load_price_coverage()
    except FileNotFoundError:
        cov = {}

    pairs = _fx_pair_ids()
    rows = [_audit_one_pair(pid, instruments=instruments, cov=cov) for pid in pairs]

    can_10y = [r["pair"] for r in rows if r["can_calculate_10y_seasonality"]]
    with_daily = [r for r in rows if r["total_daily_bars"] > 0]
    no_daily = [r["pair"] for r in rows if r["total_daily_bars"] == 0]

    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parser": "hptl.seasonality.fx_seasonality_coverage_audit",
        "audit_only": True,
        "criteria": {
            "min_years_for_10y_seasonality": MIN_YEARS_FOR_10Y_SEASONALITY,
            "min_daily_bars_for_10y": MIN_DAILY_BARS_FOR_10Y,
        },
        "summary": {
            "fx_pairs_audited": len(rows),
            "with_daily_bars": len(with_daily),
            "without_daily_bars": len(no_daily),
            "can_10y_seasonality": len(can_10y),
            "can_10y_pairs": can_10y,
            "missing_daily_pairs": no_daily,
            "pass": sum(1 for r in rows if r["status"] == "PASS"),
            "warn": sum(1 for r in rows if r["status"] == "WARN"),
            "fail": sum(1 for r in rows if r["status"] == "FAIL"),
        },
        "pairs": rows,
    }


def _render_markdown(report: dict[str, Any]) -> str:
    s = report.get("summary") or {}
    crit = report.get("criteria") or {}
    lines = [
        "# FX Seasonality Coverage Audit",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        "**Audit-only** — does not modify live seasonality logic.",
        "",
        "## Summary",
        "",
        f"- FX pairs audited: **{s.get('fx_pairs_audited', 0)}**",
        f"- With daily bars: **{s.get('with_daily_bars', 0)}**",
        f"- Without daily bars: **{s.get('without_daily_bars', 0)}**",
        f"- Can calculate 10-year seasonality: **{s.get('can_10y_seasonality', 0)}**",
        f"- PASS / WARN / FAIL: **{s.get('pass', 0)}** / **{s.get('warn', 0)}** / **{s.get('fail', 0)}**",
        "",
        f"10-year eligibility requires ≥{crit.get('min_years_for_10y_seasonality')} years coverage "
        f"and ≥{crit.get('min_daily_bars_for_10y')} daily bars.",
        "",
    ]
    if s.get("can_10y_pairs"):
        lines.append(f"**10Y-ready pairs:** {', '.join(s['can_10y_pairs'])}")
        lines.append("")
    if s.get("missing_daily_pairs"):
        lines.append(f"**No daily data:** {', '.join(s['missing_daily_pairs'][:20])}"
                     + (" …" if len(s["missing_daily_pairs"]) > 20 else ""))
        lines.append("")

    lines.extend(
        [
            "## Per-pair coverage",
            "",
            "| Pair | Source | Earliest | Latest | Bars | Years | 10Y? | Status |",
            "|---|---|---|---:|---:|---:|:---:|---|",
        ]
    )
    for row in report.get("pairs") or []:
        lines.append(
            "| {pair} | {src} | {earliest} | {latest} | {bars} | {years:.1f} | {y10} | {status} |".format(
                pair=row.get("pair"),
                src=str(row.get("data_source") or "—")[:28],
                earliest=row.get("earliest_daily_bar") or "—",
                latest=row.get("latest_daily_bar") or "—",
                bars=row.get("total_daily_bars"),
                years=float(row.get("years_of_coverage") or 0),
                y10="Yes" if row.get("can_calculate_10y_seasonality") else "No",
                status=row.get("status"),
            )
        )

    lines.append("")
    flagged = [r for r in (report.get("pairs") or []) if r.get("warnings")]
    if flagged:
        lines.append("## Warnings")
        lines.append("")
        for row in flagged:
            lines.append(f"### {row.get('pair')}")
            for w in row.get("warnings") or []:
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
    print("FX SEASONALITY COVERAGE AUDIT (audit-only)")
    print(f"JSON: {paths['json']}")
    print(f"MD:   {paths['md']}")
    print(
        f"Pairs={s['fx_pairs_audited']} with_daily={s['with_daily_bars']} "
        f"10Y_ready={s['can_10y_seasonality']} PASS/WARN/FAIL="
        f"{s['pass']}/{s['warn']}/{s['fail']}"
    )
    return payload


def main() -> int:
    run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
