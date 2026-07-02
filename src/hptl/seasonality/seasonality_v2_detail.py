"""Seasonality V2 detail export — per-pair ISO-week diagnostics (audit-only).

Writes:
    data/audits/seasonality_v2_detail.json
    data/audits/seasonality_v2_detail.md

Does not modify live seasonality scoring.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import DATA_DIR
from hptl.seasonality.seasonality_v2 import (
    BEAR_AVG_RETURN,
    BEAR_WIN_RATE,
    BULL_AVG_RETURN,
    BULL_WIN_RATE,
    CONF_HIGH,
    CONF_LOW,
    CONF_MEDIUM,
    HIGH_SAMPLE,
    HIGH_YEARS,
    HIGH_Z,
    MEDIUM_SAMPLE,
    MEDIUM_YEARS,
    MEDIUM_Z,
)

DETAIL_JSON = DATA_DIR / "audits" / "seasonality_v2_detail.json"
DETAIL_MD = DATA_DIR / "audits" / "seasonality_v2_detail.md"

HIGH_VOL_STD_PCT = 1.5
LOW_SIGNAL_TO_NOISE = 0.3


def diagnose_confidence(row: dict[str, Any]) -> dict[str, Any]:
    """Explain why confidence is Low/Medium/High for the current ISO week."""
    confidence = row.get("confidence") or CONF_LOW
    sample = int(row.get("sample_size") or 0)
    years = float(row.get("years_covered") or 0)
    z = row.get("z_score")
    std = row.get("std_dev_pct")
    win = row.get("win_rate_pct")
    avg = row.get("avg_return_pct")
    stale = bool(row.get("price_stale"))

    abs_z = abs(float(z)) if z is not None else None
    factors: list[dict[str, Any]] = []

    if sample < MEDIUM_SAMPLE:
        factors.append(
            {
                "code": "insufficient_sample_size",
                "label": "Insufficient sample size",
                "detail": f"sample_size={sample} < {MEDIUM_SAMPLE} required for Medium confidence",
            }
        )
    if stale:
        factors.append(
            {
                "code": "stale_prices",
                "label": "Stale prices",
                "detail": f"Latest bar is {row.get('price_age_days')} days old; confidence capped at Low",
            }
        )
    if years < MEDIUM_YEARS:
        factors.append(
            {
                "code": "insufficient_years",
                "label": "Insufficient years of coverage",
                "detail": f"years_covered={years:.2f} < {MEDIUM_YEARS} required for Medium confidence",
            }
        )
    if std is not None and std == 0:
        factors.append(
            {
                "code": "zero_volatility",
                "label": "Zero volatility",
                "detail": "std_dev_pct=0 — z-score undefined; confidence capped at Low",
            }
        )
    if z is None:
        factors.append(
            {
                "code": "no_z_score",
                "label": "No z-score",
                "detail": "Cannot compute z-score (need ≥2 samples and non-zero std dev)",
            }
        )
    elif abs_z < MEDIUM_Z:
        factors.append(
            {
                "code": "insufficient_statistical_significance",
                "label": "Insufficient statistical significance",
                "detail": (
                    f"|z_score|={abs_z:.4f} < {MEDIUM_Z} required for Medium; "
                    f"< {HIGH_Z} required for High"
                ),
            }
        )
    elif abs_z < HIGH_Z:
        factors.append(
            {
                "code": "below_high_z_threshold",
                "label": "Below High z-score threshold",
                "detail": f"|z_score|={abs_z:.4f} < {HIGH_Z} required for High confidence",
            }
        )

    if win is not None and BEAR_WIN_RATE < float(win) < BULL_WIN_RATE:
        factors.append(
            {
                "code": "weak_win_rate",
                "label": "Weak win rate (no directional edge)",
                "detail": (
                    f"win_rate={win}% in neutral band "
                    f"({BEAR_WIN_RATE}%–{BULL_WIN_RATE}%); no Bullish/Bearish bias"
                ),
            }
        )

    if avg is not None and abs(float(avg)) < BULL_AVG_RETURN:
        factors.append(
            {
                "code": "weak_average_return",
                "label": "Weak average return",
                "detail": (
                    f"|avg_return|={abs(float(avg)):.4f}% < {BULL_AVG_RETURN}% "
                    "required for directional bias"
                ),
            }
        )

    if std is not None and avg is not None and float(std) > 0:
        snr = abs(float(avg)) / float(std)
        if snr < LOW_SIGNAL_TO_NOISE:
            factors.append(
                {
                    "code": "high_volatility_relative_to_signal",
                    "label": "High volatility relative to signal",
                    "detail": (
                        f"signal/noise ratio |avg|/std={snr:.3f} < {LOW_SIGNAL_TO_NOISE} "
                        "(return drowned out by week-to-week noise)"
                    ),
                }
            )
        if float(std) >= HIGH_VOL_STD_PCT:
            factors.append(
                {
                    "code": "high_volatility",
                    "label": "High absolute volatility",
                    "detail": f"std_dev_pct={std}% >= {HIGH_VOL_STD_PCT}%",
                }
            )

    primary = _primary_low_confidence_reason(
        confidence=confidence,
        sample=sample,
        years=years,
        z=z,
        std=std,
        stale=stale,
    )

    return {
        "confidence_rating": confidence,
        "primary_low_confidence_reason": primary if confidence == CONF_LOW else None,
        "primary_reason_code": primary,
        "contributing_factors": factors,
        "factor_codes": [f["code"] for f in factors],
        "thresholds": {
            "medium_sample": MEDIUM_SAMPLE,
            "high_sample": HIGH_SAMPLE,
            "medium_z": MEDIUM_Z,
            "high_z": HIGH_Z,
            "medium_years": MEDIUM_YEARS,
            "high_years": HIGH_YEARS,
            "bull_win_rate_pct": BULL_WIN_RATE,
            "bear_win_rate_pct": BEAR_WIN_RATE,
            "bull_avg_return_pct": BULL_AVG_RETURN,
            "bear_avg_return_pct": BEAR_AVG_RETURN,
        },
        "gaps": {
            "z_gap_to_medium": round(max(0.0, MEDIUM_Z - (abs_z or 0.0)), 4),
            "z_gap_to_high": round(max(0.0, HIGH_Z - (abs_z or 0.0)), 4),
            "sample_gap_to_medium": max(0, MEDIUM_SAMPLE - sample),
            "sample_gap_to_high": max(0, HIGH_SAMPLE - sample),
            "years_gap_to_medium": round(max(0.0, MEDIUM_YEARS - years), 2),
            "years_gap_to_high": round(max(0.0, HIGH_YEARS - years), 2),
        },
        "meets_medium_data_gates": sample >= MEDIUM_SAMPLE and years >= MEDIUM_YEARS and not stale,
        "meets_high_data_gates": sample >= HIGH_SAMPLE and years >= HIGH_YEARS and not stale,
    }


def _primary_low_confidence_reason(
    *,
    confidence: str,
    sample: int,
    years: float,
    z: float | None,
    std: float | None,
    stale: bool,
) -> str:
    if confidence != CONF_LOW:
        return "confidence_threshold_met"

    if sample < MEDIUM_SAMPLE:
        return "insufficient_sample_size"
    if stale:
        return "stale_prices"
    if years < MEDIUM_YEARS:
        return "insufficient_years"
    if std is not None and std == 0:
        return "zero_volatility"
    if z is None:
        return "no_z_score"
    if abs(float(z)) < MEDIUM_Z:
        return "insufficient_statistical_significance"
    return "below_high_threshold"


def build_detail_row(row: dict[str, Any]) -> dict[str, Any]:
    """One FX pair × current ISO week detail block."""
    diag = diagnose_confidence(row)
    return {
        "pair": row.get("pair") or row.get("asset"),
        "data_source": row.get("data_source"),
        "current_iso_week": row.get("current_iso_week"),
        "current_iso_year": row.get("current_iso_year"),
        "earliest_date": row.get("earliest_date"),
        "latest_date": row.get("latest_date"),
        "daily_bars": row.get("daily_bars"),
        "years_covered": row.get("years_covered"),
        "ten_year_sample_size": row.get("sample_size"),
        "win_rate_pct": row.get("win_rate_pct"),
        "average_return_pct": row.get("avg_return_pct"),
        "median_return_pct": row.get("median_return_pct"),
        "standard_deviation_pct": row.get("std_dev_pct"),
        "z_score": row.get("z_score"),
        "bias": row.get("bias"),
        "confidence_rating": row.get("confidence"),
        "pass_fail_status": row.get("pass_fail_status"),
        "positive_years": row.get("positive_years"),
        "negative_years": row.get("negative_years"),
        "best_year": row.get("best_year"),
        "best_return_pct": row.get("best_return_pct"),
        "worst_year": row.get("worst_year"),
        "worst_return_pct": row.get("worst_return_pct"),
        "diagnostics": diag,
        "warnings": row.get("warnings") or [],
    }


def build_detail_report(audit_report: dict[str, Any]) -> dict[str, Any]:
    """Build detail export from a seasonality_v2_audit report payload."""
    rows = audit_report.get("pairs") or audit_report.get("assets") or []
    fx_rows = [r for r in rows if (r.get("category") or "FX") == "FX" or r.get("pair")]
    if not fx_rows and rows:
        fx_rows = [r for r in rows if r.get("asset") in {
            "EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "USDJPY", "USDCAD", "USDCHF", "EURJPY",
        }]

    detail_pairs = [build_detail_row(r) for r in fx_rows]
    primary_reasons = Counter(
        d["diagnostics"]["primary_reason_code"]
        for d in detail_pairs
        if d.get("confidence_rating") == CONF_LOW
    )
    factor_counts = Counter(
        code
        for d in detail_pairs
        for code in d["diagnostics"].get("factor_codes") or []
    )

    low_pairs = [d["pair"] for d in detail_pairs if d.get("confidence_rating") == CONF_LOW]
    med_pairs = [d["pair"] for d in detail_pairs if d.get("confidence_rating") == CONF_MEDIUM]
    high_pairs = [d["pair"] for d in detail_pairs if d.get("confidence_rating") == CONF_HIGH]

    iso_week = detail_pairs[0].get("current_iso_week") if detail_pairs else None
    iso_year = detail_pairs[0].get("current_iso_year") if detail_pairs else None

    all_failed_low_sig = (
        len(low_pairs) == len(detail_pairs)
        and len(detail_pairs) > 0
        and primary_reasons.get("insufficient_statistical_significance", 0) == len(low_pairs)
    )

    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parser": "hptl.seasonality.seasonality_v2_detail",
        "audit_only": True,
        "live_wired": False,
        "data_source_mode": audit_report.get("data_source_mode", "production"),
        "staging_dir": audit_report.get("staging_dir"),
        "current_iso_week": iso_week,
        "current_iso_year": iso_year,
        "summary": {
            "fx_pairs": len(detail_pairs),
            "high_confidence": high_pairs,
            "medium_confidence": med_pairs,
            "low_confidence": low_pairs,
            "primary_low_confidence_reasons": dict(primary_reasons),
            "contributing_factor_counts": dict(factor_counts),
            "all_pairs_failed_due_to_low_z": all_failed_low_sig,
            "interpretation": _interpret_summary(
                detail_pairs, primary_reasons, factor_counts, all_failed_low_sig
            ),
        },
        "pairs": detail_pairs,
    }


def _interpret_summary(
    detail_pairs: list[dict[str, Any]],
    primary_reasons: Counter,
    factor_counts: Counter,
    all_failed_low_sig: bool,
) -> str:
    if not detail_pairs:
        return "No FX pairs in audit — detail export empty."

    if all_failed_low_sig:
        return (
            "All FX pairs have adequate 10-year sample size (n=10) and coverage, but confidence "
            "is Low because |z-score| < 0.4 for the current ISO week — the average weekly return "
            "is too small relative to historical volatility (insufficient statistical significance). "
            "Weak win rates and weak average returns also explain Neutral bias, but data depth is not the blocker."
        )

    top_reason = primary_reasons.most_common(1)[0][0] if primary_reasons else "unknown"
    top_factors = ", ".join(c for c, _ in factor_counts.most_common(3))

    reason_labels = {
        "insufficient_sample_size": "insufficient 10-year ISO-week sample size (data depth)",
        "insufficient_years": "insufficient years of daily price history",
        "insufficient_statistical_significance": "low |z-score| (weak signal vs volatility)",
        "stale_prices": "stale latest prices",
        "no_z_score": "unable to compute z-score",
        "zero_volatility": "zero week-to-week volatility",
    }
    label = reason_labels.get(top_reason, top_reason)
    return (
        f"Primary blocker across FX pairs: {label}. "
        f"Most common contributing factors: {top_factors or 'none'}."
    )


def render_detail_markdown(report: dict[str, Any]) -> str:
    s = report.get("summary") or {}
    lines = [
        "# Seasonality V2 Detail — FX Current ISO Week",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        "**Audit-only** — does not modify live seasonality scoring.",
        "",
        f"Data source: `{report.get('data_source_mode')}`",
        f"Current ISO week: **{report.get('current_iso_week')}** (year {report.get('current_iso_year')})",
        "",
        "## Why confidence is Low",
        "",
        s.get("interpretation", ""),
        "",
        "### Primary low-confidence reasons (pair count)",
        "",
    ]
    for reason, count in sorted((s.get("primary_low_confidence_reasons") or {}).items()):
        lines.append(f"- `{reason}`: **{count}** pairs")

    lines.extend(["", "### Contributing factors (pair count)", ""])
    for code, count in sorted(
        (s.get("contributing_factor_counts") or {}).items(),
        key=lambda x: -x[1],
    ):
        lines.append(f"- `{code}`: **{count}** pairs")

    lines.extend(
        [
            "",
            "## Per-pair detail",
            "",
            "| Pair | ISO Wk | Sample | Win% | Avg% | Med% | Std% | Z | Confidence | Primary reason |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---|---|",
        ]
    )

    for row in report.get("pairs") or []:
        diag = row.get("diagnostics") or {}
        lines.append(
            "| {pair} | {wk} | {n} | {win} | {avg} | {med} | {std} | {z} | {conf} | {reason} |".format(
                pair=row.get("pair"),
                wk=row.get("current_iso_week"),
                n=row.get("ten_year_sample_size"),
                win=row.get("win_rate_pct") if row.get("win_rate_pct") is not None else "—",
                avg=row.get("average_return_pct") if row.get("average_return_pct") is not None else "—",
                med=row.get("median_return_pct") if row.get("median_return_pct") is not None else "—",
                std=row.get("standard_deviation_pct") if row.get("standard_deviation_pct") is not None else "—",
                z=row.get("z_score") if row.get("z_score") is not None else "—",
                conf=row.get("confidence_rating"),
                reason=diag.get("primary_reason_code") or "—",
            )
        )

    lines.append("")
    for row in report.get("pairs") or []:
        diag = row.get("diagnostics") or {}
        factors = diag.get("contributing_factors") or []
        if not factors:
            continue
        lines.append(f"### {row.get('pair')} — contributing factors")
        for f in factors:
            lines.append(f"- **{f.get('label')}**: {f.get('detail')}")
        gaps = diag.get("gaps") or {}
        if gaps.get("z_gap_to_medium") is not None:
            lines.append(
                f"- Z gap to Medium: **{gaps.get('z_gap_to_medium')}** "
                f"(need |z| ≥ {MEDIUM_Z}); gap to High: **{gaps.get('z_gap_to_high')}**"
            )
        lines.append("")

    return "\n".join(lines)


def write_detail_exports(
    audit_report: dict[str, Any],
    *,
    detail_json: Path | None = None,
    detail_md: Path | None = None,
) -> dict[str, Path]:
    detail = build_detail_report(audit_report)
    out_json = detail_json or DETAIL_JSON
    out_md = detail_md or DETAIL_MD
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(detail, indent=2, ensure_ascii=False), encoding="utf-8")
    out_md.write_text(render_detail_markdown(detail), encoding="utf-8")
    return {"json": out_json, "md": out_md}
