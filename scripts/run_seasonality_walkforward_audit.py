#!/usr/bin/env python3
"""Run strict walk-forward verification for the production Seasonal Roadmap.

Examples:
    python scripts/run_seasonality_walkforward_audit.py Soybeans
    python scripts/run_seasonality_walkforward_audit.py Soybeans --lookback-years 15 --step-bars 60

Outputs:
    data/audits/seasonality_walkforward_<instrument>.json
    data/audits/seasonality_walkforward_<instrument>.md
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.seasonality_workstation.indexed_seasonality import (  # noqa: E402
    load_daily_closes_for_seasonality,
)
from hptl.seasonality_workstation.walkforward_verification import (  # noqa: E402
    verify_seasonality_walkforward,
)


def _slug(value: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return s or "instrument"


def _fmt(value: Any, *, pct: bool = False) -> str:
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:.3f}%" if pct else f"{value:.4f}"
    return str(value)


def _markdown(doc: dict[str, Any]) -> str:
    verdict = doc.get("verdict") or {}
    current = doc.get("current_snapshot") or {}
    fit = current.get("historical_shape_fit") or {}
    lines = [
        f"# Seasonality Walk-Forward Audit — {doc.get('instrument')}",
        "",
        f"- Generated: `{doc.get('generated_at')}`",
        f"- Production method: `{doc.get('production_method')}`",
        f"- Verification method: `{doc.get('verification_method')}`",
        f"- History: `{doc.get('first_date')} → {doc.get('last_date')}`",
        f"- Walk-forward anchors: **{doc.get('anchors_evaluated')}**",
        f"- No-lookahead gate: **{'PASS' if doc.get('no_lookahead_pass') else 'FAIL'}**",
        f"- Verdict: **{verdict.get('status')}**",
        "",
        "## What the current chart's grey history is doing",
        "",
        "The roadmap is intentionally rebased to the current anchor price. This audit therefore compares **shape**, not absolute price levels.",
        "",
        f"- Level-path correlation: `{_fmt(fit.get('level_path_correlation'))}`",
        f"- Daily-return correlation: `{_fmt(fit.get('daily_return_correlation'))}`",
        f"- Daily direction agreement: `{_fmt(None if fit.get('daily_direction_agreement') is None else fit.get('daily_direction_agreement') * 100, pct=True)}`",
        f"- Anchor-normalised path RMSE: `{_fmt(fit.get('path_rmse_pct'), pct=True)}`",
        "",
        "## True out-of-sample forecast test",
        "",
        "Each historical anchor is rebuilt using only data available on that date, then compared with what price actually did afterward.",
        "",
        "| Horizon | n | Direction hit | Mean forecast | Mean actual | MAE | Flat MAE | Skill vs flat |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for key in ("4w", "8w", "12w"):
        row = (doc.get("out_of_sample") or {}).get(key) or {}
        hit = row.get("direction_hit_rate")
        skill = row.get("skill_vs_flat_ratio")
        lines.append(
            "| {key} | {n} | {hit} | {pred} | {actual} | {mae} | {flat} | {skill} |".format(
                key=key,
                n=row.get("n") or 0,
                hit="—" if hit is None else f"{hit * 100:.1f}%",
                pred=_fmt(row.get("mean_predicted_return_pct"), pct=True),
                actual=_fmt(row.get("mean_actual_return_pct"), pct=True),
                mae=_fmt(row.get("mae_pct_points"), pct=True),
                flat=_fmt(row.get("flat_baseline_mae_pct_points"), pct=True),
                skill="—" if skill is None else f"{skill * 100:.1f}%",
            )
        )
    lines += ["", "## Verdict reasons", ""]
    for reason in verdict.get("reasons") or []:
        lines.append(f"- `{reason}`")
    lines += [
        "",
        "## Interpretation",
        "",
        "- A poor pre-anchor shape fit means the grey seasonal history did not resemble the realised year-to-date path. That is useful context, but it does **not** by itself invalidate the forward statistics.",
        "- The decision-grade test is the historical walk-forward section: direction, forecast error, and whether it beats a flat zero-return forecast out of sample.",
        "- `SUPPORTED` means the configured evidence gate passed. It is not a guarantee of future price direction.",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Walk-forward audit of Institutional Edge production seasonality")
    parser.add_argument("instrument", nargs="?", default="Soybeans")
    parser.add_argument("--lookback-years", type=int, default=15)
    parser.add_argument(
        "--step-bars",
        type=int,
        default=60,
        help="Bars between historical anchors. Default 60 avoids overlap at the 12-week horizon.",
    )
    parser.add_argument("--json-only", action="store_true")
    args = parser.parse_args()

    daily, meta = load_daily_closes_for_seasonality(args.instrument)
    if not daily:
        print(f"ERROR: no seasonality price history for {args.instrument}: {meta}")
        return 2

    doc = verify_seasonality_walkforward(
        daily,
        instrument_id=args.instrument,
        lookback_years=args.lookback_years,
        step_bars=args.step_bars,
    )
    doc["generated_at"] = datetime.now(timezone.utc).isoformat()
    doc["load_meta"] = meta
    doc["command"] = (
        f"python scripts/run_seasonality_walkforward_audit.py {args.instrument!r} "
        f"--lookback-years {args.lookback_years} --step-bars {args.step_bars}"
    )

    out_dir = ROOT / "data" / "audits"
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"seasonality_walkforward_{_slug(args.instrument)}"
    out_json = out_dir / f"{stem}.json"
    out_md = out_dir / f"{stem}.md"
    out_json.write_text(json.dumps(doc, indent=2, allow_nan=False), encoding="utf-8")
    if not args.json_only:
        out_md.write_text(_markdown(doc), encoding="utf-8")

    verdict = doc.get("verdict") or {}
    fit = ((doc.get("current_snapshot") or {}).get("historical_shape_fit") or {})
    print("Seasonality walk-forward audit")
    print(f"Instrument: {args.instrument}")
    print(f"Production method: {doc.get('production_method')}")
    print(f"Anchors: {doc.get('anchors_evaluated')}  no-lookahead={doc.get('no_lookahead_pass')}")
    print(
        "Current pre-anchor fit: "
        f"level_corr={fit.get('level_path_correlation')} "
        f"return_corr={fit.get('daily_return_correlation')} "
        f"direction={fit.get('daily_direction_agreement')} "
        f"rmse_pct={fit.get('path_rmse_pct')}"
    )
    for key in ("4w", "8w", "12w"):
        row = (doc.get("out_of_sample") or {}).get(key) or {}
        print(
            f"{key}: n={row.get('n', 0)} "
            f"hit={row.get('direction_hit_rate')} "
            f"MAE={row.get('mae_pct_points')}pp "
            f"flat_MAE={row.get('flat_baseline_mae_pct_points')}pp "
            f"skill={row.get('skill_vs_flat_ratio')}"
        )
    print(f"VERDICT: {verdict.get('status')} — {', '.join(verdict.get('reasons') or [])}")
    print(f"Wrote {out_json}")
    if not args.json_only:
        print(f"Wrote {out_md}")
    return 4 if verdict.get("status") == "FAIL_NO_LOOKAHEAD" else 0


if __name__ == "__main__":
    raise SystemExit(main())
