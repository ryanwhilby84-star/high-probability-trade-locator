#!/usr/bin/env python3
"""Validate indexed DOY median/10Y/smooth=5 across representative instruments.

Construction is universal. Lookback is reported for stability — not tuned to a
preferred direction. Neighbouring lookbacks may flip DOWN/FLAT/UP.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.markets.usd_index_identity import ICE_DXY_ID  # noqa: E402
from hptl.seasonality_workstation.indexed_seasonality import (  # noqa: E402
    DEFAULT_SMOOTH,
    build_normalised_seasonal_curve,
    load_daily_closes_for_seasonality,
    walk_forward_hit_rate,
)

# Representative universe from the shipping caution
INSTRUMENTS = [
    ("DXY", ICE_DXY_ID),
    ("Gold", "Gold"),
    ("Soybeans", "Soybeans"),
    ("Natural Gas", "Natural Gas / NG"),
    ("Copper", "Copper / HG"),
    ("S&P 500", "S&P 500 / ES"),
    ("GBP", "British Pound / 6B"),
    ("CHF", "Swiss Franc / 6S"),
]

LOOKBACKS = (5, 8, 10, 12, 15)


def _validate_one(label: str, instrument_id: str) -> dict:
    daily, meta = load_daily_closes_for_seasonality(instrument_id)
    if not daily:
        return {
            "label": label,
            "instrument_id": instrument_id,
            "status": "FAIL",
            "error": meta.get("error") or "no_daily",
            "price_identity": meta,
        }

    # Refuse FRED broad on DXY path
    if label == "DXY":
        from hptl.prices.price_store import load_instrument_record_internal

        rec = load_instrument_record_internal(meta.get("price_instrument_id") or instrument_id) or {}
        scale = rec.get("price_scale") or {}
        if scale.get("series_id") == "DTWEXBGS" or scale.get("is_fred_broad"):
            return {
                "label": label,
                "instrument_id": instrument_id,
                "status": "FAIL",
                "error": "fred_broad_bound_to_dxy",
                "price_identity": meta,
                "price_scale": scale,
            }

    asof = daily[-1][0]
    by_lb = {}
    for lb in LOOKBACKS:
        curve = build_normalised_seasonal_curve(
            daily, asof=asof, lookback_years=lb, smooth=DEFAULT_SMOOTH
        )
        if not curve.get("available"):
            by_lb[str(lb)] = {"available": False, "reason": curve.get("reason")}
            continue
        hz = curve.get("horizons") or {}
        by_lb[str(lb)] = {
            "sample_size": curve.get("sample_size"),
            "sample_years": curve.get("sample_years"),
            "4w": hz.get("4w"),
            "8w": hz.get("8w"),
            "12w": hz.get("12w"),
            "positive_frequency_8w": curve.get("positive_frequency_8w"),
            "negative_frequency_8w": curve.get("negative_frequency_8w"),
        }

    dirs_8w = [v["8w"]["direction"] for v in by_lb.values() if v.get("8w")]
    unique_dirs = sorted(set(dirs_8w))
    primary = build_normalised_seasonal_curve(
        daily, asof=asof, lookback_years=10, smooth=DEFAULT_SMOOTH
    )
    wf = walk_forward_hit_rate(daily, lookback_years=10, smooth=DEFAULT_SMOOTH)

    last_close = daily[-1][1]
    return {
        "label": label,
        "instrument_id": instrument_id,
        "price_instrument_id": meta.get("price_instrument_id"),
        "status": "ok" if primary.get("available") else "FAIL",
        "asof": asof,
        "latest_close": last_close,
        "price_source": meta.get("source"),
        "method": primary.get("method"),
        "default_10y": {
            "sample_size": primary.get("sample_size"),
            "sample_years": primary.get("sample_years"),
            "horizons": primary.get("horizons"),
            "positive_frequency_8w": primary.get("positive_frequency_8w"),
            "negative_frequency_8w": primary.get("negative_frequency_8w"),
        },
        "walk_forward_8w": wf,
        "lookback_grid": by_lb,
        "lookback_direction_stability_8w": {
            "directions_seen": unique_dirs,
            "stable": len(unique_dirs) <= 1,
            "caution": (
                None
                if len(unique_dirs) <= 1
                else "Neighbouring lookbacks change direction — do not pick lookback for preferred bias"
            ),
        },
    }


def main() -> int:
    rows = [_validate_one(label, iid) for label, iid in INSTRUMENTS]
    unstable = [
        r["label"]
        for r in rows
        if r.get("status") == "ok"
        and not (r.get("lookback_direction_stability_8w") or {}).get("stable")
    ]
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": {
            "name": "indexed_year_path",
            "aggregation": "median",
            "alignment": "calendar_doy",
            "default_lookback_years": 10,
            "smooth": 5,
            "excludes_incomplete_current_year": True,
            "universal_construction": True,
            "lookback_policy": (
                "Default 10Y for product; market-specific lookback only with a "
                "defensible rule — never chosen to match preferred direction."
            ),
        },
        "instruments": rows,
        "summary": {
            "n": len(rows),
            "ok": sum(1 for r in rows if r.get("status") == "ok"),
            "fail": sum(1 for r in rows if r.get("status") != "ok"),
            "lookback_unstable_8w": unstable,
        },
    }

    out_dir = ROOT / "data" / "audits" / "indexed_seasonality_validation"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_json = out_dir / "indexed_seasonality_validation.json"
    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

    lines = [
        "# Indexed seasonality validation (representative markets)",
        "",
        f"Generated: {report['generated_at']}",
        "",
        "Universal construction: indexed year path -> calendar DOY -> median -> "
        "complete years only -> exclude incomplete current year -> smooth=5.",
        "Default product lookback: **10Y**. Neighbouring lookbacks are reported for stability.",
        "",
        "| Market | Status | n years | 4W | 8W | 12W | +freq 8W | WF hit | Lookback stable? |",
        "|---|---|---:|---|---|---|---:|---:|---|",
    ]
    for r in rows:
        if r.get("status") != "ok":
            lines.append(
                f"| {r['label']} | FAIL | — | — | — | — | — | — | {r.get('error')} |"
            )
            continue
        d = r["default_10y"]
        h = d.get("horizons") or {}
        stab = r.get("lookback_direction_stability_8w") or {}
        wf = r.get("walk_forward_8w") or {}
        lines.append(
            "| {lab} | ok | {n} | {d4} {m4}% | {d8} {m8}% | {d12} {m12}% | {pos} | {wf} | {stab} |".format(
                lab=r["label"],
                n=d.get("sample_size"),
                d4=(h.get("4w") or {}).get("direction"),
                m4=(h.get("4w") or {}).get("median_move_pct"),
                d8=(h.get("8w") or {}).get("direction"),
                m8=(h.get("8w") or {}).get("median_move_pct"),
                d12=(h.get("12w") or {}).get("direction"),
                m12=(h.get("12w") or {}).get("median_move_pct"),
                pos=d.get("positive_frequency_8w"),
                wf=wf.get("hit_rate"),
                stab="yes" if stab.get("stable") else f"NO ({','.join(stab.get('directions_seen') or [])})",
            )
        )
    lines.extend(
        [
            "",
            "## Caution",
            "",
            "If lookback stability is NO, do **not** pick 10Y (or any lookback) because it "
            "matches a preferred direction. Construction stays universal; lookback may later "
            "need a defensible market-specific rule.",
            "",
            f"Unstable markets at 8W across lookbacks: {', '.join(unstable) or 'none'}.",
            "",
        ]
    )
    out_md = out_dir / "indexed_seasonality_validation.md"
    out_md.write_text("\n".join(lines), encoding="utf-8")
    try:
        print(out_md.read_text(encoding="utf-8"))
    except UnicodeEncodeError:
        print(out_md.read_text(encoding="utf-8").encode("ascii", "replace").decode("ascii"))
    print(f"\nWrote {out_json}")
    return 0 if report["summary"]["fail"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
