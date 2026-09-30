"""Seasonality foundation rebuild — dense daily backfill for priority markets.

Repairs price-store history for seasonality trust grading without changing
projection math, UI, or confluence wiring.

Usage:
    python -m hptl.seasonality.seasonality_foundation_rebuild --dry-run
    python -m hptl.seasonality.seasonality_foundation_rebuild --execute
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from hptl.config import DATA_DIR
from hptl.prices.canonical_timeline import resample_weekly_closes
from hptl.prices.cot_fail_backfill import backfill_fred_instrument
from hptl.prices.fx_daily_backfill import BackfillPair, run_backfill
from hptl.prices.models import build_history_meta, compute_range_52w
from hptl.prices.price_store import load_instrument_record, write_instrument_record, write_price_store
from hptl.prices.promote_price_backfill import promote_staging_backfill
from hptl.prices.softs_foundation import FRED_PRIMARY_SOFTS, FRED_OBS_START, merge_av_with_fred_primary
from hptl.seasonality.seasonality_price_bars import history_quality, weekly_closes_for_instrument
from hptl.seasonality.seasonality_price_export import block_for_market
from hptl.seasonality.seasonality_trust import attach_trust_metadata
from hptl.seasonality.seasonality_engine import compute_seasonality_price_block
from hptl.seasonality.seasonality_price_bars import weekly_closes_for_instrument as _wc
from hptl.seasonality.seasonality_v2 import normalize_daily_bars, years_spanned

logger = logging.getLogger(__name__)

AUDIT_JSON = DATA_DIR / "audits" / "seasonality_foundation_rebuild.json"
AUDIT_MD = DATA_DIR / "audits" / "seasonality_foundation_rebuild.md"

# OANDA dense daily backfill targets (display, oanda_symbol, store_key)
OANDA_FOUNDATION_PAIRS: tuple[BackfillPair, ...] = (
    ("Silver", "XAG_USD", "Silver"),
    ("Copper", "XCU_USD", "Copper / HG"),
    ("Corn", "CORN_USD", "Corn"),
)

# Drop Alpha Vantage monthly prefix before this date when dense OANDA exists.
DENSE_DAILY_CUTOFF = "2016-06-01"

PRIORITY_MARKETS = ("Silver", "Copper / HG", "Corn", "Cotton", "Coffee")


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")


def _trust_snapshot(market: str) -> dict[str, Any]:
    bars, method, tl = weekly_closes_for_instrument(market)
    if not bars:
        return {"market": market, "available": False, "trust_grade": "C", "reason": "no_weekly_bars"}
    block = compute_seasonality_price_block(
        market,
        bars,
        price_store_key=tl.resolved_store_key if tl else market,
        bar_source="weekly",
        canonical_source=tl.canonical_source if tl else None,
        canonical_symbol=tl.canonical_symbol if tl else None,
        price_derivation=method,
        proxy=tl.proxy if tl else None,
        proxy_explanation=tl.proxy_explanation if tl else None,
    )
    attach_trust_metadata(block, bars)
    hist_yrs, avg_wpy, min_last3 = history_quality(bars)
    f8 = (block.get("forward_read") or {}).get("next_8w") or {}
    conf = block.get("confidence") or {}
    return {
        "market": market,
        "available": block.get("available"),
        "earliest_date": bars[0][0],
        "latest_date": bars[-1][0],
        "years_of_history": block.get("years_of_history"),
        "weekly_observations": len(bars),
        "avg_weeks_per_year": round(avg_wpy, 1),
        "min_weeks_last_3y": min_last3,
        "seasonal_3y_weeks": block.get("seasonal_3y_weeks"),
        "trust_grade": block.get("trust_grade"),
        "trust_score": block.get("trust_score"),
        "trust_notes": block.get("trust_notes"),
        "forward_8w_direction": f8.get("direction"),
        "forward_8w_sample_years": f8.get("sample_years"),
        "confidence_level": conf.get("level"),
        "canonical_source": tl.canonical_source if tl else None,
        "daily_bars": tl.bar_count if tl else 0,
    }


def drop_sparse_monthly_prefix(instrument_id: str, *, cutoff: str = DENSE_DAILY_CUTOFF) -> dict[str, Any]:
    """Remove sparse monthly AV prefix; keep dense OANDA daily from *cutoff* onward."""
    rec = load_instrument_record(instrument_id)
    if not rec:
        return {"instrument": instrument_id, "status": "missing", "bars_removed": 0}
    daily = normalize_daily_bars(rec.get("daily") or [])
    before = len(daily)
    trimmed = [b for b in daily if str(b["date"])[:10] >= cutoff]
    if len(trimmed) == before:
        return {"instrument": instrument_id, "status": "unchanged", "bars_removed": 0, "total_daily_bars": before}
    daily = normalize_daily_bars(trimmed)
    range_52w = compute_range_52w(daily)
    rec = {
        **rec,
        "instrument_id": instrument_id,
        "daily": daily,
        "weekly": [],
        "range_52w": range_52w,
        "history": build_history_meta(daily, [], range_52w),
        "price_scale": {
            **(rec.get("price_scale") or {}),
            "source": "oanda",
            "sparse_prefix_dropped": True,
            "sparse_prefix_cutoff": cutoff,
            "note": f"Sparse monthly prefix removed before {cutoff}; dense OANDA daily canonical.",
        },
    }
    write_instrument_record(rec, fetched_via="seasonality_foundation", historical_via="oanda_backfill")
    return {
        "instrument": instrument_id,
        "status": "trimmed",
        "bars_removed": before - len(daily),
        "total_daily_bars": len(daily),
        "earliest_date": daily[0]["date"] if daily else None,
        "latest_date": daily[-1]["date"] if daily else None,
        "years_spanned": round(years_spanned(daily), 2) if daily else 0.0,
    }


def rebuild_fred_softs(*, observation_start: str = FRED_OBS_START) -> list[dict[str, Any]]:
    """Ensure Cotton/Coffee use FRED-primary IMF monthly from *observation_start*."""
    results: list[dict[str, Any]] = []
    for instrument_id, (series_id, note) in FRED_PRIMARY_SOFTS.items():
        if instrument_id not in {"Cotton", "Coffee"}:
            continue
        row = merge_av_with_fred_primary(
            instrument_id,
            fred_series_id=series_id,
            note=note,
            observation_start=observation_start,
        )
        results.append(row)
    return results


def run_foundation_rebuild(*, execute: bool = False, years: int = 10) -> dict[str, Any]:
    """Audit and optionally rebuild priority seasonality price foundations."""
    before = {m: _trust_snapshot(m) for m in PRIORITY_MARKETS}

    actions: dict[str, Any] = {"oanda_backfill": None, "promotions": [], "trims": [], "fred_softs": []}

    if execute:
        actions["oanda_backfill"] = run_backfill(pairs=OANDA_FOUNDATION_PAIRS, years=years)
        keys = [p[2] for p in OANDA_FOUNDATION_PAIRS]
        actions["promotions"] = promote_staging_backfill(keys).get("promoted") or []
        for iid in ("Copper / HG", "Corn"):
            actions["trims"].append(drop_sparse_monthly_prefix(iid))
        actions["fred_softs"] = rebuild_fred_softs()
        from hptl.prices.price_store import load_all_instrument_records

        records = load_all_instrument_records()
        if records:
            write_price_store(records)

    after = {m: _trust_snapshot(m) for m in PRIORITY_MARKETS}

    comparison: list[dict[str, Any]] = []
    for m in PRIORITY_MARKETS:
        b, a = before[m], after[m]
        comparison.append(
            {
                "market": m,
                "trust_before": b.get("trust_grade"),
                "trust_after": a.get("trust_grade"),
                "years_before": b.get("years_of_history"),
                "years_after": a.get("years_of_history"),
                "weekly_obs_before": b.get("weekly_observations"),
                "weekly_obs_after": a.get("weekly_observations"),
                "avg_wpy_before": b.get("avg_weeks_per_year"),
                "avg_wpy_after": a.get("avg_weeks_per_year"),
                "sample_8w_before": b.get("forward_8w_sample_years"),
                "sample_8w_after": a.get("forward_8w_sample_years"),
                "confidence_before": b.get("confidence_level"),
                "confidence_after": a.get("confidence_level"),
                "before": b,
                "after": a,
            }
        )

    payload = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "execute": execute,
        "priority_markets": list(PRIORITY_MARKETS),
        "actions": actions,
        "comparison": comparison,
        "notes": (
            "Silver/Copper/Corn rebuilt via OANDA dense daily. "
            "Cotton/Coffee remain FRED IMF monthly — no OANDA/FMP free-tier daily source."
        ),
    }
    return payload


def write_audit_exports(payload: dict[str, Any]) -> None:
    AUDIT_JSON.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    lines = [
        "# Seasonality Foundation Rebuild Audit",
        "",
        f"- Generated: {payload['generated_at']}",
        f"- Execute mode: {payload['execute']}",
        "",
        "## Before vs After",
        "",
        "| Market | Trust Before | Trust After | Years | Weekly Obs | Avg Wk/Yr | 8W n | Confidence |",
        "|---|:---:|:---:|---:|---:|---:|---:|---|",
    ]
    for row in payload.get("comparison") or []:
        lines.append(
            f"| {row['market']} | {row['trust_before']} | {row['trust_after']} | "
            f"{row['years_after']} | {row['weekly_obs_after']} | {row['avg_wpy_after']} | "
            f"{row['sample_8w_after']} | {row['confidence_after']} |"
        )
    lines.extend(["", "## Notes", "", payload.get("notes", "")])
    AUDIT_MD.write_text("\n".join(lines), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seasonality foundation price rebuild")
    parser.add_argument("--execute", action="store_true", help="Run backfill + promote + trim")
    parser.add_argument("--years", type=int, default=10)
    args = parser.parse_args(argv)
    _configure_logging()
    payload = run_foundation_rebuild(execute=args.execute, years=args.years)
    write_audit_exports(payload)
    print(f"Wrote {AUDIT_JSON}")
    for row in payload["comparison"]:
        print(
            f"  {row['market']:12} {row['trust_before']} -> {row['trust_after']} "
            f"avg_wpy={row['avg_wpy_after']} n8w={row['sample_8w_after']}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
