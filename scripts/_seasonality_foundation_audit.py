"""Temporary read-only audit helper for seasonality foundation rebuild."""
from __future__ import annotations

from hptl.prices.canonical_timeline import build_canonical_timeline
from hptl.prices.price_store import load_price_store
from hptl.seasonality.seasonality_price_bars import history_quality, weekly_closes_for_instrument
from hptl.seasonality.seasonality_price_export import block_for_market

MARKETS = ["Silver", "Copper / HG", "Corn", "Cotton", "Coffee"]


def main() -> None:
    store = load_price_store()
    inst = store.get("instruments") or {}

    print("=== PRICE STORE ===")
    for m in MARKETS:
        rec = inst.get(m) or (inst.get("Copper") if m == "Copper / HG" else {}) or {}
        daily = rec.get("daily") or []
        weekly = rec.get("weekly") or []
        scale = rec.get("price_scale") or {}
        print(
            f"{m}: daily={len(daily)} weekly={len(weekly)} "
            f"source={scale.get('source')} series={scale.get('series_id')} err={rec.get('error')}"
        )
        if daily:
            print(f"  daily {daily[0]['date']} .. {daily[-1]['date']}")
        if weekly:
            print(f"  weekly {weekly[0]['date']} .. {weekly[-1]['date']}")

    print("\n=== CANONICAL + WEEKLY ISO ===")
    for m in MARKETS:
        tl = build_canonical_timeline(m, apply_supplements=False)
        bars, method, _ = weekly_closes_for_instrument(m)
        yrs, avg, min3 = history_quality(bars)
        print(
            f"{m}: daily_bars={tl.bar_count if tl else 0} src={tl.canonical_source if tl else None} "
            f"weekly={len(bars)} hist_yrs={yrs} avg_wk/yr={avg:.1f} min_last3={min3}END
        )
        if bars:
            print(f"  weekly {bars[0][0]} .. {bars[-1][0]}")

    print("\n=== SEASONALITY BLOCK (BEFORE) ===")
    for m in MARKETS:
        block = block_for_market(m, inst)
        f8 = (block.get("forward_read") or {}).get("next_8w") or {}
        conf = block.get("confidence") or {}
        print(
            f"{m}: trust={block.get('trust_grade')} yrs={block.get('years_of_history')} "
            f"avg_wpy={block.get('avg_weeks_per_year')} s3w={block.get('seasonal_3y_weeks')} "
            f"8w={f8.get('direction')} n={f8.get('sample_years')} conf={conf.get('level')}"
        )


if __name__ == "__main__":
    main()
