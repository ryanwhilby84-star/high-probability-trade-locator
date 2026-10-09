"""Plan the next long-history backfill step without modifying production data."""

from __future__ import annotations

from scripts.audit_seasonality_price_coverage import MARKETS_23, TARGET_YEARS, _coverage
from hptl.seasonality_workstation.long_history_registry import LONG_HISTORY_PLAN, TARGET_START_YEAR


def main() -> None:
    print("=" * 132)
    print("INSTITUTIONAL EDGE — 23-MARKET LONG-HISTORY BACKFILL PLAN")
    print(f"Research target: {TARGET_YEARS} complete years (roughly {TARGET_START_YEAR} onward where the instrument actually existed)")
    print("RULE: never manufacture pre-inception history; proxies/bridges must remain labelled.")
    print("=" * 132)

    rows = []
    for market in MARKETS_23:
        cov = _coverage(market)
        plan = LONG_HISTORY_PLAN[market]
        rows.append((cov, plan))

    order = {"futures_actual": 0, "index_bridge": 1, "spot_bridge": 2, "inception_limited": 3}
    rows.sort(key=lambda rp: (order.get(str(rp[1]["strategy"]), 9), -int(rp[0]["shortfall"]), str(rp[0]["market"])))

    for cov, plan in rows:
        market = str(cov["market"])
        strategy = str(plan["strategy"])
        start = plan.get("actual_start", "legacy")
        candidates = "; ".join(plan.get("candidates", []))
        print(f"{market:26} gap={int(cov['shortfall']):2d}y | {strategy:17} | actual/inception={start} | {candidates}")

    print("\nACQUISITION ORDER")
    print("1. Backfill markets where >=48 years of actual futures/exchange history is plausible.")
    print("2. Build separately-labelled cash-index/spot bridges for shorter-lived financial futures.")
    print("3. Keep inception-limited markets explicit; test them on maximum legitimate history rather than fake 48 years.")
    print("4. Re-run audit_seasonality_price_coverage.py after every backfill batch.")
    print("5. Only after coverage is frozen: rerun the Bernd rediscovery/OOS experiments unchanged.")


if __name__ == "__main__":
    main()
