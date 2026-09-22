"""Audit historical daily-price coverage for the 23 core Institutional Edge COT markets.

Purpose
-------
Before rebuilding the Bernd-style seasonality research on a ~48-year history, establish
exactly how much canonical daily price history Institutional Edge currently has for each
of the 23 core markets.

This is diagnostics only. It does not change production workstation data or scoring.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime

from hptl.seasonality_workstation.indexed_seasonality import load_daily_closes_for_seasonality

# Canonical 23-market core used by the existing direct-COT coverage audit.
# Keep this explicit: the wider registry also contains Cotton, Bitcoin and DXY, but this
# audit answers the current 23-market seasonality-data question exactly.
MARKETS_23 = (
    "NASDAQ / NQ",
    "S&P 500 / ES",
    "Dow / YM",
    "Euro FX / 6E",
    "British Pound / 6B",
    "Japanese Yen / 6J",
    "Swiss Franc / 6S",
    "Australian Dollar / 6A",
    "Canadian Dollar / 6C",
    "NZ Dollar / 6N",
    "Gold",
    "Silver",
    "Copper / HG",
    "Crude Oil / CL",
    "Natural Gas / NG",
    "Coffee",
    "Cocoa",
    "Corn",
    "Wheat",
    "Soybeans",
    "Sugar",
    "Platinum",
    "Palladium",
)

TARGET_YEARS = 48
MIN_BARS_COMPLETE_YEAR = 180


def _year_counts(rows: list[tuple[str, float]]) -> Counter[int]:
    out: Counter[int] = Counter()
    for d, _ in rows:
        try:
            out[datetime.strptime(str(d)[:10], "%Y-%m-%d").year] += 1
        except ValueError:
            continue
    return out


def _coverage(market: str) -> dict[str, object]:
    rows, meta = load_daily_closes_for_seasonality(market)
    rows = sorted(rows, key=lambda x: x[0])
    counts = _year_counts(rows)
    complete = sorted(y for y, n in counts.items() if n >= MIN_BARS_COMPLETE_YEAR)

    first = rows[0][0][:10] if rows else None
    last = rows[-1][0][:10] if rows else None
    span_years = 0.0
    if first and last:
        a = datetime.strptime(first, "%Y-%m-%d")
        b = datetime.strptime(last, "%Y-%m-%d")
        span_years = (b - a).days / 365.2425

    complete_years = len(complete)
    return {
        "market": market,
        "bars": len(rows),
        "first": first,
        "last": last,
        "span_years": span_years,
        "complete_years": complete_years,
        "complete_first": complete[0] if complete else None,
        "complete_last": complete[-1] if complete else None,
        "shortfall": max(0, TARGET_YEARS - complete_years),
        "target_met": complete_years >= TARGET_YEARS,
        "source": meta.get("source"),
        "price_id": meta.get("price_instrument_id"),
        "error": meta.get("error"),
    }


def main() -> None:
    print("=" * 118)
    print("INSTITUTIONAL EDGE — 23-MARKET SEASONALITY PRICE-COVERAGE AUDIT")
    print(f"Target: >= {TARGET_YEARS} complete historical years per market; complete year >= {MIN_BARS_COMPLETE_YEAR} daily bars")
    print("Diagnostics only — no production code or price data is changed.")
    print("=" * 118)

    results = [_coverage(m) for m in MARKETS_23]

    header = f"{'MARKET':26} {'FIRST':10} {'LAST':10} {'SPAN':>6} {'COMP':>5} {'GAP':>4} {'48Y':>4}  SOURCE"
    print(header)
    print("-" * 118)
    for r in results:
        first = str(r['first'] or '-')
        last = str(r['last'] or '-')
        status = "YES" if r["target_met"] else "NO"
        source = str(r["source"] or "-")
        err = f" ERROR={r['error']}" if r["error"] else ""
        print(
            f"{str(r['market']):26} {first:10} {last:10} "
            f"{float(r['span_years']):6.1f} {int(r['complete_years']):5d} "
            f"{int(r['shortfall']):4d} {status:>4}  {source}{err}"
        )

    ready = [r for r in results if r["target_met"]]
    short = [r for r in results if not r["target_met"]]
    missing = [r for r in results if int(r["bars"]) == 0]

    print("-" * 118)
    print(f"READY >=48 complete years : {len(ready)}/23")
    print(f"SHORT OF 48 YEARS         : {len(short)}/23")
    print(f"NO DAILY DATA             : {len(missing)}/23")

    if short:
        print("\nDATA ACQUISITION QUEUE (largest shortfall first)")
        print("-" * 118)
        for r in sorted(short, key=lambda x: (-int(x["shortfall"]), str(x["market"]))):
            print(
                f"{str(r['market']):26} need +{int(r['shortfall']):2d} complete years "
                f"| current complete range {r['complete_first'] or '-'}..{r['complete_last'] or '-'} "
                f"| price_id={r['price_id'] or '-'}"
            )

    print("\nNEXT: source/backfill the acquisition queue, then rerun this exact audit before repeating methodology tests.")


if __name__ == "__main__":
    main()
