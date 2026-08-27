"""Probe canonical OANDA symbols against the actual connected account universe."""
from __future__ import annotations

import argparse
from difflib import get_close_matches

from hptl.markets.instrument_registry import get_instrument
from hptl.oanda.oanda_client import fetch_account_instruments, instrument_names_set
from hptl.prices.coverage import load_price_coverage, oanda_symbol_for

DEFAULT_FAILED_COHORT = [
    "Australia 200",
    "Bitcoin Cash",
    "China A50",
    "Ether/Ether",
    "Europe 50",
    "France 40",
    "Germany 30",
    "Hong Kong 33",
    "India 50",
    "Japan 225",
    "Litecoin",
    "Netherlands 25",
    "Singapore 30",
    "Taiwan Index",
    "UK 100",
    "UK 10Y Gilt",
    "US 10Y T-Note",
    "US 2Y T-Note",
    "US 5Y T-Note",
    "US Nas 100",
    "US Russ 2000",
    "US SPX 500",
    "US T-Bond",
    "US Wall St 30",
    "West Texas Oil",
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check HPTL OANDA symbols against the connected OANDA account")
    parser.add_argument("--instrument", action="append", default=[], help="Instrument id; repeat for a targeted batch")
    parser.add_argument(
        "--failed-cohort",
        action="store_true",
        help="Probe the known OANDA-related failures from the latest full-universe health run",
    )
    args = parser.parse_args(argv)

    ids = [str(i).strip() for i in args.instrument if str(i).strip()]
    if args.failed_cohort or not ids:
        ids.extend(DEFAULT_FAILED_COHORT)
    ids = list(dict.fromkeys(ids))

    coverage = load_price_coverage()
    account_rows = fetch_account_instruments()
    names = instrument_names_set(account_rows)

    print(f"OANDA account instrument universe: {len(names)} symbols")
    print(f"Targeted symbol probe: {len(ids)} instrument(s)\n")

    unsupported = 0
    for iid in ids:
        spec = get_instrument(iid)
        if spec is None:
            print(f"UNKNOWN     {iid}")
            unsupported += 1
            continue
        sym = oanda_symbol_for(spec, coverage)
        if not sym:
            print(f"NO_SYMBOL   {iid}")
            unsupported += 1
            continue
        if sym in names:
            print(f"SUPPORTED   {iid:<24} -> {sym}")
            continue

        unsupported += 1
        candidates = get_close_matches(sym, sorted(names), n=5, cutoff=0.45)
        hint = ", ".join(candidates) if candidates else "none"
        print(f"UNSUPPORTED {iid:<24} -> {sym}   candidates=[{hint}]")

    print(f"\nSummary: supported={len(ids)-unsupported} unsupported={unsupported} total={len(ids)}")
    return 2 if unsupported else 0


if __name__ == "__main__":
    raise SystemExit(main())
