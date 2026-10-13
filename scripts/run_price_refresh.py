#!/usr/bin/env python3
"""CLI entry point that propagates price-refresh failures to GitHub Actions."""
from hptl.prices.run_price_refresh import main

if __name__ == "__main__":
    raise SystemExit(main())
