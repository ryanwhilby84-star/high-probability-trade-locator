#!/usr/bin/env python3
"""Verify FX valuation dashboard exports — paths, mtimes, scanner % values."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.valuation_export_verify import print_verification_report


def main() -> int:
    return print_verification_report()


if __name__ == "__main__":
    raise SystemExit(main())
