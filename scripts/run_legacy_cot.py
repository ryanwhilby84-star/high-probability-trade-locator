"""CLI shim — see ``hptl.cot.run_legacy_cot``."""
from __future__ import annotations

import sys

from hptl.cot.run_legacy_cot import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
