"""CLI shim — see ``hptl.cot.run_cot_groups_integrity``."""
from __future__ import annotations

import sys

from hptl.cot.run_cot_groups_integrity import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
