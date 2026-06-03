"""CLI: rebuild valuation_latest.json."""
from __future__ import annotations

from hptl.valuation.export import build_valuation_latest, write_valuation_exports


def main() -> int:
    payload = build_valuation_latest()
    paths = write_valuation_exports(payload)
    s = payload["summary"]
    print(f"Wrote {paths['public']}")
    print(f"Wired={s['wired_count']}/{s['total_instruments']} unavailable={s['unavailable_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
