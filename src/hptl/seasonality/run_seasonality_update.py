"""CLI: rebuild seasonality_latest.json."""
from __future__ import annotations

from hptl.seasonality.export import build_seasonality_latest, write_seasonality_exports


def main() -> int:
    payload = build_seasonality_latest()
    paths = write_seasonality_exports(payload)
    s = payload["summary"]
    print(f"Wrote {paths['public']}")
    print(f"Wired={s['wired_count']}/{s['total_instruments']} unavailable={s['unavailable_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
