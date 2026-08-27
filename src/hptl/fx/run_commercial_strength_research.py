"""Export commercial strength research JSON + audit (research layer only)."""

from __future__ import annotations

from hptl.fx.commercial_strength_research import write_commercial_strength_research


def main() -> None:
    paths = write_commercial_strength_research()
    for label, path in paths.items():
        print(f"Wrote {label}: {path}")


if __name__ == "__main__":
    main()
