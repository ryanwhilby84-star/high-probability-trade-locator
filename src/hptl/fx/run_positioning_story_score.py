"""Export FX positioning story score JSON (research / attention layer only)."""

from __future__ import annotations

from hptl.fx.positioning_story_score import write_positioning_story


def main() -> None:
    path = write_positioning_story()
    print(f"Wrote positioning story: {path}")


if __name__ == "__main__":
    main()
