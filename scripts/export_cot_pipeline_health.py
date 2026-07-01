#!/usr/bin/env python3
"""Export COT pipeline health JSON for dashboard."""
from __future__ import annotations

from hptl.cot.pipeline_health import write_cot_pipeline_health


def main() -> int:
    paths = write_cot_pipeline_health(probe_cftc=True)
    print(f"Wrote {paths['public_json']}")
    print(f"Wrote {paths['report_md']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
