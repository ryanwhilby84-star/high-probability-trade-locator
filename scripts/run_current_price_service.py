"""Development launcher for the HPTL Current Price Service (Phase 2).

Starts the FastAPI + OANDA pricing-stream service with clear logging.

Usage:
    python scripts/run_current_price_service.py
    python scripts/run_current_price_service.py --port 8787 --reload

Environment:
    OANDA_API_KEY       required (personal access token)
    OANDA_ENVIRONMENT   'live' for production stream (api/stream-fxtrade),
                        otherwise practice.
    OANDA_ACCOUNT_ID    optional; auto-resolved from the discovery mapping.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the HPTL Current Price Service")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--reload", action="store_true", help="auto-reload (development)")
    args = parser.parse_args()

    import uvicorn

    uvicorn.run(
        "hptl.prices.current_price_api:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
