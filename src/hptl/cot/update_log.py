"""Console logging helpers for COT update (flush-friendly on Windows)."""
from __future__ import annotations

import sys
from datetime import datetime, timezone


def log_step(message: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[COT {ts}Z] {message}", flush=True)


def log_kv(label: str, value: object) -> None:
    log_step(f"{label}: {value}")
