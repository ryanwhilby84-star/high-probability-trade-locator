"""FastAPI entrypoint for the Institutional Edge production backend."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException

from hptl.config import PROJECT_ROOT
from hptl.dashboard.pipeline_freshness import build_pipeline_freshness_report

PUBLIC_DATA = PROJECT_ROOT / "web-dashboard" / "public" / "data"

app = FastAPI(
    title="Institutional Edge API",
    version="0.1.0",
    description="Read-only production API for Institutional Edge market research data.",
)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise HTTPException(status_code=503, detail=f"Required data file is missing: {path.name}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=503, detail=f"Unable to read {path.name}: {exc}") from exc


def _resolve_market(markets: dict[str, Any], requested: str) -> tuple[str, dict[str, Any]] | None:
    if requested in markets:
        return requested, markets[requested]

    needle = requested.lower().replace("/", " ").replace("_", " ").strip()
    aliases = {
        "soybeans": {"soybeans", "soybean", "soybeans zs", "soybeans / zs"},
    }
    accepted = aliases.get(needle, {needle})

    for name, block in markets.items():
        normalized = name.lower().replace("/", " ").replace("_", " ").strip()
        if normalized in accepted or needle in normalized:
            return name, block
    return None


@app.get("/health")
def health() -> dict[str, Any]:
    report = build_pipeline_freshness_report()
    summary = report.summary
    healthy = int(summary.get("failed", 0)) == 0
    return {
        "status": "ok" if healthy else "degraded",
        "service": "institutional-edge-api",
        "checked_at": report.checked_at,
        "summary": summary,
    }


@app.get("/freshness")
def freshness() -> dict[str, Any]:
    return build_pipeline_freshness_report().as_dict()


@app.get("/api/v1/cot/{market}")
def cot_market(market: str) -> dict[str, Any]:
    doc = _read_json(PUBLIC_DATA / "cot_3y_series_latest.json")
    markets = doc.get("markets") or {}
    resolved = _resolve_market(markets, market)
    if resolved is None:
        raise HTTPException(status_code=404, detail=f"Unknown COT market: {market}")
    name, block = resolved
    return {
        "market": name,
        "latest_date": block.get("latest_date"),
        "data": block,
    }


@app.get("/api/v1/markets/soybeans")
def soybeans_snapshot() -> dict[str, Any]:
    cot_doc = _read_json(PUBLIC_DATA / "cot_3y_series_latest.json")
    ohlc_doc = _read_json(PUBLIC_DATA / "workstation_ohlc_latest.json")

    cot_resolved = _resolve_market(cot_doc.get("markets") or {}, "soybeans")
    ohlc_resolved = _resolve_market(ohlc_doc.get("instruments") or {}, "soybeans")

    if cot_resolved is None:
        raise HTTPException(status_code=503, detail="Soybeans COT data is unavailable")
    if ohlc_resolved is None:
        raise HTTPException(status_code=503, detail="Soybeans OHLC data is unavailable")

    cot_name, cot_block = cot_resolved
    _, ohlc_block = ohlc_resolved
    return {
        "market": cot_name,
        "cot_latest": cot_block.get("latest_date"),
        "ohlc_latest": ohlc_block.get("ohlc_last_date"),
        "cot": cot_block,
        "ohlc": ohlc_block,
    }
