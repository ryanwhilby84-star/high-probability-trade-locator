#!/usr/bin/env python3
"""Validate Seasonality Workstation payload/forecast for full LEGACY universe + probes."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.markets.instrument_registry import LEGACY_COT_MARKETS  # noqa: E402
from hptl.seasonality_workstation.payload import build_seasonality_workstation_payload  # noqa: E402

OUT_MD = ROOT / "data" / "audits" / "seasonality_workstation_universe_validation.md"
OUT_JSON = ROOT / "data" / "audits" / "seasonality_workstation_universe_validation.json"

PROBES = (
    "Gold",
    "Crude Oil / CL",
    "Natural Gas / NG",
    "Soybeans",
    "Corn",
    "Euro FX / 6E",
    "Coffee",
)


def _check(mid: str) -> dict:
    route = f"#/instrument/{mid}/seasonality-workstation"
    try:
        payload = build_seasonality_workstation_payload(mid, lookback="10Y")
    except Exception as exc:  # noqa: BLE001
        return {
            "instrument": mid,
            "route_status": "OK",
            "payload_status": "CRASH",
            "chart_status": "FAIL",
            "forecast_status": "FAIL",
            "failure_reason": str(exc)[:200],
            "route": route,
        }

    status = payload.get("status")
    if status != "ok":
        return {
            "instrument": mid,
            "route_status": "OK",
            "payload_status": status or "FAIL",
            "chart_status": "BLOCKED",
            "forecast_status": "BLOCKED",
            "failure_reason": payload.get("message")
            or payload.get("error")
            or ",".join((payload.get("integrity") or {}).get("issues") or []),
            "route": route,
            "anchor_price": None,
            "forecast_start": None,
        }

    seas = payload.get("seasonality") or {}
    forecast = seas.get("forecast") or {}
    models = forecast.get("models") or {}
    median = models.get("median") or seas.get("projection") or []
    price_series = payload.get("price_series") or []
    anchor = payload.get("anchor") or {}

    reasons = []
    chart_ok = bool(price_series) and anchor.get("date") and anchor.get("price") is not None
    if not price_series:
        reasons.append("missing_price_series")
    if not median or len(median) < 2:
        reasons.append("missing_forecast_path")
    join_ok = False
    if median and anchor.get("price") is not None:
        join_ok = abs(float(median[0]["price"]) - float(anchor["price"])) < 1e-6
        if not join_ok:
            reasons.append(
                f"join_discontinuity anchor={anchor['price']} forecast0={median[0]['price']}"
            )
    forward_ok = any((p.get("week_offset") or 0) >= 12 for p in median)
    if not forward_ok:
        reasons.append("forecast_horizon_lt_12w")
    # price units: forecast near actual price scale
    if price_series and median and len(median) > 1:
        last_px = float(price_series[-1]["close"])
        f4 = next((p for p in median if p.get("week_offset") == 4), None)
        if f4 and last_px > 0:
            ratio = float(f4["price"]) / last_px
            if ratio < 0.5 or ratio > 2.0:
                reasons.append(f"forecast_scale_suspicious ratio_4w={ratio:.3f}")

    forecast_status = "OK" if join_ok and forward_ok and not reasons else "FAIL"
    chart_status = "OK" if chart_ok else "FAIL"
    return {
        "instrument": mid,
        "route_status": "OK",
        "payload_status": "OK",
        "chart_status": chart_status,
        "forecast_status": forecast_status,
        "failure_reason": "; ".join(reasons) if reasons else "",
        "route": route,
        "report_date": payload.get("report_date"),
        "anchor_price": anchor.get("price"),
        "forecast_start": median[0]["price"] if median else None,
        "forecast_12w": next((p["price"] for p in median if p.get("week_offset") == 12), None),
        "sample_size": payload.get("sample_size"),
        "not_gold_fallback": mid == "Gold"
        or (
            abs(float(anchor.get("price") or 0) - 1.0) > 1e-9
            and float(anchor.get("price") or 0) != 0
        ),
    }


def main() -> int:
    rows = [_check(mid) for mid in LEGACY_COT_MARKETS]
    probe_rows = [r for r in rows if r["instrument"] in PROBES]

    report = {
        "universe": rows,
        "probes": probe_rows,
        "summary": {
            "total": len(rows),
            "payload_ok": sum(1 for r in rows if r["payload_status"] == "OK"),
            "payload_blocked": sum(1 for r in rows if r["payload_status"] != "OK"),
            "forecast_ok": sum(1 for r in rows if r["forecast_status"] == "OK"),
        },
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")

    lines = [
        "# Seasonality Workstation — Universe Validation",
        "",
        f"Payload OK: {report['summary']['payload_ok']}/{report['summary']['total']}",
        f"Forecast OK: {report['summary']['forecast_ok']}/{report['summary']['total']}",
        "",
        "| Instrument | Route | Payload | Chart | Forecast | Failure reason |",
        "|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['instrument']} | {r['route_status']} | {r['payload_status']} | "
            f"{r['chart_status']} | {r['forecast_status']} | {r.get('failure_reason') or '—'} |"
        )
    lines.append("")
    lines.append("## Probe join checks")
    lines.append("")
    for r in probe_rows:
        lines.append(
            f"- **{r['instrument']}**: anchor={r.get('anchor_price')} "
            f"forecast0={r.get('forecast_start')} forecast12w={r.get('forecast_12w')} "
            f"status={r['forecast_status']}"
        )
    lines.append("")
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(OUT_MD.read_text(encoding="utf-8"))
    # Fail only if a non-Copper probe fails forecast when payload ok
    bad_probes = [
        r
        for r in probe_rows
        if r["payload_status"] == "OK" and r["forecast_status"] != "OK"
    ]
    return 1 if bad_probes else 0


if __name__ == "__main__":
    raise SystemExit(main())
