#!/usr/bin/env python3
"""Gold price truth audit — one source-of-truth diagnostic across all layers.

Usage:
    python scripts/gold_price_truth_audit.py
    python scripts/gold_price_truth_audit.py --write-public
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

INSTRUMENT = "Gold"
OANDA_SYMBOL = "XAU_USD"
PUBLIC = ROOT / "web-dashboard" / "public" / "data"
AUDIT_DIR = ROOT / "data" / "audits"


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _tail(items: list, n: int = 12) -> list:
    return items[-n:] if items else []


def _fetch_oanda_live() -> dict[str, Any]:
    out: dict[str, Any] = {
        "source_symbol": OANDA_SYMBOL,
        "bid": None,
        "ask": None,
        "mid": None,
        "timestamp": None,
        "raw_api_time": None,
        "fetch_ok": False,
        "error": None,
    }
    try:
        from hptl.config import get_oanda_api_key
        from hptl.oanda.oanda_client import OandaApiError, api_get, resolve_account_id

        if not get_oanda_api_key():
            out["error"] = "OANDA_API_KEY not configured"
            return out
        aid = resolve_account_id()
        doc = api_get(
            f"/v3/accounts/{aid}/pricing",
            params={"instruments": OANDA_SYMBOL},
        )
        row = (doc.get("prices") or [{}])[0]
        bids = row.get("bids") or []
        asks = row.get("asks") or []
        bid = float(bids[0]["price"]) if bids else None
        ask = float(asks[0]["price"]) if asks else None
        mid = (bid + ask) / 2.0 if bid is not None and ask is not None else bid or ask
        out.update(
            {
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "raw_api_time": str(row.get("time") or "")[:32] or None,
                "fetch_ok": mid is not None,
            }
        )
    except Exception as exc:
        out["error"] = str(exc)[:300]
    return out


def _simulate_visible_bars(ws_block: dict[str, Any], cot_block: dict[str, Any], weeks: int = 52) -> dict[str, Any]:
    """Mirror frontend: slice bars to last N COT weeks ending at cot last date."""
    cot_series = cot_block.get("series") or []
    if not cot_series:
        return {"visible_bars": [], "visible_from": None, "visible_to": None}
    visible_cot = cot_series[-weeks:] if weeks else cot_series
    from_d = str(visible_cot[0].get("date") or "")[:10]
    to_d = str(visible_cot[-1].get("date") or "")[:10]
    weekly = ws_block.get("weekly_ohlc") or []
    visible = [b for b in weekly if (not from_d or b["date"] >= from_d) and (not to_d or b["date"] <= to_d)]
    return {
        "visible_from_cot": from_d,
        "visible_to_cot": to_d,
        "visible_bars": _tail(visible, 12),
        "last_visible_bar": visible[-1] if visible else None,
        "note": "Mirrors sliceBarsByDateRange(binding.weeklyBars, firstCotWeek, lastCotWeek)",
    }


def _detect_mismatches(audit: dict[str, Any]) -> list[dict[str, Any]]:
    mismatches: list[dict[str, Any]] = []
    live = audit.get("backend_live_quote") or {}
    live_mid = live.get("mid")
    lq = (audit.get("frontend_json") or {}).get("live_quotes_latest") or {}
    ws = (audit.get("frontend_json") or {}).get("workstation_ohlc_latest") or {}
    val = (audit.get("frontend_json") or {}).get("valuation_latest") or {}

    latest_completed = ws.get("latest_completed_weekly") or {}
    latest_close = latest_completed.get("close")
    matched = ws.get("last_cot_match") or {}
    sim = audit.get("simulated_frontend_visible") or {}
    last_vis = sim.get("last_visible_bar") or {}

    def add(claimed: str, expected: Any, actual: Any, component: str, *, severity: str = "error") -> None:
        if expected is None or actual is None:
            return
        if abs(float(expected) - float(actual)) > 0.05:
            mismatches.append(
                {
                    "claimed_source": claimed,
                    "expected": expected,
                    "actual": actual,
                    "component": component,
                    "severity": severity,
                }
            )

    if live_mid and lq.get("live_price"):
        add("live_quotes export mid", live_mid, lq["live_price"], "live_quotes_latest.json")

    if live_mid and val.get("live_price"):
        add("valuation live_price", live_mid, val["live_price"], "valuation_latest.json")

    # Chart close label uses last visible bar, not latest completed week
    if latest_close and last_vis.get("close"):
        if matched.get("ohlc_date") and last_vis.get("date") != matched.get("ohlc_date"):
            mismatches.append(
                {
                    "claimed_source": "Chart close label: completed weekly OHLC",
                    "expected": f"{matched.get('close')} (matched OHLC week {matched.get('ohlc_date')} for COT {matched.get('cot_date')})",
                    "actual": f"{last_vis.get('close')} (last visible bar {last_vis.get('date')})",
                    "component": "PositioningChartStack.visibleBars + sliceBarsByDateRange(cotLastDate excludes later OHLC week)",
                    "severity": "error",
                }
            )
        elif latest_close != last_vis.get("close"):
            mismatches.append(
                {
                    "claimed_source": "Chart close label vs latest completed weekly OHLC",
                    "expected": latest_close,
                    "actual": last_vis.get("close"),
                    "component": "buildPriceContextFromSources uses visibleBars[-1], not latest_completed_ohlc",
                    "severity": "warning",
                }
            )

    if val.get("spot_price") and latest_close and val["spot_price"] != latest_close:
        if not val.get("live_price"):
            mismatches.append(
                {
                    "claimed_source": "valuation spot_price (model weekly)",
                    "expected": latest_close,
                    "actual": val["spot_price"],
                    "component": "valuation_latest.json stale — export predates OHLC refresh",
                    "severity": "warning",
                }
            )

    cot_price = matched.get("cot_price")
    ohlc_close = matched.get("close")
    if cot_price and ohlc_close and cot_price != ohlc_close:
        mismatches.append(
            {
                "claimed_source": "COT row price field vs matched OHLC close",
                "expected": ohlc_close,
                "actual": cot_price,
                "component": "COT series forward-filled price (not chart OHLC) — informational",
                "severity": "info",
            }
        )

    return mismatches


def build_gold_price_truth_audit(*, fetch_live: bool = True) -> dict[str, Any]:
    live_quotes_doc = _load_json(PUBLIC / "live_quotes_latest.json")
    ws_doc = _load_json(PUBLIC / "workstation_ohlc_latest.json")
    val_doc = _load_json(PUBLIC / "valuation_latest.json")
    prices_doc = _load_json(PUBLIC / "prices_latest.json")
    cot_doc = _load_json(PUBLIC / "cot_3y_series_latest.json")

    lq = (live_quotes_doc.get("instruments") or {}).get(INSTRUMENT) or {}
    ws = (ws_doc.get("instruments") or {}).get(INSTRUMENT) or {}
    val = (val_doc.get("instruments") or {}).get(INSTRUMENT) or {}
    px = (prices_doc.get("instruments") or {}).get(INSTRUMENT) or {}
    cot = (cot_doc.get("markets") or {}).get(INSTRUMENT) or {}

    weekly = ws.get("weekly_ohlc") or []
    matched_all = (ws.get("tail_alignment_audit") or {}).get("final_12_matched") or []
    last_match = next((m for m in reversed(matched_all) if m.get("matched")), {})

    latest_weekly = weekly[-1] if weekly else {}
    sim = _simulate_visible_bars(ws, cot)

    audit: dict[str, Any] = {
        "instrument": INSTRUMENT,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tradingview_reference": {
            "intended": "OANDA XAUUSD spot (TradingView symbol XAUUSD)",
            "oanda_symbol": OANDA_SYMBOL,
        },
        "backend_live_quote": _fetch_oanda_live() if fetch_live else {"skipped": True},
        "backend_weekly_ohlc": {
            "source_symbol": ws.get("price_source") or ws.get("canonical_symbol"),
            "canonical_symbol": ws.get("canonical_symbol"),
            "final_12_weekly_bars": _tail(weekly),
            "latest_completed_weekly": latest_weekly,
            "last_cot_match": last_match,
            "cot_last_date": ws.get("cot_last_date"),
            "ohlc_last_date": ws.get("ohlc_last_date"),
        },
        "simulated_frontend_visible": sim,
        "frontend_json": {
            "live_quotes_latest": {
                "generated_at": live_quotes_doc.get("generated_at"),
                "row": lq,
            },
            "workstation_ohlc_latest": {
                "generated_at": ws_doc.get("generated_at"),
                "price_source": ws.get("price_source"),
                "final_12_weekly_bars": _tail(weekly),
                "final_12_matched": _tail(matched_all),
            },
            "valuation_latest": {
                "generated_at": val_doc.get("generated_at"),
                "spot_price": val.get("spot_price"),
                "model_spot_price": val.get("model_spot_price"),
                "live_price": val.get("live_price"),
                "valuation_price_used": val.get("valuation_price_used"),
                "valuation_price_source": val.get("valuation_price_source"),
                "display_valuation_pct": val.get("display_valuation_pct"),
                "fair_value": val.get("fair_value"),
            },
            "prices_latest_snapshot": {
                "generated_at": prices_doc.get("generated_at"),
                "price_mid": (px.get("price") or {}).get("mid"),
                "price_as_of": (px.get("price") or {}).get("as_of"),
                "daily_last": (px.get("daily") or [])[-1] if px.get("daily") else None,
                "weekly_last": (px.get("weekly") or [])[-1] if px.get("weekly") else None,
            },
        },
    }
    audit["mismatches"] = _detect_mismatches(audit)
    return audit


def _format_md(audit: dict[str, Any]) -> str:
    lines = [
        f"# Gold price truth audit",
        "",
        f"Generated: {audit.get('generated_at')}",
        "",
        "## 1. TradingView reference",
        f"- Intended: {audit['tradingview_reference']['intended']}",
        "",
        "## 2. Backend live quote (OANDA API)",
    ]
    live = audit["backend_live_quote"]
    for k in ("source_symbol", "bid", "ask", "mid", "timestamp", "raw_api_time", "fetch_ok", "error"):
        lines.append(f"- {k}: {live.get(k)}")
    lines.extend(["", "## 3. Backend weekly OHLC"])
    w = audit["backend_weekly_ohlc"]
    lines.append(f"- source: {w.get('source_symbol')}")
    lines.append(f"- latest completed: {w.get('latest_completed_weekly')}")
    lines.append(f"- last COT match: {w.get('last_cot_match')}")
    lines.append("- final 12 weekly bars:")
    for b in w.get("final_12_weekly_bars") or []:
        lines.append(f"  - {b.get('date')}: O={b.get('open')} H={b.get('high')} L={b.get('low')} C={b.get('close')}")
    lines.extend(["", "## 4. Simulated chart visible bars (frontend slice)"])
    sim = audit["simulated_frontend_visible"]
    lines.append(f"- COT range: {sim.get('visible_from_cot')} .. {sim.get('visible_to_cot')}")
    lines.append(f"- last visible bar: {sim.get('last_visible_bar')}")
    lines.extend(["", "## 5. Frontend JSON snapshots"])
    fj = audit["frontend_json"]
    lines.append(f"- live_quotes: {fj['live_quotes_latest']}")
    lines.append(f"- valuation spot: {fj['valuation_latest']}")
    lines.extend(["", "## 6. Mismatches"])
    if not audit.get("mismatches"):
        lines.append("- None detected")
    else:
        for m in audit["mismatches"]:
            lines.append(f"- **[{m.get('severity')}]** {m.get('claimed_source')}")
            lines.append(f"  - expected: {m.get('expected')}")
            lines.append(f"  - actual: {m.get('actual')}")
            lines.append(f"  - component: {m.get('component')}")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Gold price truth audit")
    parser.add_argument("--write-public", action="store_true", help="Copy JSON to public/data for dashboard")
    parser.add_argument("--no-fetch", action="store_true", help="Skip live OANDA fetch")
    args = parser.parse_args()

    audit = build_gold_price_truth_audit(fetch_live=not args.no_fetch)
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = AUDIT_DIR / "gold_price_truth_audit.json"
    md_path = AUDIT_DIR / "gold_price_truth_audit.md"
    json_path.write_text(json.dumps(audit, indent=2), encoding="utf-8")
    md_path.write_text(_format_md(audit), encoding="utf-8")
    print(_format_md(audit))
    if args.write_public:
        pub = PUBLIC / "gold_price_truth_audit.json"
        pub.parent.mkdir(parents=True, exist_ok=True)
        pub.write_text(json.dumps(audit, indent=2), encoding="utf-8")
        print(f"Wrote {pub}")
    print(f"Wrote {json_path}")
    return 1 if any(m.get("severity") == "error" for m in audit.get("mismatches") or []) else 0


if __name__ == "__main__":
    raise SystemExit(main())
