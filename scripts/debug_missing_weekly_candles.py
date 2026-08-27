#!/usr/bin/env python3
"""End-to-end trace: where completed weekly candles disappear vs provider."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.markets.canonical_identity import BY_ID
from hptl.prices.price_store import load_price_store
from hptl.prices.workstation_ohlc_export import derive_weekly_ohlc_from_daily

OUT_MD = ROOT / "data" / "audits" / "missing_weekly_candles_report.md"
OUT_JSON = ROOT / "data" / "audits" / "missing_weekly_candles_report.json"
WS_PUBLIC = ROOT / "web-dashboard" / "public" / "data" / "workstation_ohlc_latest.json"

PROBE = [
    ("Natural Gas / NG", "NATGAS_USD"),
    ("Copper / HG", "XCU_USD"),
    ("Crude Oil / CL", "WTICO_USD"),
    ("Euro FX / 6E", "EUR_USD"),
    ("Gold", "XAU_USD"),
]


def _finite(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _ohlc(bar):
    if not bar:
        return None
    if isinstance(bar, dict):
        return {
            "date": str(bar.get("date") or "")[:10],
            "open": _finite(bar.get("open")),
            "high": _finite(bar.get("high")),
            "low": _finite(bar.get("low")),
            "close": _finite(bar.get("close")),
        }
    return None


def fetch_oanda_weekly(symbol: str, count: int = 12) -> list[dict]:
    from hptl.oanda.oanda_client import api_get

    doc = api_get(
        f"/v3/instruments/{symbol}/candles",
        params={"granularity": "W", "count": str(count), "price": "M"},
    )
    out = []
    for c in doc.get("candles") or []:
        mid = c.get("mid") or {}
        row = {
            "date": str(c.get("time") or "")[:10],
            "open": _finite(mid.get("o")),
            "high": _finite(mid.get("h")),
            "low": _finite(mid.get("l")),
            "close": _finite(mid.get("c")),
            "complete": bool(c.get("complete", True)),
        }
        if row["date"] and row["close"] is not None:
            out.append(row)
    return out


def simulate_rendered(ws_weekly: list[dict], as_of: str = "2026-07-26") -> list[dict]:
    """Mirror frontend filterCompletedWorkstationOhlc (no COT truncation)."""
    import datetime as dt

    as_of_d = dt.date.fromisoformat(as_of)
    cur = as_of_d.isocalendar()
    cur_key = (cur.year, cur.week)
    kept = []
    for b in ws_weekly:
        d = str(b.get("date") or "")[:10]
        if not d:
            continue
        dd = dt.date.fromisoformat(d)
        y, w, _ = dd.isocalendar()
        if (y, w) >= cur_key:
            continue
        if b.get("high") is None or b.get("low") is None:
            continue
        if float(b["high"]) <= float(b["low"]):
            continue
        kept.append(_ohlc(b))
    return kept


def main() -> int:
    store = load_price_store()
    woh = json.loads(WS_PUBLIC.read_text(encoding="utf-8"))
    instruments = {}
    lines = [
        "# Missing Weekly Candles — End-to-End Trace",
        "",
        f"Generated: `{datetime.now(timezone.utc).isoformat()}`",
        "",
        "Root cause summary is at the bottom.",
        "",
    ]

    for iid, oanda_sym in PROBE:
        rec = (store.get("instruments") or {}).get(iid) or {}
        daily = rec.get("daily") or []
        store_weekly = [_ohlc(b) for b in (rec.get("weekly") or []) if _ohlc(b)]
        derived = [
            _ohlc(b)
            for b in derive_weekly_ohlc_from_daily(
                [
                    {
                        "date": b.get("date"),
                        "open": b.get("open"),
                        "high": b.get("high"),
                        "low": b.get("low"),
                        "close": b.get("close"),
                    }
                    for b in daily
                ]
            )
        ]
        ws_block = (woh.get("instruments") or {}).get(iid) or {}
        ws_weekly = [_ohlc(b) for b in (ws_block.get("weekly_ohlc") or []) if _ohlc(b)]
        provider = fetch_oanda_weekly(oanda_sym)
        provider_complete = [b for b in provider if b.get("complete")]
        rendered = simulate_rendered(ws_weekly)

        # Old COT-truncated render (bug reproduction)
        cot_last = str(ws_block.get("cot_last_date") or "")[:10]
        old_rendered_last = None
        # approximate: last COT-aligned date from prior bug was often cot_last - 7
        if rendered:
            # with fix, rendered tip == completed ws tip
            pass

        row = {
            "instrument": iid,
            "symbol": oanda_sym,
            "provider": {
                "last": _ohlc(provider_complete[-1]) if provider_complete else None,
                "count": len(provider_complete),
                "last_10_dates": [b["date"] for b in provider_complete[-10:]],
            },
            "daily_store": {
                "last_daily": str((daily[-1] or {}).get("date") or "")[:10] if daily else None,
                "store_weekly_last": store_weekly[-1] if store_weekly else None,
                "store_weekly_count": len(store_weekly),
            },
            "aggregation": {
                "derived_last": derived[-1] if derived else None,
                "derived_count": len(derived),
            },
            "published_json": {
                "last": ws_weekly[-1] if ws_weekly else None,
                "count": len(ws_weekly),
                "last_10_dates": [b["date"] for b in ws_weekly[-10:]],
                "cot_last_date": cot_last,
            },
            "frontend_rendered_completed": {
                "last": rendered[-1] if rendered else None,
                "count": len(rendered),
                "last_10_dates": [b["date"] for b in rendered[-10:]],
            },
        }

        # Compare last 10 provider vs rendered
        prov10 = provider_complete[-10:]
        rend10 = rendered[-10:]
        mismatches = []
        # Align on dates present in provider tip
        rend_by_date = {b["date"]: b for b in rend10}
        for pb in prov10:
            rb = rend_by_date.get(pb["date"])
            if not rb:
                mismatches.append(f"missing rendered candle for provider week {pb['date']}")
                continue
            for k in ("open", "high", "low", "close"):
                pv, rv = pb.get(k), rb.get(k)
                if pv is None or rv is None or abs(pv - rv) > max(abs(pv) * 0.0005, 1e-6):
                    mismatches.append(
                        f"{pb['date']} {k}: provider={pv} rendered={rv}"
                    )
                    break

        # Weeks behind
        behind = 0
        if provider_complete and rendered:
            p_dates = {b["date"] for b in provider_complete}
            r_dates = {b["date"] for b in rendered}
            for d in sorted(p_dates)[-5:]:
                if d not in r_dates:
                    behind += 1

        row["series_compare_last_10"] = {
            "mismatches": mismatches,
            "provider_weeks_missing_from_render": behind,
            "status": "PASS" if not mismatches else "FAIL",
        }
        instruments[iid] = row

        lines.extend(
            [
                f"## {iid} (`{oanda_sym}`)",
                "",
                "### Provider (OANDA W, complete only)",
                f"- Last weekly candle: `{row['provider']['last']}`",
                f"- Count: **{row['provider']['count']}**",
                f"- Last 10 dates: `{row['provider']['last_10_dates']}`",
                "",
                "### Daily store",
                f"- Last raw daily: `{row['daily_store']['last_daily']}`",
                f"- Store weekly last: `{row['daily_store']['store_weekly_last']}`",
                f"- Store weekly count: **{row['daily_store']['store_weekly_count']}**",
                "",
                "### Aggregation (ISO from daily)",
                f"- Derived weekly last: `{row['aggregation']['derived_last']}`",
                f"- Derived count: **{row['aggregation']['derived_count']}**",
                "",
                "### Published workstation JSON",
                f"- Last weekly candle: `{row['published_json']['last']}`",
                f"- Count: **{row['published_json']['count']}**",
                f"- Last 10 dates: `{row['published_json']['last_10_dates']}`",
                f"- COT last date (must NOT truncate price): `{cot_last}`",
                "",
                "### Frontend completed render (no COT truncate)",
                f"- Last weekly candle: `{row['frontend_rendered_completed']['last']}`",
                f"- Count: **{row['frontend_rendered_completed']['count']}**",
                f"- Last 10 dates: `{row['frontend_rendered_completed']['last_10_dates']}`",
                "",
                f"### Series compare (last 10) — **{row['series_compare_last_10']['status']}**",
            ]
        )
        if mismatches:
            for m in mismatches:
                lines.append(f"- FAIL: {m}")
        else:
            lines.append("- All last-10 provider weeks present with matching OHLC.")
        lines.append("")

    lines.extend(
        [
            "## Root cause",
            "",
            "1. `filterCompletedWorkstationOhlc` previously dropped bars after the COT week cap (`after_cot_last`).",
            "2. `buildPositioningWorkstationSeries` built price candles only inside the COT-week loop,",
            "   re-keying candles to COT dates. The latest COT week often failed OHLC match",
            "   (incomplete ISO week + no-reuse rule), so the chart ended ~2 COT weeks early.",
            "3. Workstation JSON preferred ISO-daily aggregation tip over OANDA native Friday weeks,",
            "   so provider week dates (Fri) did not match published tip dates (Sun/partial).",
            "",
            "Fix:",
            "- Price series = completed provider/store weekly bars (not COT-truncated).",
            "- COT values attach onto the timeline and stop at the latest report.",
            "- Export stitches provider-native weekly tip onto deep daily-derived history.",
            "- Alignment audit compares the last 10 completed weekly OHLC candles to the provider.",
            "",
        ]
    )

    overall = (
        "PASS"
        if all(v["series_compare_last_10"]["status"] == "PASS" for v in instruments.values())
        else "FAIL"
    )
    lines.extend(["## OVERALL STATUS", "", overall, ""])

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "instruments": instruments,
        "overall_status": overall,
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT_MD}")
    print(f"OVERALL STATUS: {overall}")
    return 0 if overall == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
