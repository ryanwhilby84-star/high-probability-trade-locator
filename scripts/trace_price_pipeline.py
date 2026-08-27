#!/usr/bin/env python3
"""Read-only price pipeline trace — source to workstation binding."""
from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.oanda.oanda_prices import fetch_candles, fetch_pricing
from hptl.prices.cot_fail_backfill import OANDA_COT_FAIL_PAIRS

INSTRUMENTS = [
    "Gold",
    "Crude Oil / CL",
    "Sugar",
    "Soybeans",
    "NASDAQ / NQ",
]

OANDA_MAP = {k: sym for _, sym, k in OANDA_COT_FAIL_PAIRS}
OANDA_MAP["Gold"] = "XAU_USD"
OANDA_MAP["NASDAQ / NQ"] = "NAS100_USD"

PRICES_DIR = ROOT / "data" / "processed" / "prices"
PRICES_LATEST = ROOT / "data" / "processed" / "prices_latest.json"
WS_OHLC = ROOT / "data" / "processed" / "workstation_ohlc_latest.json"
WS_OHLC_PUB = ROOT / "web-dashboard" / "public" / "data" / "workstation_ohlc_latest.json"
COT_3Y = ROOT / "data" / "processed" / "cot_3y_series_latest.json"
CACHE_NAS = ROOT / "data" / "cache" / "workstation_ohlc" / "NAS100_USD.json"
BACKFILL_DIR = PRICES_DIR / "backfill"
OUT_JSON = ROOT / "data" / "audits" / "price_pipeline_trace_latest.json"


def safe_name(iid: str) -> str:
    return re.sub(r"[^\w\-]+", "_", iid.strip()).strip("_") or "instrument"


def load_json(path: Path) -> dict | None:
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def pct_diff(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b == 0:
        return None
    return round(100 * (a - b) / b, 4)


def values_match(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return a is None and b is None
    tol = max(abs(b) * 0.0005, 1e-6)
    return abs(a - b) <= tol


def find_first_mismatch(stages: list[tuple[str, float | None, str | None]], ref: float) -> dict | None:
    """Walk stages in order; stop at first value that differs from reference."""
    for name, val, file_hint in stages:
        if val is None:
            continue
        if not values_match(val, ref):
            return {
                "stage": name,
                "value": val,
                "reference": ref,
                "diff_pct": pct_diff(val, ref),
                "file": file_hint,
            }
    return None


def run_workstation_binding() -> dict[str, dict]:
    node = r"""
const fs = require('fs');
const path = require('path');
const ROOT = process.argv[1];
const instruments = JSON.parse(process.argv[2]);

async function main() {
  const { buildCotWorkstation } = await import(pathToFileURL(path.join(ROOT, 'web-dashboard/src/cot/buildCotWorkstation.js')).href);
  const { buildPositioningWorkstationSeries } = await import(pathToFileURL(path.join(ROOT, 'web-dashboard/src/workstation/data/buildPositioningWorkstationSeries.js')).href);

  const cotPath = path.join(ROOT, 'data/processed/cot_3y_series_latest.json');
  const wsPath = path.join(ROOT, 'data/processed/workstation_ohlc_latest.json');
  const cotDoc = JSON.parse(fs.readFileSync(cotPath, 'utf8'));
  const wsDoc = JSON.parse(fs.readFileSync(wsPath, 'utf8'));

  const out = {};
  for (const iid of instruments) {
    const block = cotDoc.markets?.[iid];
    const exportBlock = wsDoc.instruments?.[iid];
    if (!block) { out[iid] = { error: 'no_cot_block' }; continue; }
    const model = buildCotWorkstation(block);
    const binding = buildPositioningWorkstationSeries(model, null, exportBlock, { preserveFullCotHistory: true });
    const bars = binding.weeklyBars || [];
    const last = bars[bars.length - 1];
    const lastRow = (binding.rows || [])[binding.rows.length - 1];
    out[iid] = {
      rendered_weekly_close: last?.close ?? null,
      rendered_weekly_date: last?.date ?? null,
      rendered_row_close: lastRow?.close ?? null,
      rendered_row_price: lastRow?.price ?? null,
      weekly_bar_count: bars.length,
      filtered_weekly_bars: binding.meta?.filteredWeeklyBars,
      aligned_ohlc_weeks: binding.meta?.alignedOhlcWeeks,
      cot_last_date: binding.meta?.cotLastDate,
      source: 'buildPositioningWorkstationSeries (CotWorkstation.jsx path)',
    };
  }
  console.log(JSON.stringify(out));
}

import { pathToFileURL } from 'url';
main().catch(e => { console.error(e); process.exit(1); });
"""
    script_path = ROOT / "data" / "audits" / "_trace_binding.mjs"
    script_path.write_text(node, encoding="utf-8")
    proc = subprocess.run(
        ["node", str(script_path), str(ROOT), json.dumps(INSTRUMENTS)],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    if proc.returncode != 0:
        return {"_error": proc.stderr or proc.stdout}
    return json.loads(proc.stdout)


def main() -> int:
    prices_latest = load_json(PRICES_LATEST) or {}
    ws_doc = load_json(WS_OHLC) or load_json(WS_OHLC_PUB) or {}
    cot_doc = load_json(COT_3Y) or {}

    symbols = sorted(set(OANDA_MAP[i] for i in INSTRUMENTS))
    live = fetch_pricing(symbols)

    binding = run_workstation_binding()

    report: list[dict] = []

    for iid in INSTRUMENTS:
        sym = OANDA_MAP[iid]
        snap = live.get(sym) or {}
        ref_live = snap.get("mid")

        entry: dict = {
            "instrument": iid,
            "source_provider": "oanda",
            "source_symbol": sym,
            "reference_live": ref_live,
            "reference_as_of": snap.get("as_of"),
            "pipeline": {},
            "first_failure": None,
        }

        # 1. Raw API latest daily
        try:
            daily_raw, meta = fetch_candles(sym, granularity="D", count=5)
            last = daily_raw[-1] if daily_raw else {}
            entry["pipeline"]["1_raw_api_daily"] = {
                "close": last.get("close"),
                "date": last.get("date"),
                "file": "OANDA API candles (live fetch)",
            }
        except Exception as exc:
            entry["pipeline"]["1_raw_api_daily"] = {"error": str(exc)}

        raw_close = entry["pipeline"]["1_raw_api_daily"].get("close")

        # Per-instrument store
        pf = PRICES_DIR / f"{safe_name(iid)}.json"
        pdoc = load_json(pf)
        proc_close = None
        if pdoc:
            pd = pdoc.get("daily") or []
            lb = pd[-1] if pd else {}
            proc_close = lb.get("close")
            entry["pipeline"]["2_processed_per_instrument"] = {
                "provider": pdoc.get("_fetched_via"),
                "symbol": (pdoc.get("price_scale") or {}).get("symbol") or sym,
                "close": proc_close,
                "date": lb.get("date"),
                "file": str(pf),
            }

        prec = (prices_latest.get("instruments") or {}).get(iid) or {}
        pd = prec.get("daily") or []
        lb = pd[-1] if pd else {}
        pl_close = lb.get("close")
        entry["pipeline"]["3_processed_prices_latest"] = {
            "close": pl_close,
            "date": lb.get("date"),
            "file": str(PRICES_LATEST),
        }

        ws_block = (ws_doc.get("instruments") or {}).get(iid) or {}
        weekly = ws_block.get("weekly_ohlc") or []
        wl = weekly[-1] if weekly else {}
        ws_close = wl.get("close")
        entry["pipeline"]["4_weekly_ohlc_export"] = {
            "provider": ws_block.get("canonical_source"),
            "symbol": ws_block.get("canonical_symbol"),
            "close": ws_close,
            "date": wl.get("date"),
            "file": str(WS_OHLC),
        }

        bind = binding.get(iid) or {}
        rendered = bind.get("rendered_weekly_close")
        entry["pipeline"]["5_workstation_rendered"] = {
            "close": rendered,
            "date": bind.get("rendered_weekly_date"),
            "row_close": bind.get("rendered_row_close"),
            "file": "web-dashboard/src/workstation/data/buildPositioningWorkstationSeries.js",
            "meta": {k: bind.get(k) for k in ("weekly_bar_count", "filtered_weekly_bars", "aligned_ohlc_weeks", "cot_last_date")},
        }

        entry["pipeline"]["6_reference_live"] = {
            "close": ref_live,
            "date": snap.get("as_of"),
            "file": "OANDA API pricing (live fetch)",
        }

        # Compare chain: raw -> processed -> export -> rendered vs reference
        # User wants first point of failure vs reference (TradingView/OANDA)
        chain = [
            ("1_raw_api_daily", raw_close, entry["pipeline"]["1_raw_api_daily"].get("file", "")),
            ("2_processed_per_instrument", proc_close, str(pf)),
            ("3_processed_prices_latest", pl_close, str(PRICES_LATEST)),
            ("4_weekly_ohlc_export", ws_close, str(WS_OHLC)),
            ("5_workstation_rendered", rendered, "buildPositioningWorkstationSeries.js"),
        ]

        # First: check if raw differs from live reference (stale vs wrong)
        if raw_close is not None and ref_live is not None:
            if not values_match(raw_close, ref_live):
                # Could be stale (different dates) — note age
                raw_date = entry["pipeline"]["1_raw_api_daily"].get("date")
                entry["raw_vs_live"] = {
                    "raw_close": raw_close,
                    "live_close": ref_live,
                    "diff_pct": pct_diff(raw_close, ref_live),
                    "raw_date": raw_date,
                    "note": "Daily candle close vs live mid — expected if last daily bar is prior session",
                }

        # Sequential stage integrity: each stage should match previous stage close
        prev_name, prev_val, prev_file = None, None, None
        for name, val, file_hint in chain:
            if val is None:
                continue
            if prev_val is not None and not values_match(val, prev_val):
                entry["first_failure"] = {
                    "type": "stage_mismatch",
                    "stage": name,
                    "value": val,
                    "previous_stage": prev_name,
                    "previous_value": prev_val,
                    "diff_pct": pct_diff(val, prev_val),
                    "file": file_hint,
                    "previous_file": prev_file,
                }
                break
            prev_name, prev_val, prev_file = name, val, file_hint

        # If all stages match each other, check rendered vs live reference
        if entry["first_failure"] is None and rendered is not None and ref_live is not None:
            if not values_match(rendered, ref_live):
                entry["first_failure"] = {
                    "type": "vs_live_reference",
                    "stage": "5_workstation_rendered",
                    "value": rendered,
                    "reference": ref_live,
                    "diff_pct": pct_diff(rendered, ref_live),
                    "file": "buildPositioningWorkstationSeries.js",
                    "note": "All internal stages agree; value differs from live OANDA mid because workstation shows last completed weekly close, not live quote",
                }

        report.append(entry)

    doc = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "instruments": report,
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    print(json.dumps(doc, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
