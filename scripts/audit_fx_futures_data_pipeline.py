#!/usr/bin/env python3
"""Phase 1G-A — FX futures data pipeline recovery audit (data only, no model work)."""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

from hptl.config import DATA_DIR, PROJECT_ROOT
from hptl.fx.currency_rates import get_currency_rate
from hptl.fx.fx_macro_history import (
    FRED_CHF_Y2_FALLBACK_ID,
    FRED_GBP_POLICY_ID,
    FRED_JPY_Y2_FALLBACK_ID,
    FRED_NZD_Y2_FALLBACK_ID,
    currency_histories,
    load_ecb_yield_history,
    load_fred_daily_map,
)
from hptl.prices.coverage import load_price_coverage, oanda_symbol_for, select_price_source
from hptl.prices.price_store import load_instrument_record_internal
from hptl.prices.unified_adapter import UnifiedPriceAdapter
from hptl.valuation.currency_futures_ive_v1 import FUTURES_REGISTRY, build_currency_futures_ive_export

FRED_SERIES_USED = [
    ("DFF", "USD policy (fed funds)", "dx_futures_fed_funds_v2 + USD legs"),
    ("DGS2", "USD 2Y yield", "G10 y2_diff models"),
    ("DGS10", "USD 10Y yield", "USD history (not in current IVE features)"),
    (FRED_JPY_Y2_FALLBACK_ID, "JPY 2Y fallback", "JPY y2 history extension"),
    ("IRLTLT01JPM156N", "JPY 10Y fallback", "JPY y10 history extension"),
    (FRED_CHF_Y2_FALLBACK_ID, "CHF 2Y fallback", "CHF y2 live + history extension"),
    ("IRLTLT01CHM156N", "CHF 10Y fallback", "CHF y10 live + history extension"),
    (FRED_NZD_Y2_FALLBACK_ID, "NZD 2Y fallback", "NZD y2 live + history"),
    ("IRLTLT01NZM156N", "NZD 10Y fallback", "NZD y10 live + history"),
    (FRED_GBP_POLICY_ID, "GBP policy fallback", "GBP policy history extension"),
    ("DTWEXBGS", "Broad USD index", "DX price backfill (cot_fail_backfill) — not live refresh"),
    ("IR3TIB01EUM156N", "EUR 2Y fallback (probe)", "Not wired — invalid probe"),
]


def _probe_fred(series_id: str) -> dict:
    try:
        from hptl.macro import fred_client

        df = fred_client.get_series_df(series_id, "2024-01-01")
        if df is None or df.empty:
            return {"series_id": series_id, "valid": False, "error": "empty", "latest": None, "n": 0}
        latest = str(df["date"].max())[:10]
        return {"series_id": series_id, "valid": True, "error": None, "latest": latest, "n": len(df)}
    except Exception as exc:
        return {"series_id": series_id, "valid": False, "error": str(exc)[:120], "latest": None, "n": 0}


def _price_row(sym: str, spec) -> dict:
    iid = spec.instrument_id
    cov = load_price_coverage()
    source = select_price_source(iid, cov)
    from hptl.markets.instrument_registry import get_instrument

    inst = get_instrument(iid)
    oanda_sym = oanda_symbol_for(inst, cov) if inst else None
    doc = load_instrument_record_internal(iid) or {}
    daily = doc.get("daily") or []
    price = doc.get("price") or {}
    adapter = UnifiedPriceAdapter(cov)
    live = adapter.fetch(iid)

    alpha_sym = None
    if inst:
        from hptl.alpha_vantage.mappings import resolve_alpha_mapping

        m = resolve_alpha_mapping(inst)
        if m:
            alpha_sym = f"{m.function}:{m.symbol}"

    return {
        "symbol": sym,
        "instrument_id": iid,
        "provider": source,
        "oanda_symbol": oanda_sym,
        "alpha_mapping": alpha_sym,
        "store_daily_n": len(daily),
        "store_last_date": daily[-1]["date"] if daily else None,
        "store_price_as_of": price.get("as_of"),
        "store_error": doc.get("error"),
        "live_fetch_error": live.get("error"),
        "live_daily_n": len(live.get("daily") or []),
        "live_price": live.get("price"),
        "history_available": bool(daily),
    }


def main() -> int:
    today = date.today().isoformat()
    report: dict = {"phase": "1G-A FX Futures Data Pipeline Recovery", "audit_date": today, "sections": {}}

    # 1. DX price failure
    dx_spec = FUTURES_REGISTRY["DX"]
    dx = _price_row("DX", dx_spec)
    report["sections"]["dx_price_failure"] = {
        "instrument_requested": dx["instrument_id"],
        "provider_queried": dx["provider"],
        "expected_symbol": "ICE DX futures continuous (CME) or FRED DTWEXBGS backfill",
        "actual_symbol": dx["oanda_symbol"] or dx["alpha_mapping"] or "none — not in price_coverage_audit",
        "provider": dx["provider"] or "none",
        "live_fetch": {
            "error": dx["live_fetch_error"],
            "price": dx["live_price"],
            "daily_n": dx["live_daily_n"],
        },
        "store": {
            "daily_n": dx["store_daily_n"],
            "last_date": dx["store_last_date"],
            "price_as_of": dx["store_price_as_of"],
            "error": dx["store_error"],
        },
        "root_cause": (
            "US Dollar Index / DX is absent from data/price_coverage_audit.json "
            "(not in oanda_supported or alpha_supported). UnifiedPriceAdapter returns "
            "unsupported_instrument without querying any provider. Stored history (2600 bars, "
            "last 2026-06-05) came from prior FRED DTWEXBGS backfill, not live refresh."
        ),
        "fix": (
            "Add DX to price coverage with a defined source (FRED DTWEXBGS tail extension "
            "via cot_fail_backfill, or Alpha Vantage / dedicated ICE DX futures feed). "
            "Until mapped, run_price_refresh sets error=unsupported_instrument and cannot "
            "extend stale price past 2026-06-05."
        ),
    }

    # 2. FRED audit
    fred_rows = []
    for sid, label, usage in FRED_SERIES_USED:
        row = _probe_fred(sid)
        row["label"] = label
        row["usage"] = usage
        if not row["valid"]:
            replacements = {
                "IR3TIB01EUM156N": "ECB YC SR_2Y csvdata (eur_2y_history cache, lastNObservations=260)",
                "DTWEXBGS": "N/A — series valid when probed with correct ID",
            }
            row["replacement"] = replacements.get(sid, "Use first-party source per fx_macro_history")
        else:
            row["replacement"] = None
        fred_rows.append(row)
    report["sections"]["fred_audit"] = fred_rows

    # 3. Instrument mapping
    mapping = [_price_row(sym, spec) for sym, spec in FUTURES_REGISTRY.items()]
    report["sections"]["instrument_mapping"] = mapping

    # 4. Macro history + currency rate freshness
    hist = currency_histories()
    macro_rows = []
    for sym, spec in FUTURES_REGISTRY.items():
        ccy = spec.currency
        rec = get_currency_rate(ccy)
        h = hist.get(ccy) or {}
        y2 = h.get("y2") or {}
        pol = h.get("policy") or {}
        macro_rows.append(
            {
                "symbol": sym,
                "currency": ccy,
                "features": list(spec.feature_names),
                "rate_y2_as_of": rec.y2_as_of,
                "rate_y2_stale": "y2" in rec.stale_fields,
                "rate_policy_as_of": rec.policy_rate_as_of,
                "rate_policy_stale": "policy_rate" in rec.stale_fields,
                "rate_cpi_as_of": rec.cpi_yoy_as_of,
                "rate_cpi_stale": "cpi_yoy" in rec.stale_fields,
                "history_y2_n": len(y2),
                "history_y2_latest": max(y2) if y2 else None,
                "history_policy_n": len(pol),
                "missing_fields": rec.missing_fields,
                "stale_fields": rec.stale_fields,
            }
        )
    report["sections"]["macro_freshness"] = macro_rows

    # EUR cache corruption check
    eur_y2_cache = load_ecb_yield_history("eur_2y")
    report["sections"]["eur_y2_cache"] = {
        "cache_key": "eur_2y.txt",
        "observations": len(eur_y2_cache),
        "latest": max(eur_y2_cache) if eur_y2_cache else None,
        "issue": (
            "ecb_adapter.fetch() uses lastNObservations=1 and overwrites eur_2y.txt "
            "with a single row, collapsing regression history to n=1 for 6E."
            if len(eur_y2_cache) < 52
            else "OK"
        ),
        "fix": "Separate live cache (eur_2y) from deep history cache (eur_2y_history, 260 obs).",
    }

    # 5. IVE export blockers
    doc = build_currency_futures_ive_export()
    blockers = {}
    for sym in FUTURES_REGISTRY:
        b = doc["by_symbol"][sym]
        blockers[sym] = {
            "model_status": b.get("model_status"),
            "wired": b.get("wired"),
            "blocker_codes": b.get("blocker_codes"),
            "blocker_reason": b.get("blocker_reason"),
            "price_as_of": b.get("price_as_of"),
            "panel_len": b.get("panel_len"),
            "r_squared": b.get("r_squared"),
        }
    report["sections"]["ive_blockers"] = blockers

    out_json = DATA_DIR / "audits" / "fx_futures_data_pipeline_audit.json"
    out_md = DATA_DIR / "audits" / "fx_futures_data_pipeline_audit.md"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

    lines = [
        "# Phase 1G-A — FX Futures Data Pipeline Recovery Audit",
        f"\nGenerated: {today}\n",
        "## 1. DX Price Failure\n",
        f"| Field | Value |",
        f"|-------|-------|",
        f"| Instrument requested | `{dx['instrument_id']}` |",
        f"| Provider queried | `{dx['provider'] or 'none'}` |",
        f"| Expected symbol | ICE DX futures or FRED DTWEXBGS |",
        f"| Actual symbol | `{dx['oanda_symbol'] or dx['alpha_mapping'] or 'none'}` |",
        f"| Live fetch error | `{dx['live_fetch_error']}` |",
        f"| Store daily bars | {dx['store_daily_n']} (last `{dx['store_last_date']}`) |",
        f"| Fix | Add to price_coverage + wire refresh source |",
        "",
        "## 2. FRED Series Audit\n",
        "| Series | Valid | Latest | N | Usage | Replacement |",
        "|--------|-------|--------|---|-------|-------------|",
    ]
    for r in fred_rows:
        rep = r.get("replacement") or "—"
        lines.append(
            f"| {r['series_id']} | {'yes' if r['valid'] else '**no**'} | {r.get('latest') or '—'} | {r['n']} | {r['usage']} | {rep} |"
        )

    lines.extend(["", "## 3. Instrument Mapping\n", "| Sym | Instrument | Provider | Provider symbol | Store bars | Last date | Live error |", "|-----|------------|----------|-----------------|------------|-----------|------------|"])
    for m in mapping:
        prov_sym = m["oanda_symbol"] or m["alpha_mapping"] or "—"
        lines.append(
            f"| {m['symbol']} | {m['instrument_id']} | {m['provider'] or 'none'} | {prov_sym} | {m['store_daily_n']} | {m['store_last_date'] or '—'} | {m['live_fetch_error'] or '—'} |"
        )

    lines.extend(["", "## 4. Freshness / Blockers\n"])
    for sym, b in blockers.items():
        lines.append(f"### {sym}\n")
        lines.append(f"- Status: **{b['model_status']}**")
        lines.append(f"- Reason: {b['blocker_reason']}")
        if b.get("panel_len") is not None:
            lines.append(f"- Panel len: {b['panel_len']}")
        lines.append("")

    lines.extend(["", "## 5. Recovery Plan\n", "### Immediate fixes\n", "1. **DX** — Register in price_coverage; wire FRED DTWEXBGS tail refresh or ICE DX source.", "2. **6E** — Stop live ECB adapter from overwriting eur_2y history; use eur_2y_history deep cache.", "3. **6S/6N** — Refresh CHF/NZD 2Y from SNB/RBNZ live or accept OECD monthly lag with extended staleness window (data policy decision).", "", "### Expected publish status after repair\n", "| Symbol | Current | After data fix |", "|--------|---------|----------------|", "| DX | DATA_STALE | VALIDATED (model ready) |", "| 6E | MODEL_INCOMPLETE (n=1) | DATA_STALE or VALIDATED if price+macro fresh |", "| 6B | MODEL_INCOMPLETE (R²) | **Still blocked** — model gate, not data |", "| 6A/6C/6J | VALIDATED | VALIDATED |", "| 6S | DATA_STALE (CHF y2) | VALIDATED if CHF 2Y refreshed |", "| 6N | DATA_STALE + R² | **Partial** — data fix needed; R² gate remains |"])

    out_md.write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"\nWrote {out_json}")
    print(f"Wrote {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
